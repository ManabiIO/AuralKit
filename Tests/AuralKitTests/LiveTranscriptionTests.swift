import Testing
import AVFoundation
import Speech
@testable import AuralKit

@Suite("Live transcription ownership")
struct LiveTranscriptionTests {
    @Test("Finish forwards final results and joins their reader before closing the public stream")
    @MainActor
    func finishDrainsFinalResults() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            await fixture.session.finishTranscribing()
            fixture.forwarded.continuation.finish()
            await fixture.forwardingConsumer?.value
            await fixture.publicConsumer?.value
            #expect(fixture.received == ["final tail"])
            #expect(fixture.finalizations == 1)
            #expect(fixture.cancellations == 0)
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.liveTranscription == nil)
            #expect(fixture.session.recognizerTask == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("Reader completion cannot close a stream while Finish is still finalizing")
    @MainActor
    func readerCannotRetireActiveFinalization() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.finalizeInput = {
                fixture.results.continuation.yield("last words")
                fixture.results.continuation.finish()
                await gate.wait()
            }
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let reader = try #require(fixture.session.recognizerTask)
            let finish = fixture.task { await fixture.session.finishTranscribing() }
            try await fixture.waitUntil { gate.entered }
            await reader.value
            #expect(fixture.session.status == .stopping)
            #expect(fixture.session.continuation != nil)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await finish.value
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Finish waits for a delayed result reader after analyzer finalization returns")
    @MainActor
    func finishJoinsDelayedReader() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.preparePipeline = { generation in
                fixture.session.recognizerTask = fixture.session.createRecognizerTask(
                    label: "Delayed live reader", results: DelayedLiveResults(gate: gate),
                    streamContinuation: fixture.forwarded.continuation, generation: generation
                )
            }
            fixture.session.liveTranscriptionHooks?.finalizeInput = { fixture.finalizations += 1 }
            fixture.start()
            try await fixture.waitUntil { gate.entered }
            let run = try #require(fixture.session.liveTranscription)
            var completed = false
            let finish = fixture.task { await fixture.session.finishTranscribing(); completed = true }
            try await fixture.waitUntil { fixture.finalizations == 1 && run.finalizationTask == nil }
            #expect(!completed)
            #expect(fixture.session.status == .stopping)
            #expect(fixture.session.continuation != nil)
            gate.open()
            await finish.value
            fixture.forwarded.continuation.finish()
            await fixture.forwardingConsumer?.value
            #expect(fixture.received == ["delayed tail"])
            #expect(completed)
        }
    }

    @Test("Concurrent finishes share finalization and reject restart until it returns")
    @MainActor
    func concurrentFinishesJoin() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.finalizeInput = {
                fixture.finalizations += 1
                await gate.wait()
                fixture.results.continuation.finish()
            }
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            var completed = 0
            let first = fixture.task { await fixture.session.finishTranscribing(); completed += 1 }
            try await fixture.waitUntil { gate.entered }
            let second = fixture.task { await fixture.session.finishTranscribing(); completed += 1 }
            let rejected = fixture.session.startTranscribing()
            await #expect(throws: SpeechSessionError.self) {
                for try await _ in rejected {}
            }
            #expect(completed == 0)
            #expect(fixture.session.status == .stopping)
            gate.open()
            await first.value
            await second.value
            #expect(completed == 2)
            #expect(fixture.finalizations == 1)
            #expect(fixture.session.liveTranscription == nil)
        }
    }

    @Test("Cancel escalates an in-flight Finish and unblocks finalization")
    @MainActor
    func cancelDuringFinish() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.finalizeInput = {
                fixture.finalizations += 1
                await gate.wait()
                try Task.checkCancellation()
            }
            fixture.session.liveTranscriptionHooks?.cancelAnalyzer = {
                fixture.cancellations += 1
                gate.open()
                fixture.results.continuation.finish()
            }
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let finish = fixture.task { await fixture.session.finishTranscribing() }
            try await fixture.waitUntil { gate.entered }
            await fixture.session.cancelTranscribing()
            await finish.value
            #expect(fixture.cancellations == 1)
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("Cancel during native input draining joins the detached native analysis task")
    @MainActor
    func cancelNativeDrain() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let nativeInputs = AsyncStream<Void>.makeStream()
            var nativeCompleted = false
            fixture.session.nativeCaptureAnalysisTask = Task { @MainActor in
                for await _ in nativeInputs.stream {}
                nativeCompleted = true
                fixture.results.continuation.finish()
            }
            fixture.session.liveTranscriptionHooks?.finalizeInput = nil
            let run = try #require(fixture.session.liveTranscription)
            let finish = fixture.task { await fixture.session.finishTranscribing() }
            try await fixture.waitUntil { run.nativeAnalysisTask != nil }
            await fixture.session.cancelTranscribing()
            await finish.value
            nativeInputs.continuation.finish()
            #expect(nativeCompleted)
            #expect(fixture.session.nativeCaptureAnalysisTask == nil)
            #expect(run.nativeAnalysisTask == nil)
        }
    }

    @Test("Finish and Cancel during startup join late setup before admitting another run", arguments: [false, true])
    @MainActor
    func terminationJoinsStartup(finish: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.authorize = { await gate.wait() }
            var preparationCalls = 0
            fixture.session.liveTranscriptionHooks?.preparePipeline = { _ in preparationCalls += 1 }
            fixture.start()
            try await fixture.waitUntil { gate.entered }
            var completed = false
            let stop = fixture.task {
                if finish { await fixture.session.finishTranscribing() }
                else { await fixture.session.cancelTranscribing() }
                completed = true
            }
            try await fixture.waitUntil { fixture.session.status == .stopping }
            #expect(!completed)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await stop.value
            #expect(completed)
            #expect(preparationCalls == 0)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Finalization failures reach the public stream and cancel remaining analysis")
    @MainActor
    func finalizationFailure() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.session.liveTranscriptionHooks?.finalizeInput = { throw LiveProbeError.finalization }
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            await fixture.session.finishTranscribing()
            await fixture.publicConsumer?.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first as? LiveProbeError == .finalization)
            #expect(fixture.cancellations == 1)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Natural result completion retires through a separate task without joining itself")
    @MainActor
    func naturalCompletion() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.results.continuation.finish()
            try await fixture.waitUntil { fixture.session.canStartTranscription }
            #expect(fixture.errors.isEmpty)
        }
    }

}

extension LiveTranscriptionTests {
    @Test("External session ownership bypasses category and activation operations")
    @MainActor
    func externalSessionOwnership() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession(audioSessionOwnership: .external)
        var configurations = 0
        session.liveTranscriptionHooks = LiveTranscriptionHooks(configureAudioSession: { configurations += 1 })
        try session.setupAudioSession()
        #expect(configurations == 0)
        #expect(!session.shouldUseNativeCaptureInputProvider)
        let managed = SpeechSession(audioSessionOwnership: .managed)
        managed.liveTranscriptionHooks = LiveTranscriptionHooks(configureAudioSession: { configurations += 1 })
        try managed.setupAudioSession()
        #expect(configurations == 1)
#if os(iOS)
        session.streamingMode = .liveMicrophone
        session.status = .transcribing
        await session.handleAudioSessionInterruption(
            typeValue: AVAudioSession.InterruptionType.began.rawValue, optionsValue: nil
        )
        #expect(session.status == .transcribing)
#endif
    }

    @Test("External audio starts without capture and snapshots caller-owned PCM", arguments: [false, true])
    @MainActor
    func externalInputCopiesAndCloses(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let input = ExternalAudioInput()
            fixture.session.liveTranscriptionHooks?.configureAudioSession = {
                Issue.record("External input must not configure or activate an audio session")
            }
            if dictation {
                let stream = try await fixture.session.startDictationTranscribing(externalAudio: input)
                fixture.consume(stream)
            } else {
                let stream = try await fixture.session.startTranscribing(externalAudio: input)
                fixture.consume(stream)
            }
            #expect(fixture.session.streamingMode == .externalAudio)
            #expect(!fixture.session.isAudioStreaming)
            #expect(fixture.session.nativeCaptureSession == nil)
            let format = try #require(fixture.session.analyzerFormat)
            let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 1))
            buffer.frameLength = 1
            buffer.floatChannelData?[0][0] = 0.25
            #expect(try input.append(buffer))
            buffer.floatChannelData?[0][0] = 0.75
            var iterator = try #require(fixture.inputs).makeAsyncIterator()
            let accepted = await iterator.next()
            #expect(accepted?.buffer.floatChannelData?[0][0] == 0.25)
            await fixture.session.finishTranscribing()
            #expect(try !input.append(buffer))

            // A finished handle cannot be rebound to a new analyzer/stream.
            let replacement = AsyncStream<AnalyzerInput>.makeStream()
            #expect(throws: SpeechSessionError.self) {
                try input.bind(to: replacement.continuation, format: format)
            }
            replacement.continuation.finish()
        }
    }

    @Test("Cancel while external startup is suspended never binds the stale input")
    @MainActor
    func externalStartupCancellation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.authorize = { await gate.wait() }
            let input = ExternalAudioInput()
            var startupReturned = false
            let start = fixture.task {
                do {
                    _ = try await fixture.session.startTranscribing(externalAudio: input)
                    Issue.record("Cancelled external startup should throw")
                } catch is CancellationError {} catch { Issue.record(error) }
                startupReturned = true
            }
            try await fixture.waitUntil { gate.entered }
            start.cancel()
            try await fixture.waitUntil { fixture.session.status == .stopping }
            #expect(!startupReturned)
            gate.open()
            await start.value
            #expect(startupReturned)
            #expect(fixture.session.externalAudioInput == nil)
            #expect(fixture.session.canStartTranscription)
        }
    }
}

