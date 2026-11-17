import Testing
import AVFoundation
import Speech
@testable import AuralKit

extension ExternalInputLifecycleTests {
    @Test("External session ownership bypasses category and activation operations")
    @MainActor
    func externalSessionOwnership() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession(audioSessionOwnership: .external)
        var configurations = 0
        session.liveTranscriptionHooks = LiveTranscriptionHooks(configureAudioSession: { configurations += 1 })
        try await session.setupAudioSession()
        #expect(configurations == 0)
        #expect(!session.shouldUseNativeCaptureInputProvider)
        let managed = SpeechSession(audioSessionOwnership: .managed)
        managed.liveTranscriptionHooks = LiveTranscriptionHooks(configureAudioSession: { configurations += 1 })
        try await managed.setupAudioSession()
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
            let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 160))
            buffer.frameLength = 160
            buffer.int16ChannelData?.pointee.initialize(repeating: 8_192, count: 160)
            #expect(try input.append(buffer))
            buffer.int16ChannelData?[0][0] = 24_576
            var iterator = try #require(fixture.inputs).makeAsyncIterator()
            let accepted = try #require(await iterator.next())
            let acceptedBuffer = accepted.buffer
            // Keep the PCM owner alive while inspecting its borrowed sample pointer.
            let sample = withExtendedLifetime(acceptedBuffer) {
                acceptedBuffer.int16ChannelData?[0][0]
            }
            #expect(sample == 8_192)
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
            #expect(fixture.session.liveTranscription == nil)
            #expect(fixture.session.canStartTranscription)
        }
    }
}

extension ExternalInputLifecycleTests {
    @Test("External result EOF cannot hide late finalization failure", arguments: [false, true])
    @MainActor
    func externalEOFWaitsForFinalizer(fails: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let input = ExternalAudioInput()
            let stream = try await fixture.session.startTranscribing(externalAudio: input)
            fixture.consume(stream)
            let run = try #require(fixture.session.liveTranscription)
            fixture.results.continuation.finish()
            await run.recognizerTask?.value
            #expect(!run.retired)
            #expect(fixture.session.continuation != nil)
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.finalizeInput = {
                await gate.wait()
                if fails { throw LiveProbeError.finalization }
                return true
            }
            var returned = false
            let finish = fixture.task {
                await fixture.session.finishTranscribing()
                returned = true
            }
            try await fixture.waitUntil { gate.entered }
            #expect(!returned && !run.retired)
            gate.open()
            await finish.value
            await fixture.publicConsumer?.value
            #expect(returned)
            #expect(fixture.errors.count == (fails ? 1 : 0))
            if fails { #expect(fixture.errors.first as? LiveProbeError == .finalization) }
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("External handles reject pre-ready and closed input and own converted PCM")
    @MainActor
    func externalConvertedInputOwnership() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let input = ExternalAudioInput()
            let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 8_000, channels: 1))
            let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 128))
            buffer.frameLength = 128
            for frame in 0..<128 { buffer.floatChannelData?[0][frame] = 0.25 }
            #expect(try !input.append(buffer))
            fixture.consume(try await fixture.session.startTranscribing(externalAudio: input))
            #expect(try input.append(buffer))
            for frame in 0..<128 { buffer.floatChannelData?[0][frame] = 0.75 }
            var iterator = try #require(fixture.inputs).makeAsyncIterator()
            let accepted = try #require(await iterator.next())
            let acceptedBuffer = accepted.buffer
            #expect(acceptedBuffer !== buffer)
            #expect(acceptedBuffer.frameLength > 0)
            let samples = try #require(acceptedBuffer.int16ChannelData?[0])
            let peak = withExtendedLifetime(acceptedBuffer) {
                (0..<Int(acceptedBuffer.frameLength)).map { abs(Double(samples[$0]) / 32_768) }.max() ?? 0
            }
            #expect(peak.isFinite && abs(peak - 0.25) < 0.1)
            await fixture.session.cancelTranscribing()
            #expect(try !input.append(buffer))
        }
    }

    @Test("Awaited setup retains a successful receipt after immediate result completion")
    @MainActor
    func readyReceiptSurvivesImmediateCompletion() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let run = try #require(fixture.session.liveTranscription)
            let setup = try #require(run.setupTask)
            fixture.results.continuation.finish()
            try await fixture.waitUntil { fixture.session.liveTranscription == nil }
            try await fixture.session.awaitLiveSetup(setup, run: run)
            #expect(run.ready)
        }
    }
}
