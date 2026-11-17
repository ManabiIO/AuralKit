import Testing
import AVFoundation
import Speech
@testable import AuralKit

@Suite("External input and acknowledged startup ownership")
struct ExternalInputLifecycleTests {
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
            #expect(fixture.cancellations == 1)
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
                return true
            }
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let reader = try #require(fixture.session.liveTranscription?.recognizerTask)
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
            fixture.session.liveTranscriptionHooks?.preparePipeline = { run, _ in
                run.recognizerTask = fixture.session.makeLiveRecognizerTask(
                    run: run, results: DelayedLiveResults(gate: gate),
                    streamContinuation: fixture.forwarded.continuation
                )
            }
            fixture.session.liveTranscriptionHooks?.finalizeInput = { fixture.finalizations += 1; return true }
            fixture.start()
            try await fixture.waitUntil { gate.entered }
            var completed = false
            let finish = fixture.task { await fixture.session.finishTranscribing(); completed = true }
            try await fixture.waitUntil { fixture.finalizations == 1 }
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
                return true
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
                return true
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
            await fixture.publicConsumer?.value
            #expect(fixture.cancellations == 1)
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
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
            let run = try #require(fixture.session.liveTranscription)
            run.usesNativeInput = true
            run.inputTask = Task { @MainActor in
                for await _ in nativeInputs.stream {}
                nativeCompleted = true
                fixture.results.continuation.finish()
            }
            fixture.session.liveTranscriptionHooks?.finalizeInput = nil
            let finish = fixture.task { await fixture.session.finishTranscribing() }
            try await fixture.waitUntil { run.finishing && run.finishTask != nil }
            await fixture.session.cancelTranscribing()
            await finish.value
            nativeInputs.continuation.finish()
            #expect(nativeCompleted)
            #expect(fixture.session.liveTranscription == nil)
            #expect(run.inputTask == nil)
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
            fixture.session.liveTranscriptionHooks?.preparePipeline = { _, _ in preparationCalls += 1 }
            fixture.start()
            try await fixture.waitUntil { gate.entered }
            var completed = false
            let stop = fixture.task {
                if finish {
                    await fixture.session.finishTranscribing()
                } else {
                    await fixture.session.cancelTranscribing()
                }
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
