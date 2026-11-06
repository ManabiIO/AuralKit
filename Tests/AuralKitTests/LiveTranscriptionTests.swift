import AVFoundation
import Speech
import Testing
@testable import AuralKit

@Suite("Live transcription ownership")
struct LiveTranscriptionTests {
    @Test("idle stop leaves a queued public start owned", arguments: [false, true])
    @MainActor
    func idleStopPreservesQueuedStart(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let authorization = fixture.gate()
            fixture.installReadyPipeline()
            fixture.session.liveTranscriptionHooks?.authorize = { await authorization.wait() }
            let queuedStart = fixture.task {
                fixture.start(dictation: dictation)
            }
            await fixture.session.stopTranscribing()
            await queuedStart.value
            try await fixture.waitUntil { authorization.entered }
            let run = try #require(fixture.session.liveTranscription)
            #expect(!run.retired)
            #expect(fixture.session.status == .preparing)
            authorization.open()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.session.liveTranscription === run)
            #expect(fixture.errors.isEmpty)
            await fixture.session.stopTranscribing()
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.starts == 2)
        }
    }

    @Test("stop joins pending live authorization before a retry", arguments: [false, true])
    @MainActor
    func stopJoinsAuthorization(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            fixture.installReadyPipeline()
            fixture.session.liveTranscriptionHooks?.authorize = { await gate.wait() }
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { gate.entered }
            let run = try #require(fixture.session.liveTranscription)
            var stopped = false
            let stop = fixture.task {
                await fixture.session.stopTranscribing()
                stopped = true
            }
            try await fixture.waitUntil { run.retired }
            #expect(!stopped)
            #expect(fixture.starts == 0)
            gate.open()
            await stop.value
            #expect(stopped)
            #expect(fixture.session.liveTranscription == nil)
            #expect(fixture.session.status == .idle)
            #expect(fixture.starts == 0)
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.starts == 1)
        }
    }

    @Test("concurrent stops join a result child installed by retired setup", arguments: [false, true])
    @MainActor
    func stopJoinsLateResultChild(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let setup = fixture.gate()
            let resultExit = fixture.gate()
            let detectorExit = fixture.gate()
            var resultCancelled = false
            var detectorCancelled = false
            fixture.session.liveTranscriptionHooks = LiveTranscriptionHooks(
                authorize: {},
                preparePipeline: { run, _ in
                    await setup.wait()
                    run.recognizerTask = Task { @MainActor in
                        await resultExit.wait()
                        resultCancelled = Task.isCancelled
                    }
                    fixture.session.speechDetectorResultsTask = Task { @MainActor in
                        await detectorExit.wait()
                        detectorCancelled = Task.isCancelled
                    }
                },
                startAudio: { fixture.starts += 1 }
            )
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { setup.entered }
            let run = try #require(fixture.session.liveTranscription)
            var stops = 0
            let first = fixture.task { await fixture.session.stopTranscribing(); stops += 1 }
            let second = fixture.task { await fixture.session.stopTranscribing(); stops += 1 }
            try await fixture.waitUntil { run.retired }
            setup.open()
            try await fixture.waitUntil { resultExit.entered && detectorExit.entered }
            #expect(stops == 0)
            #expect(fixture.starts == 0)
            resultExit.open()
            try await fixture.waitUntil { resultCancelled }
            #expect(stops == 0)
            detectorExit.open()
            await first.value
            await second.value
            #expect(stops == 2)
            #expect(resultCancelled)
            #expect(detectorCancelled)
            #expect(fixture.session.liveTranscription == nil)
        }
    }

    @Test("stop joins admitted audio work and rejects old queued buffers", arguments: [false, true])
    @MainActor
    func stopJoinsAudioWork(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            var conversions = 0
            fixture.installReadyPipeline()
            fixture.session.liveTranscriptionHooks?.processAudio = { _ in
                conversions += 1
                await gate.wait()
            }
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let old = try #require(fixture.session.liveTranscription)
            let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 48_000, channels: 1))
            let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 16))
            buffer.frameLength = 16
            let input = SpeechSession.SendablePCMBuffer(buffer: buffer)
            fixture.session.enqueueLiveAudioBuffer(input, run: old)
            try await fixture.waitUntil { gate.entered }
            var stopped = false
            let stop = fixture.task { await fixture.session.stopTranscribing(); stopped = true }
            try await fixture.waitUntil { old.retired }
            #expect(!stopped)
            gate.open()
            await stop.value
            #expect(stopped)
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.enqueueLiveAudioBuffer(input, run: old)
            #expect(conversions == 1)
            #expect(old.audioTasks.isEmpty)
        }
    }

    @Test("late predecessor termination and activation cannot retire a successor", arguments: [false, true])
    @MainActor
    func staleTerminationPreservesRetry(dictation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let old = try #require(fixture.session.liveTranscription)
            let terminate = try #require(fixture.terminationCallback())
            await fixture.session.stopTranscribing()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let current = try #require(fixture.session.liveTranscription)
            terminate()
            for _ in 0..<32 { await Task.yield() }
            await #expect(throws: CancellationError.self) {
                try await fixture.session.setupAudioSession(liveRun: old)
            }
            #expect(current !== old)
            #expect(fixture.session.liveTranscription === current)
            #expect(!current.retired)
            #expect(fixture.session.status == .transcribing)
            #expect(fixture.starts == 2)
        }
    }

    @Test("result EOF or independent cancellation settles the live stream", arguments: [false, true], [false, true])
    @MainActor
    func resultTerminationSettles(dictation: Bool, cancelled: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let results = try #require(fixture.resultContinuations.last)
            results.yield("one")
            try await fixture.waitUntil { fixture.values == ["one"] }
            if cancelled { results.finish(throwing: CancellationError()) }
            else { results.finish() }
            try await fixture.waitUntil { fixture.session.liveTranscription == nil }
            await fixture.publicConsumers.last?.value
            #expect(fixture.session.status == .idle)
            #expect(fixture.errors.count == (cancelled ? 1 : 0))
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
        }
    }

    @MainActor
    @available(iOS 26.0, macOS 26.0, *)
    private func withFixture(_ operation: @MainActor (LiveFixture) async throws -> Void) async throws {
        let fixture = LiveFixture()
        do { try await operation(fixture) }
        catch { await fixture.cleanup(); throw error }
        await fixture.cleanup()
    }
}

extension LiveTranscriptionTests {
    @Test("Stop rejects a late format before setup publishes", arguments: [false, true], [false, true])
    @MainActor
    func stopRejectsLateFormat(dictation: Bool, returnsNil: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let gate = fixture.gate()
            let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
            var rejected = false
            fixture.installReadyPipeline()
            fixture.session.liveTranscriptionHooks?.preparePipeline = { _, _ in
                do {
                    try await fixture.session.startAnalyzer(modules: [], selectFormat: { _ in
                        await gate.wait()
                        return returnsNil ? nil : format
                    })
                    Issue.record("Retired setup was admitted")
                } catch {
                    rejected = error is CancellationError
                    // Inspect before retirement clears fields: late setup must never publish them.
                    #expect(fixture.session.analyzerFormat == nil)
                    #expect(fixture.session.inputSequence == nil)
                    #expect(fixture.session.inputBuilder == nil)
                    throw error
                }
            }
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { gate.entered }
            let run = try #require(fixture.session.liveTranscription)
            let admitOldSetup = fixture.session.captureAnalyzerSetupAdmission()
            var stopped = false
            let stop = fixture.task { await fixture.session.stopTranscribing(); stopped = true }
            try await fixture.waitUntil { run.retired }
            #expect(!stopped)
            #expect(throws: CancellationError.self) { try admitOldSetup() }
            gate.open()
            await stop.value
            #expect(rejected)
            #expect(fixture.starts == 0)
            fixture.installReadyPipeline()
            fixture.start(dictation: dictation)
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.starts == 1)
            #expect(throws: CancellationError.self) { try admitOldSetup() }
        }
    }

    @Test("Current format selection retains normal validation", arguments: [false, true])
    @MainActor
    func currentFormatSelection(returnsNil: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        await #expect(throws: SpeechSessionError.self) {
            try await session.startAnalyzer(modules: [], selectFormat: { _ in returnsNil ? nil : format })
        }
        #expect((session.analyzerFormat == nil) == returnsNil)
        #expect((session.inputSequence == nil) == returnsNil)
        #expect((session.inputBuilder == nil) == returnsNil)
        _ = session.detachTranscriberState()
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class LiveGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var opened = false
    private(set) var entered = false

    func wait() async {
        entered = true
        guard !opened else { return }
        await withCheckedContinuation { continuation = $0 }
    }

    func open() {
        opened = true
        continuation?.resume()
        continuation = nil
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class LiveFixture {
    let session = SpeechSession()
    var starts = 0
    var values: [String] = []
    var errors: [Error] = []
    var resultContinuations: [AsyncThrowingStream<String, Error>.Continuation] = []
    var publicConsumers: [Task<Void, Never>] = []
    private var outputContinuations: [AsyncThrowingStream<String, Error>.Continuation] = []
    private var gates: [LiveGate] = []
    private var tasks: [Task<Void, Never>] = []

    func gate() -> LiveGate {
        let gate = LiveGate()
        gates.append(gate)
        return gate
    }

    func task(_ operation: @escaping @MainActor () async -> Void) -> Task<Void, Never> {
        let task = Task { @MainActor in await operation() }
        tasks.append(task)
        return task
    }

    func installReadyPipeline() {
        session.liveTranscriptionHooks = LiveTranscriptionHooks(
            authorize: {},
            preparePipeline: { [unowned self] run, _ in
                let (results, input) = AsyncThrowingStream<String, Error>.makeStream()
                let (output, continuation) = AsyncThrowingStream<String, Error>.makeStream()
                resultContinuations.append(input)
                outputContinuations.append(continuation)
                run.recognizerTask = session.makeLiveRecognizerTask(
                    run: run, results: results, streamContinuation: continuation
                )
                _ = task {
                    do { for try await value in output { self.values.append(value) } }
                    catch {}
                }
            },
            startAudio: { [unowned self] in starts += 1 }
        )
    }

    func start(dictation: Bool) {
        let consumer: Task<Void, Never>
        if dictation {
            let stream = session.startDictationTranscribing()
            consumer = task {
                do { for try await _ in stream {} }
                catch { self.errors.append(error) }
            }
        } else {
            let stream = session.startTranscribing()
            consumer = task {
                do { for try await _ in stream {} }
                catch { self.errors.append(error) }
            }
        }
        publicConsumers.append(consumer)
    }

    func terminationCallback() -> (() -> Void)? {
        switch session.continuation {
        case .speech(let continuation):
            let callback = continuation.onTermination
            return { callback?(.cancelled) }
        case .dictation(let continuation):
            let callback = continuation.onTermination
            return { callback?(.cancelled) }
        case nil:
            return nil
        }
    }

    func waitUntil(_ predicate: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(2))
        while !predicate() {
            guard ContinuousClock.now < deadline else { throw TestTimeoutError.timedOut(seconds: 2) }
            await Task.yield()
        }
    }

    func cleanup() async {
        gates.forEach { $0.open() }
        resultContinuations.forEach { $0.finish() }
        await session.stopTranscribing()
        outputContinuations.forEach { $0.finish() }
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
        session.liveTranscriptionHooks = nil
    }
}
