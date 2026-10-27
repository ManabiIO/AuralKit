import Testing
import AVFoundation
@testable import AuralKit

@Suite("File transcription")
struct FileTranscriptionTests {

    @Test("incomplete file ingestion fails and retires instead of leaving analysis open")
    @MainActor
    func incompleteFileInputRetires() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {}, preparePipeline: { _ in }, ingest: { false },
                finalizeInput: { Issue.record("Incomplete input must not report successful finalization") }
            )
            let consumer = fixture.transcribe()
            await consumer.value
            #expect(fixture.errors.count == 1)
            let error = try #require(fixture.errors.first as? SpeechSessionError)
            guard case .audioFileReadFailed = error else {
                Issue.record("Incomplete input must report audioFileReadFailed")
                return
            }
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("independent results cancellation fails the public stream, joins ingestion and permits retry")
    @MainActor
    func independentFileResultsCancellationRetires() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let ingestion = fixture.gate()
            let analyzerResults = AsyncThrowingStream<String, Error>.makeStream()
            let forwardedResults = AsyncThrowingStream<String, Error>.makeStream()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = fixture.session.makeFileRecognizerTask(
                        run: run, results: analyzerResults.stream,
                        streamContinuation: forwardedResults.continuation
                    )
                },
                ingest: { await ingestion.wait(); return true }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { ingestion.entered }
            let run = try #require(fixture.session.fileTranscription)
            analyzerResults.continuation.finish(throwing: CancellationError())
            try await fixture.waitUntil { run.retired }
            #expect(fixture.session.fileTranscription === run)
            ingestion.open()
            await consumer.value
            forwardedResults.continuation.finish()
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.fileTranscription == nil)
            #expect(run.ingestionTask == nil)
            #expect(run.recognizerTask == nil)

            let retryAuthorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await retryAuthorization.wait() }, preparePipeline: { _ in }
            )
            let retry = fixture.transcribe()
            try await fixture.waitUntil { retryAuthorization.entered }
            let retryRun = try #require(fixture.session.fileTranscription)
            #expect(retryRun !== run)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { retryRun.retired }
            retryAuthorization.open()
            await stop.value
            await retry.value
            #expect(fixture.session.fileTranscription == nil)
        }
    }

    @Test("file EOF explicitly finalizes analysis and drains its final results before retirement")
    @MainActor
    func fileEOFFinalizesAndDrainsResults() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let finalization = fixture.gate()
            // Model an autonomous analyzer: input EOF alone leaves results
            // open. Only explicit finalization publishes the final result.
            let analyzerResults = AsyncThrowingStream<String, Error>.makeStream()
            let forwardedResults = AsyncThrowingStream<String, Error>.makeStream()
            var finalizationCalls = 0
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = fixture.session.makeFileRecognizerTask(
                        run: run, results: analyzerResults.stream,
                        streamContinuation: forwardedResults.continuation
                    )
                },
                ingest: {
                    await MainActor.run { fixture.ingestionReturned = true }
                    return true
                },
                finalizeInput: {
                    finalizationCalls += 1
                    #expect(fixture.ingestionReturned)
                    await finalization.wait()
                    // Both publication and sequence end happen before the
                    // reader resumes, exercising its buffered final drain.
                    analyzerResults.continuation.yield("final words")
                    analyzerResults.continuation.finish()
                }
            )
            var transcribeCompleted = false
            let consumer = fixture.task {
                do { _ = try await fixture.session.transcribe(audioFile: fixture.audioURL) }
                catch { fixture.errors.append(error) }
                transcribeCompleted = true
            }
            try await fixture.waitUntil { finalization.entered }
            let run = try #require(fixture.session.fileTranscription)
            #expect(finalizationCalls == 1)
            #expect(!run.retired)
            #expect(run.recognizerTask?.isCancelled == false)
            #expect(!transcribeCompleted)
            finalization.open()
            await consumer.value
            forwardedResults.continuation.finish()
            let delivered = try await forwardedResults.stream.reduce(into: [String]()) { $0.append($1) }
            #expect(delivered == ["final words"])
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
            #expect(run.ingestionTask == nil)
            #expect(run.recognizerTask == nil)
        }
    }

    @Test("stop during EOF finalization joins the finalizer and all file tasks")
    @MainActor
    func stopJoinsFileEOFFinalization() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let finalization = fixture.gate()
            let analyzerResults = AsyncThrowingStream<String, Error>.makeStream()
            let forwardedResults = AsyncThrowingStream<String, Error>.makeStream()
            var finalizationReturned = false
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = fixture.session.makeFileRecognizerTask(
                        run: run, results: analyzerResults.stream,
                        streamContinuation: forwardedResults.continuation
                    )
                },
                ingest: { true },
                finalizeInput: {
                    await finalization.wait()
                    finalizationReturned = true
                }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { finalization.entered }
            let run = try #require(fixture.session.fileTranscription)
            var stopCompleted = false
            let stop = fixture.task { await fixture.session.stopTranscribing(); stopCompleted = true }
            try await fixture.waitUntil { run.retired }
            #expect(!stopCompleted)
            #expect(!finalizationReturned)
            #expect(fixture.session.fileTranscription === run)
            finalization.open()
            await stop.value
            await consumer.value
            analyzerResults.continuation.finish()
            forwardedResults.continuation.finish()
            #expect(finalizationReturned)
            #expect(stopCompleted)
            #expect(fixture.session.fileTranscription == nil)
            #expect(run.ingestionTask == nil)
            #expect(run.recognizerTask == nil)
        }
    }

    @Test("EOF finalization failure propagates and retires without joining ingestion itself", arguments: [false, true])
    @MainActor
    func fileEOFFinalizationFailureRetires(cancelled: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {}, preparePipeline: { _ in }, ingest: { true },
                finalizeInput: {
                    if cancelled { throw CancellationError() }
                    throw FileLifecycleProbeError.finalization
                }
            )
            let consumer = fixture.transcribe()
            await consumer.value
            #expect(fixture.errors.count == 1)
            if cancelled {
                #expect(fixture.errors.first is CancellationError)
            } else {
                #expect(fixture.errors.first as? FileLifecycleProbeError == .finalization)
            }
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("stop joins suspended file authorization and prevents late pipeline startup")
    @MainActor
    func stopJoinsSuspendedFileAuthorization() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let authorization = fixture.gate()
            var pipelineStarts = 0
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await authorization.wait() },
                preparePipeline: { _ in pipelineStarts += 1 },
                ingest: { Issue.record("Retired setup must not ingest audio"); return true }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { authorization.entered }
            let run = try #require(fixture.session.fileTranscription)
            consumer.cancel()
            await consumer.value
            try await fixture.waitUntil { run.retired }
            var stopEntered = false
            var stopCompleted = false
            let stop = fixture.task {
                stopEntered = true
                await fixture.session.stopTranscribing()
                stopCompleted = true
            }
            try await fixture.waitUntil { stopEntered }
            #expect(!stopCompleted)
            #expect(fixture.session.fileTranscription === run)
            authorization.open()
            await stop.value
            await consumer.value
            #expect(stopCompleted)
            #expect(pipelineStarts == 0)
            #expect(fixture.session.fileTranscription == nil)
            #expect(run.setupTask == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("stop joins late child admitted by suspended analyzer setup")
    @MainActor
    func stopJoinsLatePipelineChild() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let preparation = fixture.gate()
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    await preparation.wait()
                    // Model/analyzer setup may ignore cancellation and admit a
                    // result task before returning to the outer owner check.
                    run.recognizerTask = Task { @MainActor in await results.wait() }
                },
                ingest: { Issue.record("Retired preparation must not ingest audio"); return true }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { preparation.entered }
            let run = try #require(fixture.session.fileTranscription)
            var stopCompleted = false
            let stop = fixture.task { await fixture.session.stopTranscribing(); stopCompleted = true }
            try await fixture.waitUntil { run.retired }
            #expect(!stopCompleted)
            preparation.open()
            try await fixture.waitUntil { results.entered }
            try await fixture.waitUntil { run.recognizerTask?.isCancelled == true }
            #expect(!stopCompleted)
            results.open()
            await stop.value
            await consumer.value
            #expect(stopCompleted)
            #expect(run.recognizerTask == nil)
            #expect(fixture.session.fileTranscription == nil)
        }
    }

    @Test("concurrent stops join file ingestion and recognizer before returning")
    @MainActor
    func concurrentStopsJoinFileProducers() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let ingestion = fixture.gate()
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = Task { @MainActor in
                        await results.wait()
                        fixture.session.retireFileTranscription(run, error: nil)
                    }
                },
                ingest: {
                    await ingestion.wait()
                    await MainActor.run { fixture.ingestionReturned = true }
                    return true
                }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { ingestion.entered && results.entered }
            let run = try #require(fixture.session.fileTranscription)
            var completedStops = 0
            let first = fixture.task { await fixture.session.stopTranscribing(); completedStops += 1 }
            let second = fixture.task { await fixture.session.stopTranscribing(); completedStops += 1 }
            try await fixture.waitUntil { run.retired }
            #expect(completedStops == 0)
            ingestion.open()
            try await fixture.waitUntil { fixture.ingestionReturned }
            #expect(completedStops == 0)
            #expect(fixture.session.status == .stopping)
            #expect(fixture.session.fileTranscription === run)
            results.open()
            await first.value
            await second.value
            await consumer.value
            #expect(completedStops == 2)
            #expect(run.ingestionTask == nil)
            #expect(run.recognizerTask == nil)
            #expect(fixture.session.fileTranscription == nil)
        }
    }

    @Test("recognizer normal finish retires without joining itself and permits retry")
    @MainActor
    func normalFileFinishAndRetryRejectOldRetirement() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = Task { @MainActor in
                        await results.wait()
                        fixture.session.retireFileTranscription(run, error: nil)
                    }
                },
                ingest: { await MainActor.run { fixture.ingestionReturned = true }; return true }
            )
            let first = fixture.consume()
            try await fixture.waitUntil { fixture.ingestionReturned && results.entered }
            let oldRun = try #require(fixture.session.fileTranscription)
            results.open()
            await first.value
            await fixture.session.stopTranscribing()
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.fileTranscription == nil)

            let retryAuthorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await retryAuthorization.wait() },
                preparePipeline: { _ in Issue.record("Stopped retry must not start a pipeline") }
            )
            let retry = fixture.consume()
            try await fixture.waitUntil { retryAuthorization.entered }
            let retryRun = try #require(fixture.session.fileTranscription)
            #expect(retryRun !== oldRun)
            #expect(fixture.session.retireFileTranscription(oldRun, error: nil) == nil)
            #expect(!retryRun.retired)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { retryRun.retired }
            retryAuthorization.open()
            await stop.value
            await retry.value
        }
    }

    @Test("file producer error completes cleanup without awaiting its own task")
    @MainActor
    func fileIngestionFailureDoesNotSelfJoin() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {}, preparePipeline: { _ in },
                ingest: { throw FileLifecycleProbeError.ingestion }
            )
            let consumer = fixture.consume()
            await consumer.value
            await fixture.session.stopTranscribing()
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first as? FileLifecycleProbeError == .ingestion)
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("rejected file and microphone streams cannot retire an admitted file run")
    @MainActor
    func rejectedStreamsPreserveAdmittedFileRun() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let authorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await authorization.wait() }, preparePipeline: { _ in }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { authorization.entered }
            let run = try #require(fixture.session.fileTranscription)
            for stream in [
                fixture.session.streamTranscription(from: fixture.audioURL),
                fixture.session.startTranscribing()
            ] {
                await #expect(throws: SpeechSessionError.self) {
                    for try await _ in stream {}
                }
            }
            let dictation = fixture.session.startDictationTranscribing()
            await #expect(throws: SpeechSessionError.self) {
                for try await _ in dictation {}
            }
            // Let queued termination callbacks from all rejected streams run.
            for _ in 0..<32 { await Task.yield() }
            #expect(fixture.session.fileTranscription === run)
            #expect(!run.retired)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { run.retired }
            authorization.open()
            await stop.value
            await consumer.value
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    @MainActor
    private func withFileLifecycleFixture(
        _ operation: @MainActor (FileLifecycleFixture) async throws -> Void
    ) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let fixture = FileLifecycleFixture(audioURL: try createSilenceAudioFile(duration: 0.05))
        do {
            try await operation(fixture)
        } catch {
            await fixture.cleanup()
            throw error
        }
        await fixture.cleanup()
    }

    @Test("transcribe throws audioFileNotFound for missing file")
    @MainActor
    func transcribeMissingFileThrowsNotFound() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let missingURL = URL(fileURLWithPath: "/tmp/not-real-")

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: missingURL)
        }
    }

    @Test("transcribe rejects audio exceeding maximum duration")
    @MainActor
    func transcribeRejectsAudioExceedingMaximumDuration() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let tempURL = try createSilenceAudioFile(duration: 2.0)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let session = SpeechSession()
        let options = FileTranscriptionOptions(maxDuration: 1.0)

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: tempURL, options: options)
        }
    }

    private func createSilenceAudioFile(duration: TimeInterval, sampleRate: Double = 44_100) throws -> URL {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension("caf")

        guard let format = AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: 1) else {
            throw NSError(
                domain: "FileTranscriptionTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Failed to create audio format"]
            )
        }

        let frameCount = AVAudioFrameCount(duration * sampleRate)
        guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frameCount) else {
            throw NSError(
                domain: "FileTranscriptionTests",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "Failed to create buffer"]
            )
        }

        buffer.frameLength = frameCount
        if let channelData = buffer.floatChannelData {
            for channel in 0..<Int(buffer.format.channelCount) {
                channelData[channel].initialize(repeating: 0, count: Int(buffer.frameLength))
            }
        }

        let audioFile = try AVAudioFile(forWriting: tempURL, settings: format.settings)
        try audioFile.write(from: buffer)

        return tempURL
    }

    @Test("transcribe throws audioFileInvalidURL for non-file URL")
    @MainActor
    func transcribeNonFileURLThrowsInvalidURL() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let httpURL = URL(string: "https://example.com/audio.wav")!

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: httpURL)
        }
    }

    @Test("transcribe throws audioFileOutsideAllowedDirectories for restricted path")
    @MainActor
    func transcribeOutsideAllowedDirectoriesThrows() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let tempURL = try createSilenceAudioFile(duration: 0.5)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let session = SpeechSession()
        let restrictedOptions = FileTranscriptionOptions(
            allowedDirectories: [URL(fileURLWithPath: "/nonexistent/path")]
        )

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: tempURL, options: restrictedOptions)
        }
    }
}

private enum FileLifecycleProbeError: Error, Equatable { case ingestion, finalization, arrivalTimeout }

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class FileLifecycleGate {
    private(set) var entered = false
    private var isOpen = false
    private var releases: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        entered = true
        guard !isOpen else { return }
        await withCheckedContinuation { releases.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = releases
        releases.removeAll()
        for continuation in continuations { continuation.resume() }
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class FileLifecycleFixture {
    let session = SpeechSession()
    let audioURL: URL
    var errors: [Error] = []
    var ingestionReturned = false
    private var gates: [FileLifecycleGate] = []
    private var tasks: [Task<Void, Never>] = []

    init(audioURL: URL) { self.audioURL = audioURL }

    func gate() -> FileLifecycleGate {
        let gate = FileLifecycleGate()
        gates.append(gate)
        return gate
    }

    func task(_ operation: @escaping @MainActor () async -> Void) -> Task<Void, Never> {
        let task = Task { @MainActor in await operation() }
        tasks.append(task)
        return task
    }

    func consume() -> Task<Void, Never> {
        // Admit synchronously so tests can immediately inspect the actual run.
        let stream = session.streamTranscription(from: audioURL)
        return task {
            do { for try await _ in stream {} }
            catch { self.errors.append(error) }
        }
    }

    func transcribe() -> Task<Void, Never> {
        task {
            do { _ = try await self.session.transcribe(audioFile: self.audioURL) }
            catch { self.errors.append(error) }
        }
    }

    func waitUntil(_ predicate: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while !predicate() {
            guard ContinuousClock.now < deadline else { throw FileLifecycleProbeError.arrivalTimeout }
            try await Task.sleep(for: .milliseconds(1))
        }
    }

    func cleanup() async {
        // Open-before-stop also covers late task admission after a failed wait.
        for gate in gates { gate.open() }
        await session.stopTranscribing()
        for task in tasks { await task.value }
        session.fileTranscriptionHooks = nil
        tasks.removeAll()
        try? FileManager.default.removeItem(at: audioURL)
    }
}
