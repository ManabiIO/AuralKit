import Testing
import AVFoundation
import Speech
@testable import AuralKit

extension FileTranscriptionTests {
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

    @Test("independent results cancellation fails, joins ingestion and permits retry", arguments: [false, true])
    @MainActor
    func independentFileResultsCancellationRetires(throwsCancellation: Bool) async throws {
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
            if throwsCancellation {
                analyzerResults.continuation.finish(throwing: CancellationError())
            } else {
                run.recognizerTask?.cancel()
            }
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
                do {
                    _ = try await fixture.session.transcribe(audioFile: fixture.audioURL)
                } catch {
                    fixture.errors.append(error)
                }
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
            let delivered = try await forwardedResults.stream.reduce(into: [String]()) { @Sendable in
                $0.append($1)
            }
            #expect(delivered == ["final words"])
            fixture.expectSuccessfulRetirement(run)
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

}

extension FileTranscriptionTests {
    @Test("A physical short read fails before closing analyzer input")
    @MainActor
    func physicalShortReadDoesNotSignalSuccessfulEOF() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let file = try AVAudioFile(forReading: fixture.audioURL)
            let format = try #require(AVAudioFormat(
                commonFormat: .pcmFormatInt16, sampleRate: 16_000, channels: 1, interleaved: true
            ))
            let declaredFrames = file.length + 1
            let inputs = AsyncStream<AnalyzerInput>.makeStream()
            defer { inputs.continuation.finish() }
            let run = FileTranscriptionRun()
            fixture.session.fileTranscription = run
            fixture.session.inputBuilder = inputs.continuation
            fixture.session.analyzerFormat = format
            do {
                _ = try await fixture.session.feedAudioFile(
                    (file: file, totalFrames: declaredFrames), run: run,
                    progressHandler: { _ in }
                )
                Issue.record("A premature physical EOF must report audioFileReadFailed")
            } catch SpeechSessionError.audioFileReadFailed { }
            let probe = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 1))
            probe.frameLength = 1
            if case .terminated = inputs.continuation.yield(AnalyzerInput(buffer: probe)) {
                Issue.record("The feeder signaled input EOF before its read failure was admitted")
            }
        }
    }

    @Test("Early result EOF cannot hide a later ingestion or finalization failure", arguments: [false, true])
    @MainActor
    func earlyResultEOFPreservesProducerFailure(finalization: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let failure = fixture.gate()
            let results = AsyncThrowingStream<String, Error>.makeStream()
            let forwarded = AsyncThrowingStream<String, Error>.makeStream()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = fixture.session.makeFileRecognizerTask(
                        run: run, results: results.stream,
                        streamContinuation: forwarded.continuation
                    )
                },
                ingest: {
                    if !finalization {
                        await failure.wait()
                        throw FileLifecycleProbeError.ingestion
                    }
                    return true
                },
                finalizeInput: {
                    guard finalization else {
                        Issue.record("A failed ingester must not finalize input")
                        return
                    }
                    await failure.wait()
                    throw FileLifecycleProbeError.finalization
                }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { failure.entered }
            let run = try #require(fixture.session.fileTranscription)
            results.continuation.finish()
            await run.recognizerTask?.value
            #expect(!run.retired)
            #expect(fixture.session.fileTranscription === run)
            failure.open()
            await consumer.value
            forwarded.continuation.finish()
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first as? FileLifecycleProbeError ==
                    (finalization ? .finalization : .ingestion))
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }
}
