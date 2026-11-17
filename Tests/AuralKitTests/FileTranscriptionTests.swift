import Testing
import AVFoundation
import Speech
@testable import AuralKit

@Suite("File transcription")
struct FileTranscriptionTests {

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
            var rejected = [
                fixture.session.streamTranscription(from: fixture.audioURL),
                fixture.session.startTranscribing()
            ]
#if canImport(ScreenCaptureKit)
            if #available(iOS 27.0, macOS 26.0, *) {
                rejected.append(fixture.session.startTranscribingScreenCapture())
            }
#endif
            for stream in rejected {
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
    func withFileLifecycleFixture(
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

#if swift(>=6.4)
    @Test("Cancelling a media analyzer before it receives input closes the session", arguments: [false, true])
    @MainActor
    func cancelBeforeAnalyzerInputClosesSession(analyzing: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = FileTranscriptionRun()
        session.fileTranscription = run
        let (results, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        let analyzer = SpeechAnalyzer(modules: [])
        session.analyzer = analyzer
        session.continuation = .speech(continuation)
        session.streamingMode = .mediaPlayback
        session.setStatus(.preparing)

        let (input, inputBuilder) = AsyncStream<AnalyzerInput>.makeStream()
        defer { inputBuilder.finish() }
        if analyzing {
            let ready = AsyncStream<Void>.makeStream()
            session.startFileIngestion(run: run) { _ in
                ready.continuation.finish()
                _ = try await analyzer.analyzeSequence(input)
            }
            for await _ in ready.stream {}
        }

        let watchdog = Task {
            do {
                try await Task.sleep(for: .seconds(3))
                Issue.record("Cancelling an analyzer with no input did not return")
                await analyzer.cancelAndFinishNow()
            } catch is CancellationError {
                // Cancel completed before the deadline.
            }
        }
        await session.cancelTranscribing()
        watchdog.cancel()
        try await watchdog.value

        await #expect(throws: CancellationError.self) {
            for try await _ in results {}
        }
        #expect(session.status == .idle)
    }

    @Test("A cancelled recognizer cannot hide a file read error")
    @MainActor
    func fileReadErrorSurvivesRecognizerCancellation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let output = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        let transcriber = SpeechTranscriber(locale: Locale(identifier: "en-US"), preset: .transcription)
        session.analyzer = SpeechAnalyzer(modules: [transcriber])
        let run = FileTranscriptionRun()
        session.fileTranscription = run
        session.continuation = .speech(output.continuation)
        session.streamingMode = .mediaPlayback
        session.setStatus(.transcribing)
        let recognizer = session.makeFileRecognizerTask(
            run: run, results: transcriber.results, streamContinuation: output.continuation
        )
        run.recognizerTask = recognizer
        session.recognizerTask = recognizer
        session.startFileIngestion(run: run) { _ in
            throw SpeechSessionError.audioFileReadFailed(nil)
        }
        let ingestion = try #require(run.ingestionTask)

        await recognizer.value
        await ingestion.value
        await run.terminalTask?.value
        var iterator = output.stream.makeAsyncIterator()
        do {
            _ = try await iterator.next()
            Issue.record("Expected the file read error to reach the transcription stream")
        } catch SpeechSessionError.audioFileReadFailed { }
        #expect(session.status == .idle)
    }

    @Test("Media transcription rejects an unknown audio track without falling back")
    @MainActor
    func mediaTranscriptionRejectsUnknownTrack() async throws {
        guard #available(iOS 27.0, macOS 27.0, *) else { return }
        let url = try createSilenceAudioFile(duration: 2.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let asset = AVURLAsset(url: url)
        let tracks = try await asset.loadTracks(withMediaType: .audio)
        let track = try #require(tracks.first)
        let session = SpeechSession()

        let stream = session.streamTranscription(fromMediaFile: url, audioTrackID: track.trackID + 1)
        do {
            for try await _ in stream { }
            Issue.record("Expected the unknown track to be rejected")
        } catch SpeechSessionError.audioFileUnsupportedFormat(let reason) {
            #expect(reason.contains("\(track.trackID + 1)"))
        }
    }
#endif

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

extension FileTranscriptionTests {
    @Test("independent file setup cancellation fails and permits retry", arguments: [false, true])
    @MainActor
    func independentFileSetupCancellationRetires(duringPreparation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let setup = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {
                    if !duringPreparation {
                        await setup.wait()
                        throw CancellationError()
                    }
                },
                preparePipeline: { _ in
                    if duringPreparation {
                        await setup.wait()
                        throw CancellationError()
                    }
                },
                ingest: { Issue.record("Failed setup must not ingest audio"); return true }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { setup.entered }
            let failedRun = try #require(fixture.session.fileTranscription)
            setup.open()
            await consumer.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
            #expect(failedRun.setupTask == nil)

            let retryAuthorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await retryAuthorization.wait() },
                preparePipeline: { _ in Issue.record("Stopped retry must not prepare analysis") }
            )
            let retry = fixture.transcribe()
            try await fixture.waitUntil { retryAuthorization.entered }
            let retryRun = try #require(fixture.session.fileTranscription)
            #expect(retryRun !== failedRun)
            #expect(fixture.session.retireFileTranscription(failedRun, error: CancellationError()) == nil)
            #expect(!retryRun.retired)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { retryRun.retired }
            retryAuthorization.open()
            await stop.value
            await retry.value
            #expect(fixture.errors.count == 1, "Intentional retry stop adds no setup failure")
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("public stop keeps its outcome when suspended setup later throws cancellation", arguments: [false, true])
    @MainActor
    func publicStopOwnsLateFileSetupCancellation(duringPreparation: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let setup = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {
                    if !duringPreparation {
                        await setup.wait()
                        throw CancellationError()
                    }
                },
                preparePipeline: { _ in
                    if duringPreparation {
                        await setup.wait()
                        throw CancellationError()
                    }
                },
                ingest: { Issue.record("Stopped setup must not ingest audio"); return true }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { setup.entered }
            let run = try #require(fixture.session.fileTranscription)
            var stopCompleted = false
            let stop = fixture.task {
                await fixture.session.stopTranscribing()
                stopCompleted = true
            }
            try await fixture.waitUntil { run.retired }
            #expect(!stopCompleted)
            #expect(fixture.session.fileTranscription === run)
            setup.open()
            await stop.value
            await consumer.value
            #expect(stopCompleted)
            #expect(fixture.errors.isEmpty)
            #expect(run.setupTask == nil)
            fixture.expectSuccessfulRetirement(run)
        }
    }
}
