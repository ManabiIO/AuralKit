import Foundation
import AVFoundation
import Speech

/// Configuration used by `SpeechSession` when transcribing an audio file from disk.
@available(iOS 26.0, macOS 26.0, *)
public struct FileTranscriptionOptions {
    /// Optional contextual strings to improve transcription accuracy.
    public var contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]?

    /// Maximum allowed duration for the audio file. Files longer than this will fail validation.
    public var maxDuration: TimeInterval?

    /// Directories considered safe for audio file reads. Defaults to the app sandbox directories.
    public var allowedDirectories: [URL]

    /// Creates a new set of file transcription options.
    /// - Parameters:
    ///   - contextualStrings: Tag-keyed terms that bias recognition toward domain-specific vocabulary.
    ///   - maxDuration: Upper limit for the audio length; longer files trigger `.audioFileTooLong`.
    ///   - allowedDirectories: Whitelisted directories that file URLs must reside in after resolving symlinks.
    public init(
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil,
        maxDuration: TimeInterval? = nil,
        allowedDirectories: [URL]? = nil
    ) {
        self.contextualStrings = contextualStrings
        self.maxDuration = maxDuration
        self.allowedDirectories = FileTranscriptionOptions.prepareAllowedDirectories(allowedDirectories)
    }

    private static func prepareAllowedDirectories(_ directories: [URL]?) -> [URL] {
        let fileManager = FileManager.default
        var resolved = directories?.filter(\.isFileURL) ?? []

        if resolved.isEmpty {
            resolved.append(contentsOf: fileManager.urls(for: .documentDirectory, in: .userDomainMask))
            resolved.append(contentsOf: fileManager.urls(for: .cachesDirectory, in: .userDomainMask))
            resolved.append(contentsOf: fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask))
            resolved.append(fileManager.temporaryDirectory)
            resolved.append(URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true))
            #if os(macOS)
            resolved.append(fileManager.homeDirectoryForCurrentUser)
            #endif
        }

        var unique: [String: URL] = [:]
        for directory in resolved {
            let sanitized = directory.standardizedFileURL.resolvingSymlinksInPath()
            unique[sanitized.path] = sanitized
        }
        return Array(unique.values)
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
public extension SpeechSession {
    /// Stream transcription results for an audio file from disk.
    ///
    /// - Parameters:
    ///   - audioFile: Location of the audio file to transcribe.
    ///   - options: Additional configuration such as contextual strings or duration limits.
    ///   - progressHandler: Optional closure invoked with progress (0...1) as the file is processed.
    /// - Returns: An async throwing stream emitting results as transcription progresses.
    func streamTranscription(
        from audioFile: URL,
        options: FileTranscriptionOptions = .init(),
        progressHandler: (@Sendable (Double) -> Void)? = nil
    ) -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        let (stream, newContinuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()

        guard continuation == nil, recognizerTask == nil,
              fileTranscription == nil, streamingMode == .inactive else {
            newContinuation.finish(throwing: SpeechSessionError.recognitionStreamSetupFailed)
            return stream
        }

        let run = FileTranscriptionRun()
        fileTranscription = run
        newContinuation.onTermination = { [weak self, weak run] _ in
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                self.retireFileTranscription(run, error: nil)
            }
        }

        setStatus(.preparing)
        continuation = .speech(newContinuation)

        run.setupTask = Task { @MainActor [weak self] in
            guard let self else { return }
            await self.startFilePipeline(
                run: run,
                with: newContinuation,
                audioFileURL: audioFile,
                options: options,
                progressHandler: progressHandler
            )
        }

        return stream
    }

    /// Convenience wrapper that collects final results from file transcription.
    ///
    /// - Parameters:
    ///   - audioFile: Location of the audio file to transcribe.
    ///   - options: Additional configuration such as contextual strings or duration limits.
    ///   - progressHandler: Optional closure invoked with progress (0...1) as the file is processed.
    /// - Returns: Aggregated final results once transcription completes.
    /// - Throws: `SpeechSessionError` when validation or transcription fails.
    func transcribe(
        audioFile: URL,
        options: FileTranscriptionOptions = .init(),
        progressHandler: (@Sendable (Double) -> Void)? = nil
    ) async throws -> [SpeechTranscriber.Result] {
        let stream = streamTranscription(
            from: audioFile,
            options: options,
            progressHandler: progressHandler
        )
        let accumulator: @Sendable (
            inout [SpeechTranscriber.Result],
            SpeechTranscriber.Result
        ) -> Void = { results, result in
            if result.isFinal {
                results.append(result)
            }
        }
        let finalResults = try await stream.reduce(into: [SpeechTranscriber.Result](), accumulator)

        return finalResults
    }
}

// MARK: - Private helpers

@available(iOS 26.0, macOS 26.0, *)
private extension SpeechSession {
    func startFilePipeline(
        run: FileTranscriptionRun,
        with streamContinuation: AsyncThrowingStream<SpeechTranscriber.Result, Error>.Continuation,
        audioFileURL: URL,
        options: FileTranscriptionOptions,
        progressHandler: (@Sendable (Double) -> Void)?
    ) async {
        do {
            try requireCurrentFileTranscription(run)
            let validation = try validateAudioFile(at: audioFileURL, options: options)
            if let authorize = fileTranscriptionHooks?.authorize {
                try await authorize()
            } else {
                try await ensureSpeechRecognitionAuthorization(context: "for file transcription")
            }
            try requireCurrentFileTranscription(run)
            if let preparePipeline = fileTranscriptionHooks?.preparePipeline {
                try await preparePipeline(run)
            } else {
                try await setUpFilePipeline(
                    run: run,
                    with: streamContinuation,
                    contextualStrings: options.contextualStrings
                )
            }
            try requireCurrentFileTranscription(run)
            let handler = progressHandler
            let ingest = fileTranscriptionHooks?.ingest
            run.ingestionTask = Task<Void, Never> { [weak self] in
                guard let self else { return }

                do {
                    try self.requireCurrentFileTranscription(run)
                    let completed: Bool
                    if let ingest {
                        completed = try await ingest()
                    } else {
                        completed = try await self.feedAudioFile(validation, progressHandler: handler)
                    }
                    try self.requireCurrentFileTranscription(run)
                    if completed, let handler {
                        handler(1.0)
                    }
                } catch is CancellationError {
                    // Swallow cancellation triggered by cleanup.
                } catch {
                    self.retireFileTranscription(run, error: error)
                }
            }
        } catch is CancellationError {
            retireFileTranscription(run, error: nil)
        } catch {
            if Self.shouldLog(.error) {
                Self.logger.error("File transcription pipeline failed: \(error.localizedDescription, privacy: .public)")
            }
            retireFileTranscription(run, error: error)
        }
    }

    func validateAudioFile(
        at url: URL,
        options: FileTranscriptionOptions
    ) throws -> (file: AVAudioFile, totalFrames: AVAudioFramePosition) {
        let safeURL = try sanitizeAudioFileURL(
            url,
            allowedDirectories: options.allowedDirectories
        )

        let audioFile: AVAudioFile
        do {
            audioFile = try AVAudioFile(forReading: safeURL)
        } catch {
            throw SpeechSessionError.audioFileReadFailed(error)
        }

        audioFile.framePosition = 0

        let totalFrames = audioFile.length
        guard totalFrames > 0 else {
            let description = String(describing: audioFile.fileFormat.settings)
            throw SpeechSessionError.audioFileUnsupportedFormat(description)
        }

        if let maxDuration = options.maxDuration {
            let duration = Double(totalFrames) / audioFile.processingFormat.sampleRate
            if duration > maxDuration {
                throw SpeechSessionError.audioFileTooLong(maximum: maxDuration, actual: duration)
            }
        }

        return (audioFile, totalFrames)
    }

    func sanitizeAudioFileURL(
        _ url: URL,
        allowedDirectories: [URL]
    ) throws -> URL {
        guard url.isFileURL else {
            throw SpeechSessionError.audioFileInvalidURL(url)
        }

        let standardized = url.standardizedFileURL.resolvingSymlinksInPath()
        let targetComponents = standardized.pathComponents
        let allowedComponentSets = allowedDirectories
            .map { $0.standardizedFileURL.resolvingSymlinksInPath().pathComponents }

        guard FileManager.default.fileExists(atPath: standardized.path) else {
            throw SpeechSessionError.audioFileNotFound(standardized)
        }

        let isWithinAllowedDirectory = allowedComponentSets.contains { allowedComponents in
            guard allowedComponents.count <= targetComponents.count else { return false }
            return targetComponents.starts(with: allowedComponents)
        }

        guard isWithinAllowedDirectory else {
            throw SpeechSessionError.audioFileOutsideAllowedDirectories(standardized)
        }

        return standardized
    }

    func setUpFilePipeline(
        run: FileTranscriptionRun,
        with streamContinuation: AsyncThrowingStream<SpeechTranscriber.Result, Error>.Continuation,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]?
    ) async throws {
        if Self.shouldLog(.notice) {
            Self.logger.notice("Starting file transcription pipeline")
        }

        let transcriber = try await setUpSpeechTranscriber(contextualStrings: contextualStrings)
        try requireCurrentFileTranscription(run)
        activeResultKind = .speech

        run.recognizerTask = Task<Void, Never> { [weak self] in
                guard let self else { return }

                do {
                    for try await result in transcriber.results {
                        try self.requireCurrentFileTranscription(run)
                        streamContinuation.yield(result)
                    }
                    if Self.shouldLog(.notice) {
                        Self.logger.notice("File recognizer task completed without error")
                    }
                    self.retireFileTranscription(run, error: nil)
                } catch is CancellationError {
                    if Self.shouldLog(.debug) {
                        Self.logger.debug("File recognizer task cancelled")
                    }
                } catch {
                    if Self.shouldLog(.error) {
                        Self.logger.error(
                            "File recognizer task failed: \(error.localizedDescription, privacy: .public)"
                        )
                    }
                    self.retireFileTranscription(run, error: error)
                }
        }

        streamingMode = .filePlayback
        setStatus(.transcribing)
        if Self.shouldLog(.info) {
            Self.logger.info("File transcription pipeline active (mode: file playback)")
        }
    }

    nonisolated func feedAudioFile(
        _ payload: (file: AVAudioFile, totalFrames: AVAudioFramePosition),
        progressHandler: (@Sendable (Double) -> Void)?
    ) async throws -> Bool {
        var processedFrames: AVAudioFramePosition = 0
        let frameCapacity: AVAudioFrameCount = 4096
        let file = payload.file
        let totalFrames = payload.totalFrames
        let format = file.processingFormat

        guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frameCapacity) else {
            throw SpeechSessionError.conversionBufferCreationFailed
        }

        while processedFrames < totalFrames {
            if Task.isCancelled { break }

            let framesRemaining = AVAudioFrameCount(totalFrames - processedFrames)
            let framesToRead = min(frameCapacity, framesRemaining)

            buffer.frameLength = framesToRead

            do {
                try file.read(into: buffer, frameCount: framesToRead)
            } catch {
                throw SpeechSessionError.audioFileReadFailed(error)
            }

            if buffer.frameLength == 0 {
                break
            }

            guard let bufferCopy = buffer.copy() as? AVAudioPCMBuffer else {
                throw SpeechSessionError.conversionBufferCreationFailed
            }

            let sendableBuffer = SendablePCMBuffer(buffer: bufferCopy)

            try await self.processAudioBuffer(sendableBuffer)

            processedFrames += AVAudioFramePosition(buffer.frameLength)

            if let progressHandler, totalFrames > 0 {
                let progress = min(1.0, max(0.0, Double(processedFrames) / Double(totalFrames)))
                await MainActor.run {
                    progressHandler(progress)
                }
            }
        }

        await MainActor.run {
            self.inputBuilder?.finish()
        }

        return !Task.isCancelled && processedFrames >= totalFrames
    }

}
