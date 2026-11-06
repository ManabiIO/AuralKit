import Foundation
import Speech
import CryptoKit
import OSLog

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {

    /// Configure the session's custom vocabulary, compiling assets as needed.
    ///
    /// - Parameter vocabulary: The vocabulary descriptor to activate, or `nil` to clear the current
    ///   configuration.
    /// - Throws: A `SpeechSessionError` when the session is active, the descriptor locale mismatches, or
    ///   compilation fails.
    public func configureCustomVocabulary(_ vocabulary: CustomVocabulary?) async throws {
        try await configureCustomVocabulary(vocabulary, liveRun: nil)
    }

    private func requireVocabularyAdmission(_ liveRun: LiveTranscriptionRun?) throws {
        try Task.checkCancellation()
        if let liveRun {
            try requireCurrentLiveTranscription(liveRun)
        } else {
            guard continuation == nil, recognizerTask == nil,
                  fileTranscription == nil, liveTranscription == nil, streamingMode == .inactive else {
                throw SpeechSessionError.customVocabularyRequiresIdleSession
            }
        }
    }

    private func configureCustomVocabulary(
        _ vocabulary: CustomVocabulary?, liveRun: LiveTranscriptionRun?
    ) async throws {
        try requireVocabularyAdmission(liveRun)
        let requestID = UUID()
        customVocabularyRequestID = requestID

        guard let vocabulary else {
            clearCustomVocabularyArtifacts(removeDescriptor: true)
            return
        }

        let requestedLocale = vocabulary.locale.identifier(.bcp47)
        let sessionLocale = locale.identifier(.bcp47)
        guard requestedLocale == sessionLocale else {
            throw SpeechSessionError.customVocabularyUnsupportedLocale(vocabulary.locale)
        }

        let cacheKey: String
        do {
            cacheKey = try vocabulary.stableCacheKey()
        } catch {
            throw SpeechSessionError.customVocabularyCompilationFailed(error)
        }

        if cacheKey == customVocabularyCacheKey,
           customVocabularyConfiguration != nil {
            customVocabularyDescriptor = vocabulary
            return
        }

        // Offload compilation to background task to avoid blocking main actor UI
        // Capture compiler before detached task to avoid cross-actor access
        let compiler = customVocabularyCompiler
        let compilation = try await Task.detached(priority: .userInitiated) {
            try await compiler.compile(descriptor: vocabulary)
        }.value

        // Detached compilers may finish after cancellation, clearing or replacement.
        // Only the admitted request may publish or remove the previous assets.
        try requireVocabularyAdmission(liveRun)
        guard customVocabularyRequestID == requestID else { throw CancellationError() }

        if let previousDirectory = customVocabularyOutputDirectory,
           previousDirectory != compilation.outputDirectory {
            do {
                try FileManager.default.removeItem(at: previousDirectory)
            } catch {
                logCustomVocabularyError(
                    "Failed to delete previous vocabulary directory.",
                    path: previousDirectory.path,
                    error: error
                )
            }
        }

        customVocabularyDescriptor = vocabulary
        customVocabularyConfiguration = compilation.configuration
        customVocabularyCacheKey = compilation.cacheKey
        customVocabularyOutputDirectory = compilation.outputDirectory
    }

    /// Start a dictation-backed transcription stream that leverages the currently configured custom vocabulary.
    ///
    /// - Parameter contextualStrings: Optional contextual strings to bias recognition alongside the custom
    ///   language model.
    /// - Returns: An async throwing stream producing `DictationTranscriber.Result` values.
    public func startDictationTranscribing(
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) -> AsyncThrowingStream<DictationTranscriber.Result, Error> {
        do {
            let (stream, newContinuation, run) = try admitDictationTranscription()
            run.setupTask = Task { @MainActor [weak self] in
                guard let self else { return }
                await self.startDictationPipeline(
                    run: run, with: newContinuation, contextualStrings: contextualStrings
                )
            }
            return stream
        } catch {
            let (stream, continuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()
            continuation.finish(throwing: error)
            return stream
        }
    }

    private func admitDictationTranscription() throws -> (
        AsyncThrowingStream<DictationTranscriber.Result, Error>,
        AsyncThrowingStream<DictationTranscriber.Result, Error>.Continuation,
        LiveTranscriptionRun
    ) {
        guard continuation == nil, recognizerTask == nil,
              fileTranscription == nil, liveTranscription == nil, streamingMode == .inactive else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        let (stream, newContinuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()
        let run = LiveTranscriptionRun()
        liveTranscription = run
        newContinuation.onTermination = { [weak self, weak run] _ in
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                self.retireLiveTranscription(run, error: nil)
            }
        }
        setStatus(.preparing)
        continuation = .dictation(newContinuation)
        return (stream, newContinuation, run)
    }

    /// Compile and activate the provided custom vocabulary, then begin dictation-backed transcription.
    ///
    /// - Parameters:
    ///   - customVocabulary: The descriptor describing phrases, pronunciations, and templates to prefer.
    ///   - contextualStrings: Optional contextual strings to further bias the transcription.
    /// - Returns: An async throwing stream emitting dictation transcriber results enriched by the custom vocabulary.
    /// - Throws: Errors encountered while configuring the vocabulary or starting the dictation pipeline.
    public func startTranscribing(
        customVocabulary: CustomVocabulary,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async throws -> AsyncThrowingStream<DictationTranscriber.Result, Error> {
        try requireVocabularyAdmission(nil)
        let (stream, newContinuation, run) = try admitDictationTranscription()
        let preparation = Task { @MainActor in
            try await self.configureCustomVocabulary(customVocabulary, liveRun: run)
            try self.requireCurrentLiveTranscription(run)
        }
        run.setupTask = Task { @MainActor in
            do {
                try await preparation.value
                try self.requireCurrentLiveTranscription(run)
                await self.startDictationPipeline(
                    run: run, with: newContinuation, contextualStrings: contextualStrings
                )
            } catch {
                self.retireLiveTranscription(run, error: error)
            }
        }
        do {
            return try await withTaskCancellationHandler {
                try await preparation.value
                try Task.checkCancellation()
                return stream
            } onCancel: {
                preparation.cancel()
                Task { @MainActor [weak self, weak run] in
                    guard let self, let run else { return }
                    self.retireLiveTranscription(run, error: nil)
                }
            }
        } catch {
            // The caller is outside setup, so it can join retirement before retrying.
            await retireLiveTranscription(run, error: error)?.value
            throw error
        }
    }

    func clearCustomVocabularyArtifacts(removeDescriptor: Bool) {
        customVocabularyRequestID = nil
        if let directory = customVocabularyOutputDirectory {
            // Best-effort cleanup of vocabulary directory
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                logCustomVocabularyError(
                    "Failed to delete vocabulary directory during cleanup.",
                    path: directory.path,
                    error: error
                )
            }
        }

        customVocabularyConfiguration = nil
        customVocabularyCacheKey = nil
        customVocabularyOutputDirectory = nil

        if removeDescriptor {
            customVocabularyDescriptor = nil
        }
    }
}
