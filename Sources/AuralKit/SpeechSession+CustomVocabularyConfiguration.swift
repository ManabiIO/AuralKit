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
    /// - Note: The session must remain idle during compilation. Competing starts and configuration
    ///   calls are rejected. Cancel the calling task to abandon an in-progress configuration;
    ///   `cancelTranscribing()` only cancels transcription runs.
    /// - Throws: A `SpeechSessionError` when the session is active, the descriptor locale mismatches, or
    ///   compilation fails.
    public func configureCustomVocabulary(_ vocabulary: CustomVocabulary?) async throws {
        guard canStartTranscription else {
            throw SpeechSessionError.customVocabularyRequiresIdleSession
        }
        isConfiguringCustomVocabulary = true
        defer { isConfiguringCustomVocabulary = false }
        try await applyCustomVocabulary(vocabulary)
    }

    /// Called with either idle configuration admission or a live setup run held.
    private func applyCustomVocabulary(_ vocabulary: CustomVocabulary?) async throws {
        try Task.checkCancellation()

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

        let compilation = try await compileCustomVocabulary(vocabulary)
        // Native compilation may finish after cancellation. Do not replace the
        // previous configuration or start capture with those abandoned results.
        try Task.checkCancellation()

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

    private func compileCustomVocabulary(_ vocabulary: CustomVocabulary) async throws -> CustomVocabularyCompilation {
        // Keep heavy compilation off the main actor, but retain and join its
        // task even when the native compiler cannot stop immediately.
        let compiler = customVocabularyCompiler
        let compilationTask = Task.detached(priority: .userInitiated) {
            try await compiler.compile(descriptor: vocabulary)
        }
        return try await withTaskCancellationHandler {
            try await compilationTask.value
        } onCancel: {
            compilationTask.cancel()
        }
    }

    /// Start a dictation-backed transcription stream that leverages the currently configured custom vocabulary.
    ///
    /// - Parameter contextualStrings: Optional contextual strings to bias recognition alongside the custom
    ///   language model.
    /// - Returns: An async throwing stream producing `DictationTranscriber.Result` values.
    public func startDictationTranscribing(
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) -> AsyncThrowingStream<DictationTranscriber.Result, Error> {
        let (stream, newContinuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()

        guard canStartTranscription else {
            newContinuation.finish(throwing: SpeechSessionError.recognitionStreamSetupFailed)
            return stream
        }

        let generation = beginLiveTranscription().generation
        newContinuation.onTermination = { [weak self] termination in
            guard case .cancelled = termination else { return }
            Task { @MainActor [weak self] in
                await self?.handleStreamTermination(generation: generation)
            }
        }

        setStatus(.preparing)
        continuation = .dictation(newContinuation)

        pipelineTask = Task { @MainActor [weak self] in
            guard let self else { return }
            await self.startDictationPipeline(
                with: newContinuation,
                contextualStrings: contextualStrings,
                generation: generation
            )
        }

        return stream
    }

    /// Compile and activate the provided custom vocabulary, then begin dictation-backed transcription.
    ///
    /// - Parameters:
    ///   - customVocabulary: The descriptor describing phrases, pronunciations, and templates to prefer.
    ///   - contextualStrings: Optional contextual strings to further bias the transcription.
    /// - Returns: An async throwing stream emitting dictation transcriber results enriched by the custom vocabulary.
    ///   Returns after compilation and pipeline startup complete. Canceling this task or calling
    ///   `cancelTranscribing()` during startup joins compilation without starting capture afterward.
    /// - Throws: Errors encountered while configuring the vocabulary or starting the dictation pipeline.
    public func startTranscribing(
        customVocabulary: CustomVocabulary,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async throws -> AsyncThrowingStream<DictationTranscriber.Result, Error> {
        try Task.checkCancellation()
        guard canStartTranscription else { throw SpeechSessionError.recognitionStreamSetupFailed }
        let (stream, newContinuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()
        let run = beginLiveTranscription()
        newContinuation.onTermination = { [weak self] termination in
            guard case .cancelled = termination else { return }
            Task { @MainActor [weak self] in
                await self?.handleStreamTermination(generation: run.generation)
            }
        }
        continuation = .dictation(newContinuation)
        setStatus(.preparing)
        let setup = Task { @MainActor in
            do {
                try self.checkLiveSetupAdmission(generation: run.generation)
                try await self.applyCustomVocabulary(customVocabulary)
                try self.checkLiveSetupAdmission(generation: run.generation)
                await self.startDictationPipeline(
                    with: newContinuation, contextualStrings: contextualStrings, generation: run.generation
                )
            } catch is CancellationError {
                await self.finishCancelledPipelineSetup(generation: run.generation)
            } catch {
                await self.finishWithStartupError(error, generation: run.generation)
            }
        }
        pipelineTask = setup
        try await awaitLiveSetup(setup, run: run)
        return stream
    }

    func clearCustomVocabularyArtifacts(removeDescriptor: Bool) {
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
