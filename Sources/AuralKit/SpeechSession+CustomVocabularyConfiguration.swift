import Foundation
import Speech
import CryptoKit
import OSLog

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {

    /// Configure the session's custom vocabulary, compiling assets as needed.
    /// Purged temporary artifacts are rebuilt. Cancellation joins compilation
    /// before rejecting its result; native consumers retain their own artifacts.
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
           customVocabularyFilesExist {
            customVocabularyDescriptor = vocabulary
            return
        }

        let compilation = try await compileCustomVocabulary(vocabulary)
        try requireVocabularyAdmission(liveRun)
        guard customVocabularyRequestID == requestID else { throw CancellationError() }
        applyCustomVocabularyCompilation(compilation, descriptor: vocabulary)
    }

    var customVocabularyFilesExist: Bool {
        guard let configuration = customVocabularyConfiguration else { return false }
        let paths = [configuration.languageModel, configuration.vocabulary].compactMap { $0 }
        return paths.allSatisfy { FileManager.default.isReadableFile(atPath: $0.path) }
    }

    func compileCustomVocabulary(_ vocabulary: CustomVocabulary) async throws -> CustomVocabularyCompilation {
        let compiler = customVocabularyCompiler
        let task = Task.detached(priority: .userInitiated) {
            try await compiler.compile(descriptor: vocabulary)
        }
        let result = await withTaskCancellationHandler {
            await task.result
        } onCancel: {
            task.cancel()
        }
        // The result's artifact owner is released only after compilation joins.
        try Task.checkCancellation()
        return try result.get()
    }

    func applyCustomVocabularyCompilation(_ compilation: CustomVocabularyCompilation, descriptor: CustomVocabulary) {
        customVocabularyDescriptor = descriptor
        customVocabularyConfiguration = compilation.configuration
        customVocabularyCacheKey = compilation.cacheKey
        customVocabularyOutputDirectory = compilation.outputDirectory
        customVocabularyArtifacts = compilation.artifacts
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
        // Active native consumers retain their own owner until teardown joins.
        customVocabularyArtifacts = nil

        customVocabularyConfiguration = nil
        customVocabularyCacheKey = nil
        customVocabularyOutputDirectory = nil

        if removeDescriptor {
            customVocabularyDescriptor = nil
        }
    }
}
