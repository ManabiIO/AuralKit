import Foundation
import Speech

/// One live stream owns setup, finalization and cancellation until every child
/// has returned. A producer requests termination and returns; it never joins itself.
@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class LiveTranscriptionRun {
    let generation: Int
    var terminalTask: Task<Void, Never>?
    var finalizationTask: Task<Void, Error>?
    var cancellationTask: Task<Void, Never>?
    var nativeAnalysisTask: Task<Void, Never>?
    var captureControlTask: Task<Void, Error>?
    var cancelRequested = false
    var error: Error?

    init(generation: Int) { self.generation = generation }
}

/// Instance-local native boundaries used by deterministic lifecycle tests.
@available(iOS 26.0, macOS 26.0, *)
struct LiveTranscriptionHooks {
    var authorize: (@MainActor @Sendable () async throws -> Void)?
    var preparePipeline: (@MainActor @Sendable (Int) async throws -> Void)?
    var finalizeInput: (@MainActor @Sendable () async throws -> Void)?
    var cancelAnalyzer: (@MainActor @Sendable () async -> Void)?
    var configureAudioSession: (@MainActor @Sendable () throws -> Void)?
    var pauseCapture: (@MainActor @Sendable () async -> Void)?
    var resumeCapture: (@MainActor @Sendable () async throws -> Void)?
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    /// Compatibility alias for `cancelTranscribing()`, safe to call while idle.
    /// Use `finishTranscribing()` when the final words must be preserved.
    public func stopTranscribing() async {
        await cancelTranscribing()
    }

    /// Stop accepting live audio, finalize all accepted input, and drain the
    /// recognizer's final results before closing the public stream.
    ///
    /// Keep consuming the result stream while awaiting this method. Canceling
    /// that consumer instead requests cancellation and can discard final text.
    /// Concurrent finishes join the same operation. `cancelTranscribing()` can
    /// interrupt a finish. A finish during startup cancels the unready pipeline.
    /// For files, this waits for the complete file and its normal finalization.
    /// Recognition/finalization errors are reported through the result stream.
    public func finishTranscribing() async {
        if let run = fileTranscription {
            await run.setupTask?.value
            await run.ingestionTask?.value
            await run.recognizerTask?.value
            await run.terminalTask?.value
            return
        }
        await requestLiveTermination(generation: sessionGeneration, gracefully: true)?.value
    }

    /// Stop immediately, cancel pending recognition, and join all owned work.
    /// Already-delivered results remain valid; pending final results may be lost.
    /// The host's audio session and external capture engine are never stopped.
    public func cancelTranscribing() async {
        if let run = fileTranscription {
            await retireFileTranscription(run, error: nil)?.value
            return
        }
        await requestLiveTermination(generation: sessionGeneration, gracefully: false)?.value
    }

    var canStartTranscription: Bool {
        fileTranscription == nil && liveTranscription == nil && continuation == nil
            && pipelineTask == nil && recognizerTask == nil && streamingMode == .inactive
            && !isConfiguringCustomVocabulary
    }

    /// Async starts keep ownership while awaiting setup, including operations
    /// such as model compilation that may not immediately honor cancellation.
    func awaitLiveSetup(_ setup: Task<Void, Never>, run: LiveTranscriptionRun) async throws {
        await withTaskCancellationHandler {
            await setup.value
        } onCancel: {
            // Set the setup flag immediately, before the main-actor teardown
            // can run, so an awaited compiler or authorization cannot resume
            // into additional setup in the intervening scheduling window.
            setup.cancel()
            Task { @MainActor [weak self] in
                self?.requestLiveTermination(generation: run.generation, gracefully: false)
            }
        }
        if Task.isCancelled || run.cancelRequested || liveTranscription !== run {
            await requestLiveTermination(generation: run.generation, gracefully: false)?.value
            throw run.error ?? CancellationError()
        }
        if let error = run.error { throw error }
    }

    func beginLiveTranscription() -> LiveTranscriptionRun {
        let run = LiveTranscriptionRun(generation: beginStreamGeneration())
        liveTranscription = run
        return run
    }

    func checkLiveSetupAdmission(generation: Int) throws {
        try Task.checkCancellation()
        guard generation == sessionGeneration,
              liveTranscription?.terminalTask == nil else { throw CancellationError() }
    }

    @discardableResult
    func requestLiveTermination(
        generation: Int, gracefully: Bool, error: Error? = nil
    ) -> Task<Void, Never>? {
        guard generation == sessionGeneration, fileTranscription == nil else { return nil }
        guard liveTranscription != nil || continuation != nil || pipelineTask != nil else { return nil }
        let run = liveTranscription ?? LiveTranscriptionRun(generation: generation)
        liveTranscription = run
        if run.error == nil { run.error = error }
        if !gracefully || status == .preparing || error != nil {
            run.cancelRequested = true
            pipelineTask?.cancel()
            recognizerTask?.cancel()
            nativeCaptureAnalysisTask?.cancel()
            run.nativeAnalysisTask?.cancel()
            run.captureControlTask?.cancel()
            run.finalizationTask?.cancel()
            startLiveAnalyzerCancellation(run)
        }
        if let terminal = run.terminalTask { return terminal }

        prepareForStop()
        // Stop admission synchronously. A retained handle is tied to this
        // continuation and cannot feed a later stream on the same session.
        externalAudioInput?.finish()
        stopAudioStreaming()
        let terminal = Task { @MainActor in await self.completeLiveTermination(run) }
        run.terminalTask = terminal
        return terminal
    }

    private func startLiveAnalyzerCancellation(_ run: LiveTranscriptionRun) {
        guard run.cancellationTask == nil else { return }
        let cancel = liveTranscriptionHooks?.cancelAnalyzer
        guard analyzer != nil || cancel != nil else { return }
        let activeAnalyzer = analyzer
        run.cancellationTask = Task { @MainActor in
            if let cancel {
                await cancel()
            } else {
                await activeAnalyzer?.cancelAndFinishNow()
            }
        }
    }

    private func completeLiveTermination(_ run: LiveTranscriptionRun) async {
        // The screen picker owns a checked continuation. Stop it before joining
        // setup, because task cancellation alone cannot resume that continuation.
        await stopScreenCaptureStreamingIfNeeded()
        // A paused/resumed native provider can still be completing an async
        // control operation. Join it before finalizing or admitting another run.
        try? await run.captureControlTask?.value
        // Setup can resume from an uncancellable authorization/model operation.
        // Keep admission closed and join it before detaching shared resources.
        await pipelineTask?.value
        guard liveTranscription === run else { return }
        pipelineTask = nil
        externalAudioInput?.finish()
        externalAudioInput = nil
        stopAudioStreaming()
        let nativeAnalysis = endNativeCaptureInput()
        run.nativeAnalysisTask = nativeAnalysis
        inputBuilder?.finish()
        await stopScreenCaptureStreamingIfNeeded()

        if !run.cancelRequested { await finalizeLiveInput(run, nativeAnalysis: nativeAnalysis) }

        if run.cancelRequested {
            recognizerTask?.cancel()
            nativeAnalysis?.cancel()
            startLiveAnalyzerCancellation(run)
        }
        await run.cancellationTask?.value
        await nativeAnalysis?.value
        // Analyzer finalization publishes results; it does not mean the
        // recognizer task has forwarded them to the public stream yet.
        await recognizerTask?.value
        guard liveTranscription === run else { return }
        recognizerTask = nil
        _ = detachTranscriberState()
        deactivateAudioSessionIfNeeded()
#if os(iOS)
        shouldResumeAfterInterruption = false
#endif
        await modelManager.releaseLocales()
        guard liveTranscription === run else { return }
        streamingMode = .inactive
        activeResultKind = nil
        setStatus(.idle)
        await finishStream(error: run.error, generation: run.generation)
        liveTranscription = nil
        run.cancellationTask = nil
        run.nativeAnalysisTask = nil
        run.terminalTask = nil
    }

    private func finalizeLiveInput(_ run: LiveTranscriptionRun, nativeAnalysis: Task<Void, Never>?) async {
        let activeAnalyzer = analyzer
        let finalize = liveTranscriptionHooks?.finalizeInput
        let finalization = Task<Void, Error> { @MainActor in
            if let finalize {
                try await finalize()
            } else if let nativeAnalysis {
                // Native analysis finalizes after its input has drained.
                await nativeAnalysis.value
            } else {
                try await activeAnalyzer?.finalizeAndFinishThroughEndOfInput()
            }
        }
        run.finalizationTask = finalization
        do {
            try await finalization.value
        } catch {
            if !run.cancelRequested { run.error = error }
            run.cancelRequested = true
        }
        run.finalizationTask = nil
    }
}
