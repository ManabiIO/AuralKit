import Foundation
import Speech

/// One live capture stream owns setup, result delivery and admitted buffer work.
/// Producers request retirement and return; the terminal task performs the joins.
@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class LiveTranscriptionRun {
    var microphoneInput: MicrophoneAudioIngress?
    var externalInput: ExternalAudioInput?
    var ready = false
    var startupError: Error?
    var captureControlTask: Task<Void, Error>?
    var captureControlDestination: SpeechSession.Status?
    var captureControlID: UUID?
    var usesNativeInput = false
    var inputCompleted = false
    var resultsCompleted = false
    var inputTask: Task<Void, Never>?
    var setupTask: Task<Void, Never>?
    var recognizerTask: Task<Void, Never>?
    var audioTasks: [UUID: Task<Void, Never>] = [:]
    var terminalTask: Task<Void, Never>?
    var finishTask: Task<Void, Never>?
    var screenStopTask: Task<Void, Error>?
    var terminalError: Error?
    var cancellationRequested = false
    var finishing = false
    var retired = false

    func releaseRetiredResources() {
        externalInput = nil
        captureControlTask = nil
        captureControlDestination = nil
        captureControlID = nil
        inputTask = nil
        setupTask = nil
        recognizerTask = nil
        audioTasks.removeAll()
        terminalTask = nil
        finishTask = nil
        screenStopTask = nil
    }
}

/// Instance-local collaborators for deterministic lifecycle tests. Nil hooks
/// retain native permissions, SpeechAnalyzer setup and microphone capture.
@available(iOS 26.0, macOS 26.0, *)
struct LiveTranscriptionHooks {
    var authorize: (@MainActor @Sendable () async throws -> Void)?
    var preparePipeline: (
        @MainActor @Sendable (LiveTranscriptionRun, SpeechSession.TranscriptionResultKind) async throws -> Void
    )?
    var startAudio: (@MainActor @Sendable () throws -> Void)?
    var processAudio: (@MainActor @Sendable (SpeechSession.SendablePCMBuffer) async throws -> Void)?
    var configureAudioSession: (@MainActor @Sendable () throws -> Void)?
    var finalizeInput: (@MainActor @Sendable () async throws -> Bool)?
    var cancelAnalyzer: (@MainActor @Sendable () async -> Void)?
    var pauseCapture: (@MainActor @Sendable () async -> Void)?
    var resumeCapture: (@MainActor @Sendable () async throws -> Void)?
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    var canStartTranscription: Bool {
        continuation == nil && recognizerTask == nil && fileTranscription == nil
            && liveTranscription == nil && streamingMode == .inactive
            && customVocabularyPreparationCount == 0
    }

    /// Await the actual setup owner, retaining success even if results complete
    /// before this caller resumes. Cancellation joins teardown before throwing.
    func awaitLiveSetup(_ setup: Task<Void, Never>, run: LiveTranscriptionRun) async throws {
        await withTaskCancellationHandler {
            await setup.value
        } onCancel: {
            setup.cancel()
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                self.retireLiveTranscription(run, error: CancellationError())
            }
        }
        if Task.isCancelled {
            await retireLiveTranscription(run, error: CancellationError())?.value
            throw CancellationError()
        }
        if !run.ready || run.startupError != nil {
            await retireLiveTranscription(run, error: run.startupError)?.value
            throw run.startupError ?? run.terminalError ?? CancellationError()
        }
    }

    func requireCurrentLiveTranscription(_ run: LiveTranscriptionRun) throws {
        try Task.checkCancellation()
        guard liveTranscription === run, !run.retired else { throw CancellationError() }
    }

    func makeLiveRecognizerTask<Sequence: AsyncSequence>(
        run: LiveTranscriptionRun,
        results: Sequence,
        vocabularyArtifacts: CustomVocabularyArtifacts? = nil,
        streamContinuation: AsyncThrowingStream<Sequence.Element, Error>.Continuation
    ) -> Task<Void, Never>
    where Sequence: Sendable, Sequence.Element: Sendable {
        // Setup may already have started the analyzer. Retain its artifact owner
        // from task creation, including before this reader first runs.
        Task<Void, Never> { @MainActor [self, vocabularyArtifacts] in
            defer { withExtendedLifetime(vocabularyArtifacts) {} }
            do {
                for try await result in results {
                    try self.requireCurrentLiveTranscription(run)
                    streamContinuation.yield(result)
                }
                try Task.checkCancellation()
                self.completeLiveResults(run)
            } catch {
                // Independent result cancellation must also settle its stream.
                self.retireLiveTranscription(run, error: error)
            }
        }
    }

    func completeLiveInput(_ run: LiveTranscriptionRun) {
        guard liveTranscription === run, !run.retired else { return }
        run.inputCompleted = true
        if run.resultsCompleted { retireLiveTranscription(run, error: nil) }
    }

    private func completeLiveResults(_ run: LiveTranscriptionRun) {
        guard liveTranscription === run, !run.retired else { return }
        run.resultsCompleted = true
        if (!run.usesNativeInput && !run.finishing) || run.inputCompleted {
            retireLiveTranscription(run, error: nil)
        }
    }

    func enqueueLiveAudioBuffer(_ buffer: SendablePCMBuffer, run: LiveTranscriptionRun) {
        guard liveTranscription === run, !run.retired, !run.finishing else { return }
        let id = UUID()
        run.audioTasks[id] = Task { @MainActor [weak self] in
            defer { run.audioTasks[id] = nil }
            guard let self else { return }
            do {
                try self.requireCurrentLiveTranscription(run)
                if let processAudio = self.liveTranscriptionHooks?.processAudio {
                    try await processAudio(buffer)
                    try self.requireCurrentLiveTranscription(run)
                } else {
                    try await self.processAudioBuffer(buffer, liveRun: run)
                }
            } catch is CancellationError {
                // Retirement owns the terminal outcome and joins this task.
            } catch {
                if Self.shouldLog(.error) {
                    Self.logger.error("Audio processing error: \(error.localizedDescription, privacy: .public)")
                }
            }
        }
    }

    func finishLivePipelineSetupFailure(_ error: Error, run: LiveTranscriptionRun) {
        guard liveTranscription === run else { return }
        if run.finishing, Task.isCancelled { return }
        if !run.ready { run.startupError = error }
        retireLiveTranscription(run, error: error)
    }

    /// Finish drains accepted input; the same retirement owner still performs cleanup.
    func finishLiveTranscription(_ run: LiveTranscriptionRun, sourceEnded: Bool = false) async {
        guard liveTranscription === run else { return }
        if let terminal = run.terminalTask { await terminal.value; return }
        if let finish = run.finishTask {
            await finish.value
            await run.terminalTask?.value
            return
        }
        guard (status != .preparing || sourceEnded), let results = run.recognizerTask else {
            await retireLiveTranscription(run, error: nil)?.value
            return
        }
        run.finishing = true
        if status == .preparing { run.setupTask?.cancel() }
        prepareForStop()
        run.externalInput?.finish()
        stopAudioStreaming()
        let screenStop = beginStoppingScreenCapture(run: run)
        let finish = Task { @MainActor in
            do {
                try await screenStop?.value
                // Controls request retirement but never join their own terminal owner.
                try? await run.captureControlTask?.value
                try self.requireCurrentLiveTranscription(run)
                let audioTasks = Array(run.audioTasks.values)
                for task in audioTasks { await task.value }
                try self.requireCurrentLiveTranscription(run)
                try await self.finishAcceptedLiveInput(run)
                await results.value
            } catch {
                // Producers request retirement without joining their own finish task.
                self.retireLiveTranscription(run, error: error)
            }
        }
        run.finishTask = finish
        await finish.value
        await run.terminalTask?.value
    }

    private func finishAcceptedLiveInput(_ run: LiveTranscriptionRun) async throws {
        if let nativeInput = run.inputTask {
            tearDownNativeCaptureStreaming()
            await nativeInput.value
        } else {
            let hasResults: Bool
            if let finalize = liveTranscriptionHooks?.finalizeInput {
                hasResults = try await finalize()
            } else {
                hasResults = try await finishAnalyzerInput()
            }
            try requireCurrentLiveTranscription(run)
            if !hasResults {
                retireLiveTranscription(run, error: nil)
            } else {
                // Input and finalization must settle even when
                // result EOF arrived first. A late failure still wins.
                completeLiveInput(run)
            }
        }
    }

    @discardableResult
    func retireLiveTranscription(_ run: LiveTranscriptionRun, error: Error?) -> Task<Void, Never>? {
        guard liveTranscription === run else { return nil }
        if let terminal = run.terminalTask { return terminal }
        run.retired = true
        run.externalInput?.finish()
        run.captureControlTask?.cancel()
        // Cancellation closes input immediately; teardown still joins capture and setup.
        inputBuilder?.finish()
        inputBuilder = nil
        run.terminalError = error
        prepareForStop()
        stopAudioStreaming()
        run.inputTask?.cancel()
        run.setupTask?.cancel()
        run.recognizerTask?.cancel()
        run.finishTask?.cancel()
        run.audioTasks.values.forEach { $0.cancel() }
        let screenStop = beginStoppingScreenCapture(run: run)
        let terminal = Task { @MainActor in
            // Dismiss pending selection before joining setup, which may be waiting on it.
            if case .failure(let stopError)? = await screenStop?.result, run.terminalError == nil {
                run.terminalError = stopError
            }
            await run.setupTask?.value
            try? await run.captureControlTask?.value
            run.inputTask?.cancel()
            // Setup may install a result reader after cancellation was requested.
            run.recognizerTask?.cancel()
            let detectorTask = self.speechDetectorResultsTask
            detectorTask?.cancel()
            let audioTasks = Array(run.audioTasks.values)
            audioTasks.forEach { $0.cancel() }
            for task in audioTasks { await task.value }
            guard self.liveTranscription === run else { return }
            await self.cleanupPipelineResources(
                cancelRecognizer: true, publishIdle: false, inputTask: run.inputTask,
                finishTask: run.finishTask
            )
            await run.recognizerTask?.value
            await detectorTask?.value
            guard self.liveTranscription === run else { return }
            self.setStatus(.idle)
            await self.finishStream(error: run.cancellationRequested ? CancellationError() : run.terminalError)
            self.liveTranscription = nil
            run.releaseRetiredResources()
        }
        run.terminalTask = terminal
        return terminal
    }
}
