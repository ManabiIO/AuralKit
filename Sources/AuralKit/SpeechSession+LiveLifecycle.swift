import Foundation
import Speech

/// One live capture stream owns setup, result delivery and admitted buffer work.
/// Producers request retirement and return; the terminal task performs the joins.
@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class LiveTranscriptionRun {
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
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
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
                self.retireLiveTranscription(run, error: nil)
            } catch {
                // Independent result cancellation must also settle its stream.
                self.retireLiveTranscription(run, error: error)
            }
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
        stopAudioStreaming()
        let screenStop = beginStoppingScreenCapture(run: run)
        let finish = Task { @MainActor in
            do {
                try await screenStop?.value
                try self.requireCurrentLiveTranscription(run)
                let audioTasks = Array(run.audioTasks.values)
                for task in audioTasks { await task.value }
                try self.requireCurrentLiveTranscription(run)
                if let nativeInput = run.inputTask {
                    self.tearDownNativeCaptureStreaming()
                    await nativeInput.value
                } else if try await !self.finishAnalyzerInput() {
                    self.retireLiveTranscription(run, error: nil)
                }
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

    @discardableResult
    func retireLiveTranscription(_ run: LiveTranscriptionRun, error: Error?) -> Task<Void, Never>? {
        guard liveTranscription === run else { return nil }
        if let terminal = run.terminalTask { return terminal }
        run.retired = true
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
            run.inputTask = nil
            run.setupTask = nil
            run.recognizerTask = nil
            run.audioTasks.removeAll()
            run.terminalTask = nil
            run.finishTask = nil
            run.screenStopTask = nil
        }
        run.terminalTask = terminal
        return terminal
    }
}
