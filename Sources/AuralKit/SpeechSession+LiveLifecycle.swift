import Foundation
import Speech

/// One microphone stream owns setup, result delivery and admitted buffer work.
/// Producers request retirement and return; the terminal task performs the joins.
@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class LiveTranscriptionRun {
    var setupTask: Task<Void, Never>?
    var recognizerTask: Task<Void, Never>?
    var audioTasks: [UUID: Task<Void, Never>] = [:]
    var terminalTask: Task<Void, Never>?
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
        streamContinuation: AsyncThrowingStream<Sequence.Element, Error>.Continuation
    ) -> Task<Void, Never>
    where Sequence: Sendable, Sequence.Element: Sendable {
        // Setup may already have started the analyzer. Retain its artifact owner
        // from task creation, including before this reader first runs.
        Task<Void, Never> { @MainActor [self] in
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
        guard liveTranscription === run, !run.retired else { return }
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

    @discardableResult
    func retireLiveTranscription(_ run: LiveTranscriptionRun, error: Error?) -> Task<Void, Never>? {
        guard liveTranscription === run else { return nil }
        if let terminal = run.terminalTask { return terminal }
        run.retired = true
        prepareForStop()
        stopAudioStreaming()
        run.setupTask?.cancel()
        run.recognizerTask?.cancel()
        run.audioTasks.values.forEach { $0.cancel() }
        let terminal = Task { @MainActor in
            await run.setupTask?.value
            // Setup may install a result reader after cancellation was requested.
            run.recognizerTask?.cancel()
            let detectorTask = self.speechDetectorResultsTask
            detectorTask?.cancel()
            let audioTasks = Array(run.audioTasks.values)
            audioTasks.forEach { $0.cancel() }
            for task in audioTasks { await task.value }
            guard self.liveTranscription === run else { return }
            await self.cleanupPipelineResources(cancelRecognizer: true, publishIdle: false)
            await run.recognizerTask?.value
            await detectorTask?.value
            guard self.liveTranscription === run else { return }
            self.setStatus(.idle)
            await self.finishStream(error: error)
            self.liveTranscription = nil
            run.setupTask = nil
            run.recognizerTask = nil
            run.audioTasks.removeAll()
            run.terminalTask = nil
        }
        run.terminalTask = terminal
        return terminal
    }
}
