import Foundation
import Speech
import OSLog

/// The three actual tasks belonging to one file stream. Retirement is latched
/// before cancellation, and only its separate terminal task joins these tasks.
/// A producer must request retirement and return, never await its own join.
@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class FileTranscriptionRun {
    var setupTask: Task<Void, Never>?
    var ingestionTask: Task<Void, Never>?
    var recognizerTask: Task<Void, Never>?
    var terminalTask: Task<Void, Never>?
    var retired = false
}

/// Instance-local seams for deterministic file lifecycle tests. Nil operations
/// use the native authorization, analyzer and AVAudioFile paths.
@available(iOS 26.0, macOS 26.0, *)
struct FileTranscriptionHooks {
    var authorize: (@MainActor @Sendable () async throws -> Void)?
    var preparePipeline: (@MainActor @Sendable (FileTranscriptionRun) async throws -> Void)?
    var ingest: (@Sendable () async throws -> Bool)?
    var finalizeInput: (@MainActor @Sendable () async throws -> Void)?
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    /// Ending an autonomous analyzer's input does not finish its analysis.
    /// Keep this run and its reader active while finalization publishes the
    /// last results. The reader requests retirement after draining them.
    func finalizeFileInput(_ run: FileTranscriptionRun) async throws {
        try requireCurrentFileTranscription(run)
        inputBuilder?.finish()
        if let finalizeInput = fileTranscriptionHooks?.finalizeInput {
            try await finalizeInput()
        } else {
            try await analyzer?.finalizeAndFinishThroughEndOfInput()
        }
    }

    /// Shared by native speech results and lifecycle probes so the latter
    /// exercise the same admission checks, forwarding and terminal ordering.
    func makeFileRecognizerTask<Sequence: AsyncSequence>(
        run: FileTranscriptionRun,
        results: Sequence,
        streamContinuation: AsyncThrowingStream<Sequence.Element, Error>.Continuation
    ) -> Task<Void, Never>
    where Sequence: Sendable, Sequence.Element: Sendable {
        Task<Void, Never> { @MainActor [weak self] in
            guard let self else { return }
            do {
                for try await result in results {
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
                // A results iterator can fail independently of public stop.
                // Only an already-retired run has a teardown owner to join.
                if self.fileTranscription === run, !run.retired {
                    self.retireFileTranscription(run, error: CancellationError())
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
    }

    func requireCurrentFileTranscription(_ run: FileTranscriptionRun) throws {
        try Task.checkCancellation()
        guard fileTranscription === run, !run.retired else {
            throw CancellationError()
        }
    }

    /// File producers call synchronously and return. Public stop awaits this
    /// same task, including when stream termination or another stop raced it.
    @discardableResult
    func retireFileTranscription(
        _ run: FileTranscriptionRun, error: Error?
    ) -> Task<Void, Never>? {
        guard fileTranscription === run else { return nil }
        if let task = run.terminalTask { return task }
        run.retired = true
        prepareForStop()
        run.setupTask?.cancel()
        run.ingestionTask?.cancel()
        run.recognizerTask?.cancel()
        let terminal = Task { @MainActor in
            // Setup may have been suspended before it installed either child.
            // Join it first, then capture the final child handles.
            await run.setupTask?.value
            run.ingestionTask?.cancel()
            run.recognizerTask?.cancel()
            await run.ingestionTask?.value
            guard self.fileTranscription === run else { return }
            // Finalization ends the result sequence before joining its reader;
            // do not assume an arbitrary result iterator wakes on cancellation.
            await self.cleanupPipelineResources(cancelRecognizer: true, publishIdle: false)
            await run.recognizerTask?.value
            self.setStatus(.idle)
            await self.finishStream(error: error)
            self.fileTranscription = nil
            run.setupTask = nil
            run.ingestionTask = nil
            run.recognizerTask = nil
            run.terminalTask = nil
        }
        run.terminalTask = terminal
        return terminal
    }
}
