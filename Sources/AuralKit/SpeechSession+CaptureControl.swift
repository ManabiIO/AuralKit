import Foundation

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    /// Stop capture and join every admitted setup, input and result task.
    ///
    /// Safe to call even if `startTranscribing()` has not been invoked or the stream has already
    /// completed; the method simply waits for cleanup and returns.
    /// For live transcription, waits for setup, admitted audio processing and result
    /// delivery to exit. For file transcription, also waits for setup, file ingestion and result
    /// delivery to exit before returning. The caller may then release its audio
    /// file lease or remove its request-owned workspace.
    public func stopTranscribing() async {
        if let run = liveTranscription {
            await retireLiveTranscription(run, error: nil)?.value
            return
        }
        if let run = fileTranscription {
            await retireFileTranscription(run, error: nil)?.value
            return
        }
        // Every accepted transcription owns a run before setup can suspend.
        // An idle stop must not start unowned cleanup that can overlap a new run.
    }

    /// Drain accepted audio and deliver final results before returning.
    /// Preparing streams are closed without starting capture.
    public func finishTranscribing() async {
        if let run = liveTranscription {
            await finishLiveTranscription(run)
        } else if let run = fileTranscription {
            if status == .preparing {
                await retireFileTranscription(run, error: nil)?.value
            } else {
                await run.setupTask?.value
                await run.ingestionTask?.value
                await run.recognizerTask?.value
                await run.terminalTask?.value
            }
        }
    }

    /// Abandon the run and join its teardown. The stream ends with cancellation.
    public func cancelTranscribing() async {
        if let run = liveTranscription {
            run.cancellationRequested = true
            await retireLiveTranscription(run, error: CancellationError())?.value
        } else if let run = fileTranscription {
            run.cancellationRequested = true
            await retireFileTranscription(run, error: CancellationError())?.value
        }
    }

    /// Concurrent controls of the same kind share one native operation. Finish
    /// and retirement join that operation before releasing run resources.
    public func pauseTranscribing() async {
        try? await transitionCapture(from: .transcribing, to: .paused) { run in
            if let pause = self.liveTranscriptionHooks?.pauseCapture {
                await pause()
            } else if self.streamingMode == .screenCapture {
                await self.pauseScreenCaptureStreamingIfNeeded(run: run)
            } else {
                self.stopAudioStreaming()
            }
        }
    }

    public func resumeTranscribing() async throws {
        try await transitionCapture(from: .paused, to: .transcribing) { run in
            if let resume = self.liveTranscriptionHooks?.resumeCapture {
                try await resume()
            } else if self.streamingMode == .screenCapture {
                try await self.resumeScreenCaptureStreamingIfNeeded(run: run)
            } else {
                try self.startAudioStreaming()
            }
        }
    }

    private func transitionCapture(
        from source: Status, to destination: Status,
        operation: @escaping @MainActor @Sendable (LiveTranscriptionRun) async throws -> Void
    ) async throws {
        guard let run = liveTranscription, !run.retired, !run.finishing,
              isPausableStreamingMode else { return }
        let task: Task<Void, Error>
        if let current = run.captureControlTask, run.captureControlDestination == destination {
            task = current
        } else {
            guard status == source else { return }
            task = makeCaptureControlTask(
                run: run, source: source, destination: destination, operation: operation
            )
        }
        do {
            try await task.value
            if run.retired {
                // A delegate can retire the run while the provider returns normally.
                // The public caller joins teardown; the control task never joins itself.
                await run.terminalTask?.value
                if !run.cancellationRequested, let error = run.terminalError { throw error }
            }
        } catch {
            // The operation only requests retirement; its public caller may join it.
            await run.terminalTask?.value
            throw error
        }
    }

    private func makeCaptureControlTask(
        run: LiveTranscriptionRun, source: Status, destination: Status,
        operation: @escaping @MainActor @Sendable (LiveTranscriptionRun) async throws -> Void
    ) -> Task<Void, Error> {
        let previous = run.captureControlTask
        let controlID = UUID()
        let controlsScreen = streamingMode == .screenCapture
        let usesHook = destination == .paused
            ? liveTranscriptionHooks?.pauseCapture != nil : liveTranscriptionHooks?.resumeCapture != nil
        let task = Task<Void, Error> { @MainActor in
            defer {
                if run.captureControlID == controlID {
                    run.captureControlTask = nil
                    run.captureControlDestination = nil
                    run.captureControlID = nil
                }
            }
            do {
                // A control admitted after native publication may run before
                // the old observer returns. Join only its predecessor task.
                try? await previous?.value
                try self.requireCurrentLiveTranscription(run)
                try await operation(run)
                guard self.liveTranscription === run, !run.retired, !run.finishing,
                      run.captureControlID == controlID else { return }
                // Screen transitions own their publication through picker replacements.
                if (!controlsScreen || usesHook), self.status == source { self.setStatus(destination) }
            } catch {
                // An interrupted Resume must not convert Finish into cancellation.
                if !run.retired, !run.finishing, run.captureControlID == controlID {
                    self.retireLiveTranscription(run, error: error)
                }
                throw error
            }
        }
        run.captureControlID = controlID
        run.captureControlDestination = destination
        run.captureControlTask = task
        return task
    }
}
