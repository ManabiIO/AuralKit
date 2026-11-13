import Foundation

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    /// Pause capture without tearing down the analyzer pipeline.
    ///
    /// Calls outside live microphone/screen capture or while not transcribing are ignored.
    /// Concurrent pauses join the same operation. Finish/Cancel wait for it to return.
    public func pauseTranscribing() async {
        try? await transitionCapture(from: .transcribing, to: .paused) {
            if let pause = self.liveTranscriptionHooks?.pauseCapture {
                await pause()
            } else if self.streamingMode == .screenCapture {
                await self.pauseScreenCaptureStreamingIfNeeded()
            } else {
                self.stopAudioStreaming()
            }
        }
    }

    /// Resume a paused capture session. Concurrent resumes join the same operation.
    ///
    /// - Throws: The capture error if audio streaming cannot restart, including
    ///   `CancellationError` when an in-flight native resume is interrupted by termination.
    public func resumeTranscribing() async throws {
        try await transitionCapture(from: .paused, to: .transcribing) {
            if let resume = self.liveTranscriptionHooks?.resumeCapture {
                try await resume()
            } else if self.streamingMode == .screenCapture {
                try await self.resumeScreenCaptureStreamingIfNeeded()
            } else {
                try self.startAudioStreaming()
            }
        }
    }

    private func transitionCapture(
        from source: Status, to destination: Status,
        operation: @escaping @MainActor @Sendable () async throws -> Void
    ) async throws {
        guard status == source, isPausableStreamingMode,
              let run = liveTranscription, run.terminalTask == nil else { return }
        let task: Task<Void, Error>
        if let current = run.captureControlTask {
            task = current
        } else {
            task = Task { @MainActor in
                defer { run.captureControlTask = nil }
                do {
                    try Task.checkCancellation()
                    guard self.liveTranscription === run, run.terminalTask == nil,
                          self.status == source else { return }
                    try await operation()
                    guard self.liveTranscription === run, run.terminalTask == nil,
                          self.status == source else { return }
                    self.setStatus(destination)
                } catch {
                    // Native capture can fail while Finish is stopping its source.
                    // That terminal owner must still drain the accepted audio.
                    if !run.cancelRequested, run.terminalTask == nil {
                        self.requestLiveTermination(generation: run.generation, gracefully: false, error: error)
                    }
                    throw error
                }
            }
            run.captureControlTask = task
        }
        do {
            try await task.value
        } catch {
            // The native-operation task only requests termination; its caller
            // can join the terminal owner without making that task join itself.
            await run.terminalTask?.value
            throw error
        }
    }
}
