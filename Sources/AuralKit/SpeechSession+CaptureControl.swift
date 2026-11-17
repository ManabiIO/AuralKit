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

    /// Pause capture without tearing down the analyzer pipeline.
    ///
    /// Safe to call only when the session is actively transcribing live audio (microphone or
    /// screen capture). Calls during file transcription or in any other state are ignored.
    public func pauseTranscribing() async {
        guard status == .transcribing, isPausableStreamingMode,
              let run = liveTranscription, !run.retired else { return }
        if streamingMode == .screenCapture {
            return await pauseScreenCaptureStreamingIfNeeded(run: run)
        }
        stopAudioStreaming()
        guard liveTranscription === run, !run.retired else { return }
        setStatus(.paused)
    }

    /// Resume a paused capture session.
    /// - Throws: `SpeechSessionError` if audio streaming cannot restart.
    public func resumeTranscribing() async throws {
        guard status == .paused, isPausableStreamingMode,
              let run = liveTranscription else { return }
        do {
            try requireCurrentLiveTranscription(run)
            if streamingMode == .screenCapture {
                try await resumeScreenCaptureStreamingIfNeeded(run: run)
                return
            } else {
                try startAudioStreaming()
            }
            setStatus(.transcribing)
        } catch {
            await retireLiveTranscription(run, error: error)?.value
            throw error
        }
    }
}
