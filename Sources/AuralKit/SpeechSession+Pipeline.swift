import Foundation
import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {

    // MARK: - Permissions

    /// Check if all required permissions are granted
    func ensurePermissions() async throws {
        // Check microphone permission (iOS & macOS)
        switch AVCaptureDevice.authorizationStatus(for: .audio) {
        case .authorized:
            if Self.shouldLog(.info) {
                Self.logger.info("Microphone permission already authorized")
            }
        case .notDetermined:
            if Self.shouldLog(.notice) {
                Self.logger.notice("Requesting microphone permission")
            }
            let granted = await AVCaptureDevice.requestAccess(for: .audio)
            if !granted {
                if Self.shouldLog(.error) {
                    Self.logger.error("Microphone permission denied")
                }
                throw SpeechSessionError.microphonePermissionDenied
            }
        default:
            if Self.shouldLog(.error) {
                Self.logger.error("Microphone permission unavailable")
            }
            throw SpeechSessionError.microphonePermissionDenied
        }

        try await ensureSpeechRecognitionAuthorization()
    }

    func ensureSpeechRecognitionAuthorization(context: String? = nil) async throws {
        let suffix = context.map { " \($0)" } ?? ""

        switch SFSpeechRecognizer.authorizationStatus() {
        case .authorized:
            if Self.shouldLog(.info) {
                Self.logger.info("Speech recognition permission already authorized\(suffix)")
            }
            return
        case .notDetermined:
            if Self.shouldLog(.notice) {
                Self.logger.notice("Requesting speech recognition permission\(suffix)")
            }
            let granted = await withCheckedContinuation { continuation in
                SFSpeechRecognizer.requestAuthorization { status in
                    continuation.resume(returning: status == .authorized)
                }
            }
            if !granted {
                if Self.shouldLog(.error) {
                    Self.logger.error("Speech recognition permission denied\(suffix)")
                }
                throw SpeechSessionError.speechRecognitionPermissionDenied
            }
        default:
            if Self.shouldLog(.error) {
                Self.logger.error("Speech recognition permission unavailable\(suffix)")
            }
            throw SpeechSessionError.speechRecognitionPermissionDenied
        }
    }

    // MARK: - Pipeline Orchestration

    func startSpeechPipeline(
        run: LiveTranscriptionRun,
        with streamContinuation: AsyncThrowingStream<SpeechTranscriber.Result, Error>.Continuation,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async {
        await startLivePipeline(run: run, kind: .speech) {
            let transcriber = try await self.setUpSpeechTranscriber(contextualStrings: contextualStrings)
            try self.requireCurrentLiveTranscription(run)
            return self.makeLiveRecognizerTask(
                run: run, results: transcriber.results, streamContinuation: streamContinuation
            )
        }
    }

    func startDictationPipeline(
        run: LiveTranscriptionRun,
        with streamContinuation: AsyncThrowingStream<DictationTranscriber.Result, Error>.Continuation,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async {
        await startLivePipeline(run: run, kind: .dictation) {
            let transcriber = try await self.setUpDictationTranscriber(contextualStrings: contextualStrings)
            try self.requireCurrentLiveTranscription(run)
            return self.makeLiveRecognizerTask(
                run: run, results: transcriber.results, streamContinuation: streamContinuation
            )
        }
    }

    private func startLivePipeline(
        run: LiveTranscriptionRun,
        kind: TranscriptionResultKind,
        prepareRecognizer: @MainActor () async throws -> Task<Void, Never>
    ) async {
        do {
            try requireCurrentLiveTranscription(run)
            if let authorize = liveTranscriptionHooks?.authorize {
                try await authorize()
            } else {
                try await ensurePermissions()
            }
            try requireCurrentLiveTranscription(run)
            if let preparePipeline = liveTranscriptionHooks?.preparePipeline {
                try await preparePipeline(run, kind)
            } else {
                try await setupAudioSession(liveRun: run)
                try requireCurrentLiveTranscription(run)
                run.recognizerTask = try await prepareRecognizer()
            }
            try requireCurrentLiveTranscription(run)
            recognizerTask = run.recognizerTask
            if let startAudio = liveTranscriptionHooks?.startAudio {
                try startAudio()
            } else {
                try startAudioStreaming()
            }
            try requireCurrentLiveTranscription(run)
            streamingMode = .liveMicrophone
            activeResultKind = kind
            setStatus(.transcribing)
        } catch {
            // A producer requests retirement and returns; it never joins itself.
            retireLiveTranscription(run, error: error)
        }
    }

    func cleanupPipelineResources(cancelRecognizer: Bool, publishIdle: Bool = true) async {
        if Self.shouldLog(.debug) {
            Self.logger.debug("Cleanup started (cancelRecognizer: \(cancelRecognizer, privacy: .public))")
        }
        let task = recognizerTask
        recognizerTask = nil

        if cancelRecognizer {
            if Self.shouldLog(.debug) {
                Self.logger.debug("Cancelling recognizer task")
            }
            task?.cancel()
        }

        streamingMode = .inactive
        activeResultKind = nil
        stopAudioStreaming()
        deactivateAudioSessionIfNeeded()
#if os(iOS)
        shouldResumeAfterInterruption = false
#endif
        await stopTranscriberAndCleanup()
        if publishIdle { setStatus(.idle) }
        if Self.shouldLog(.debug) {
            Self.logger.debug("Cleanup completed")
        }
    }

    func finishStream(error: Error?) async {
        guard let continuation else { return }
        self.continuation = nil

        switch continuation {
        case .speech(let speechContinuation):
            if let error {
                if Self.shouldLog(.error) {
                    Self.logger.error(
                        "Finishing stream with error: \(error.localizedDescription, privacy: .public)"
                    )
                }
                speechContinuation.finish(throwing: error)
            } else {
                if Self.shouldLog(.notice) {
                    Self.logger.notice("Finishing stream successfully")
                }
                speechContinuation.finish()
            }
        case .dictation(let dictationContinuation):
            if let error {
                if Self.shouldLog(.error) {
                    Self.logger.error(
                        "Finishing dictation stream with error: \(error.localizedDescription, privacy: .public)"
                    )
                }
                dictationContinuation.finish(throwing: error)
            } else {
                if Self.shouldLog(.notice) {
                    Self.logger.notice("Finishing dictation stream successfully")
                }
                dictationContinuation.finish()
            }
        }
    }

    // MARK: - Helper Methods

    func setupAudioSession(liveRun: LiveTranscriptionRun? = nil) async throws {
#if os(iOS)
        try await MainActor.run {
            if let liveRun { try requireCurrentLiveTranscription(liveRun) }
            let audioSession = AVAudioSession.sharedInstance()
            try audioSession.setCategory(
                audioConfig.category,
                mode: audioConfig.mode,
                options: audioConfig.options
            )
            try audioSession.setActive(true)
            isAudioSessionActive = true
        }
#endif
#if os(iOS) || os(macOS)
        if let liveRun { try requireCurrentLiveTranscription(liveRun) }
        publishCurrentAudioInputInfo()
#endif
    }

#if os(iOS)
    private func deactivateAudioSessionIfNeeded() {
        guard isAudioSessionActive else { return }
        do {
            let audioSession = AVAudioSession.sharedInstance()
            try audioSession.setActive(false, options: [.notifyOthersOnDeactivation])
            isAudioSessionActive = false
            if Self.shouldLog(.info) {
                Self.logger.info("Audio session deactivated")
            }
        } catch {
            if Self.shouldLog(.error) {
                Self.logger.error("Failed to deactivate audio session: \(error.localizedDescription, privacy: .public)")
            }
        }
    }
#else
    private func deactivateAudioSessionIfNeeded() {}
#endif

}
