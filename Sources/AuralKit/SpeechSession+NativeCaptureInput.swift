import Foundation
@preconcurrency import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {
    var shouldUseNativeCaptureInputProvider: Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *) {
            return inputProviderPreference == .automatic
        }
#endif
        return false
    }

    func setUpNativeCaptureStreamingIfAvailable(run: LiveTranscriptionRun) async throws -> Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *), shouldUseNativeCaptureInputProvider {
            try await setUpNativeCaptureStreaming(run: run)
            return true
        }
#endif
        return false
    }

    func tearDownNativeCaptureStreaming() {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *) {
            tearDownNativeCaptureStreamingIfAvailable()
        }
#endif
    }

    func restartNativeCaptureStreamingIfAvailable() throws -> Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *) {
            guard stopPreparedNativeCaptureStreaming(),
                  startPreparedNativeCaptureStreaming() else {
                throw SpeechSessionError.recognitionStreamSetupFailed
            }
            return true
        }
#endif
        return false
    }
}

#if swift(>=6.4)
@available(iOS 27.0, macOS 27.0, *)
extension SpeechSession {
    private var canUseNativeCaptureInputProvider: Bool {
        inputProviderPreference == .automatic
    }

    func setUpNativeCaptureStreaming(run: LiveTranscriptionRun) async throws {
        guard !isAudioStreaming else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        guard canUseNativeCaptureInputProvider else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        guard let modules = activeModules, let analyzer else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        guard let captureDevice = AVCaptureDevice.default(for: .audio) else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }

        let provider = try await CaptureInputSequenceProvider.providerWithSession(
            from: captureDevice,
            compatibleWith: modules,
            priority: analyzerConfiguration.priority
        )
        try requireCurrentLiveTranscription(run)
        nativeCaptureSession = provider.captureSession

        try await prepareAnalyzerForStartIfNeeded(in: nil)
        try requireCurrentLiveTranscription(run)

        let analyzerInputs = provider.analyzerInputs
        run.inputTask = Task<Void, Never> { @MainActor [self, analyzer, analyzerInputs] in
            do {
                let lastAudioTime = try await analyzer.analyzeSequence(analyzerInputs)
                if let lastAudioTime {
                    try await analyzer.finalizeAndFinish(through: lastAudioTime)
                } else {
                    try await analyzer.finalizeAndFinishThroughEndOfInput()
                }
                // Input completion is owned by the result reader, which drains final results.
            } catch {
                self.retireLiveTranscription(run, error: error)
            }
        }
        startSpeechDetectorMonitoringIfNeeded()

        _ = startPreparedNativeCaptureStreaming()
    }

    func startPreparedNativeCaptureStreaming() -> Bool {
        guard let nativeCaptureSession else { return false }

        if Self.shouldLog(.debug) {
            Self.logger.debug("Starting native capture session")
        }
        nativeCaptureSession.startRunning()
        isAudioStreaming = true
        return true
    }

    func stopPreparedNativeCaptureStreaming() -> Bool {
        guard let nativeCaptureSession else { return false }

        if Self.shouldLog(.debug) {
            Self.logger.debug("Stopping native capture session")
        }
        nativeCaptureSession.stopRunning()
        isAudioStreaming = false
        return true
    }

    func tearDownNativeCaptureStreamingIfAvailable() {
        if nativeCaptureSession != nil {
            _ = stopPreparedNativeCaptureStreaming()
        }
        nativeCaptureSession = nil
    }

}
#endif
