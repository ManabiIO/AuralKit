import Foundation
@preconcurrency import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {
    var shouldUseNativeCaptureInputProvider: Bool {
        // AVCaptureSession can independently configure the application's audio
        // session. Use AVAudioEngine when the host owns that session instead.
        guard audioSessionOwnership == .managed else { return false }
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *) {
            return inputProviderPreference == .automatic
        }
#endif
        return false
    }

    func setUpNativeCaptureStreamingIfAvailable(generation: Int) async throws -> Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *), shouldUseNativeCaptureInputProvider {
            try await setUpNativeCaptureStreaming(generation: generation)
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

    /// Releasing the capture session/output terminates the provider's input
    /// sequence. Keep its analysis task alive so it can drain and finalize.
    func endNativeCaptureInput() -> Task<Void, Never>? {
        let task = nativeCaptureAnalysisTask
        nativeCaptureAnalysisTask = nil
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *), nativeCaptureSession != nil {
            _ = stopPreparedNativeCaptureStreaming()
        }
#endif
        nativeCaptureSession = nil
        return task
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

    func setUpNativeCaptureStreaming(generation: Int) async throws {
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
        try Task.checkCancellation()
        nativeCaptureSession = provider.captureSession

        try await prepareAnalyzerForStartIfNeeded(in: nil)
        try Task.checkCancellation()

        let analyzerInputs = provider.analyzerInputs
        nativeCaptureAnalysisTask = Task<Void, Never> { [weak self, analyzer, analyzerInputs] in
            do {
                let lastAudioTime = try await analyzer.analyzeSequence(analyzerInputs)
                if let lastAudioTime {
                    try await analyzer.finalizeAndFinish(through: lastAudioTime)
                } else {
                    try await analyzer.finalizeAndFinishThroughEndOfInput()
                }
            } catch is CancellationError {
                if !Task.isCancelled {
                    await self?.finishFromNativeCaptureAnalysis(CancellationError(), generation: generation)
                }
            } catch {
                await self?.finishFromNativeCaptureAnalysis(error, generation: generation)
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
        nativeCaptureAnalysisTask?.cancel()
        nativeCaptureAnalysisTask = nil
        if nativeCaptureSession != nil {
            _ = stopPreparedNativeCaptureStreaming()
        }
        nativeCaptureSession = nil
    }

    func finishFromNativeCaptureAnalysis(_ error: Error, generation: Int) async {
        guard generation == sessionGeneration else { return }
        nativeCaptureAnalysisTask = nil
        await finishFromRecognizerTask(error: error, generation: generation)
    }
}
#endif
