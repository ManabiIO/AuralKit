import Foundation
import AVFoundation
import Speech

#if swift(>=6.4)
@available(iOS 27.0, macOS 27.0, *)
extension SpeechSession {
    nonisolated var shouldUseNativeAssetInputProvider: Bool {
        get async {
            await MainActor.run {
                inputProviderPreference == .automatic
            }
        }
    }

    nonisolated func feedAudioFileWithNativeAnalyzer(
        _ file: AVAudioFile,
        progressHandler _: (@Sendable (Double) -> Void)?
    ) async throws -> Bool {
        guard !Task.isCancelled else { return false }

        let (run, analyzer) = try await MainActor.run {
            guard let run = fileTranscription, let analyzer else {
                throw SpeechSessionError.recognitionStreamSetupFailed
            }
            try requireCurrentFileTranscription(run)
            return (run, analyzer)
        }

        try await prepareAnalyzerForStartIfNeeded(in: file.processingFormat)
        try Task.checkCancellation()
        try await MainActor.run {
            try requireCurrentFileTranscription(run)
            startSpeechDetectorMonitoringIfNeeded()
        }
        try Task.checkCancellation()

        let lastAudioTime = try await analyzer.analyzeSequence(from: file)
        try await MainActor.run { try requireCurrentFileTranscription(run) }
        if let lastAudioTime {
            try await analyzer.finalizeAndFinish(through: lastAudioTime)
        } else {
            await MainActor.run { retireFileTranscription(run, error: nil) }
        }

        return !Task.isCancelled
    }
}
#endif
