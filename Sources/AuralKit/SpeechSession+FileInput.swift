import Foundation
import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {
    func ingestFileInput(
        _ run: FileTranscriptionRun,
        validation: (file: AVAudioFile, totalFrames: AVAudioFramePosition),
        progressHandler: (@Sendable (Double) -> Void)?,
        ingest: (@Sendable () async throws -> Bool)?
    ) async {
        do {
            try requireCurrentFileTranscription(run)
            let completed: Bool
            if let ingest {
                completed = try await ingest()
            } else {
                completed = try await feedAudioFile(validation, run: run, progressHandler: progressHandler)
            }
            try requireCurrentFileTranscription(run)
            guard completed else {
                // Cancellation was checked above. An input that ended
                // before its declared length is an incomplete read;
                // don't leave analysis open or report partial success.
                throw Self.incompleteAudioFileReadError()
            }
            if let progressHandler { reportFileProgress(1.0, handler: progressHandler, run: run) }
            if !run.usesNativeFileAnalyzer {
                try await finalizeFileInput(run)
            }
            try requireCurrentFileTranscription(run)
            completeFileIngestion(run)
        } catch is CancellationError {
            // A finalizer may fail with CancellationError even when
            // public stop did not initiate cancellation.
            if fileTranscription === run, !run.retired {
                retireFileTranscription(run, error: CancellationError())
            }
        } catch {
            retireFileTranscription(run, error: error)
        }
    }

    nonisolated func feedAudioFile(
        _ payload: (file: AVAudioFile, totalFrames: AVAudioFramePosition),
        run: FileTranscriptionRun,
        progressHandler: (@Sendable (Double) -> Void)?
    ) async throws -> Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *),
           progressHandler == nil,
           await shouldUseNativeAssetInputProvider {
            return try await feedAudioFileWithNativeAnalyzer(payload.file, progressHandler: progressHandler)
        }
#endif
        var processedFrames: AVAudioFramePosition = 0
        let frameCapacity: AVAudioFrameCount = 4096
        let file = payload.file
        let totalFrames = payload.totalFrames
        let format = file.processingFormat

        guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frameCapacity) else {
            throw SpeechSessionError.conversionBufferCreationFailed
        }

        while processedFrames < totalFrames {
            if Task.isCancelled { break }

            let framesToRead = AudioFileReadSize.next(
                remaining: totalFrames - processedFrames, capacity: frameCapacity
            )

            buffer.frameLength = framesToRead

            do {
                try file.read(into: buffer, frameCount: framesToRead)
            } catch {
                throw SpeechSessionError.audioFileReadFailed(error)
            }

            if buffer.frameLength == 0 {
                throw Self.incompleteAudioFileReadError()
            }

            guard let bufferCopy = buffer.copy() as? AVAudioPCMBuffer else {
                throw SpeechSessionError.conversionBufferCreationFailed
            }

            let sendableBuffer = SendablePCMBuffer(buffer: bufferCopy)

            try await self.processAudioBuffer(sendableBuffer)

            processedFrames += AVAudioFramePosition(buffer.frameLength)

            if let progressHandler, totalFrames > 0 {
                let progress = min(1.0, max(0.0, Double(processedFrames) / Double(totalFrames)))
                await MainActor.run {
                    self.reportFileProgress(progress, handler: progressHandler, run: run)
                }
            }
        }

        return !Task.isCancelled && processedFrames >= totalFrames
    }

    nonisolated private static func incompleteAudioFileReadError() -> SpeechSessionError {
        .audioFileReadFailed(CocoaError(.fileReadUnknown, userInfo: [
            NSLocalizedDescriptionKey: "Audio input ended before all declared frames were read."
        ]))
    }

    func shouldUseNativeFileAnalyzer(progressHandler: (@Sendable (Double) -> Void)?) -> Bool {
#if swift(>=6.4)
        if #available(iOS 27.0, macOS 27.0, *) {
            return inputProviderPreference == .automatic && progressHandler == nil
        }
#endif
        return false
    }

    func reportFileProgress(
        _ progress: Double,
        handler: @Sendable (Double) -> Void,
        run: FileTranscriptionRun
    ) {
        guard !Task.isCancelled, fileTranscription === run, !run.retired,
              streamingMode == .filePlayback, status == .transcribing else { return }
        handler(progress)
    }

}
