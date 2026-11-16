#if swift(>=6.4)
import AVFoundation
import CoreMedia
import Foundation
import Speech

@available(iOS 27.0, macOS 27.0, *)
public extension SpeechSession {
    /// Stream transcription results from one audio track of a local media file, such as a movie.
    ///
    /// - Parameters:
    ///   - mediaFile: A local file URL in one of `options.allowedDirectories`.
    ///   - audioTrackID: An ID from the file's audio tracks. Pass `nil` to use its first audio track.
    ///   - options: Contextual strings, allowed directories, and the maximum selected track duration.
    /// - Returns: An async throwing stream of transcription results on the media timeline.
    ///
    /// The audio must be stored inside the media file and the session must use `.automatic` input.
    func streamTranscription(
        fromMediaFile mediaFile: URL,
        audioTrackID: CMPersistentTrackID? = nil,
        options: FileTranscriptionOptions = .init()
    ) -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        startFileTranscription(mode: .mediaPlayback) { session, continuation, run in
            await session.startMediaFilePipeline(
                mediaFile, audioTrackID: audioTrackID, options: options,
                continuation: continuation, run: run
            )
        }
    }
}

@available(iOS 27.0, macOS 27.0, *)
private extension SpeechSession {
    func startMediaFilePipeline(
        _ mediaFile: URL,
        audioTrackID: CMPersistentTrackID?,
        options: FileTranscriptionOptions,
        continuation: FileResultContinuation,
        run: FileTranscriptionRun
    ) async {
        do {
            try requireCurrentFileTranscription(run)
            guard inputProviderPreference == .automatic else {
                throw MediaTranscriptionConfigurationError()
            }
            let url = try sanitizeAudioFileURL(mediaFile, allowedDirectories: options.allowedDirectories)
            let asset = Self.makeMediaAsset(url: url, needsPreciseDuration: options.maxDuration != nil)
            let track = try await loadAudioTrack(from: asset, id: audioTrackID, maxDuration: options.maxDuration)
            try requireCurrentFileTranscription(run)
            try await ensureSpeechRecognitionAuthorization(context: "for media file transcription")
            try requireCurrentFileTranscription(run)
            run.usesNativeFileAnalyzer = true
            try await setUpFilePipeline(
                run: run, with: continuation,
                contextualStrings: options.contextualStrings,
                mode: .mediaPlayback
            )
            try requireCurrentFileTranscription(run)
            guard let analyzer, let activeModules else {
                throw SpeechSessionError.recognitionStreamSetupFailed
            }
            guard let format = await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith: activeModules) else {
                throw SpeechSessionError.invalidAudioDataType
            }
            try requireCurrentFileTranscription(run)
            let provider = AssetInputSequenceProvider(asset: asset, track: track, analyzerFormat: format)
            let inputs = provider.analyzerInputs
            startFileIngestion(run: run) { session in
                try await session.prepareAnalyzerForStartIfNeeded(in: format)
                try session.requireCurrentFileTranscription(run)
                session.startSpeechDetectorMonitoringIfNeeded()

                let endTime = try await analyzer.analyzeSequence(inputs)
                try session.requireCurrentFileTranscription(run)
                if let endTime {
                    try await analyzer.finalizeAndFinish(through: endTime)
                } else {
                    // With no input samples, Speech may leave the module's results stream open.
                    session.retireFileTranscription(run, error: nil)
                }
            }
        } catch {
            retireFileTranscription(run, error: error)
        }
    }

    static func makeMediaAsset(url: URL, needsPreciseDuration: Bool) -> AVURLAsset {
        var options: [String: Any] = [
            AVURLAssetReferenceRestrictionsKey: AVAssetReferenceRestrictions.forbidAll.rawValue
        ]
        if needsPreciseDuration {
            options[AVURLAssetPreferPreciseDurationAndTimingKey] = true
        }
        return AVURLAsset(url: url, options: options)
    }

    func loadAudioTrack(
        from asset: AVAsset,
        id: CMPersistentTrackID?,
        maxDuration: TimeInterval?
    ) async throws -> AVAssetTrack {
        let tracks = try await asset.loadTracks(withMediaType: .audio)
        guard let track = tracks.first(where: { id == nil || $0.trackID == id }) else {
            let reason = if let id {
                String.localizedStringWithFormat(NSLocalizedString(
                    "audio track ID %d was not found", comment: "The requested audio track is missing"
                ), id)
            } else {
                NSLocalizedString("the media file has no audio tracks", comment: "The media asset has no audio track")
            }
            throw SpeechSessionError.audioFileUnsupportedFormat(reason)
        }
        let duration = try await track.load(.timeRange).duration.seconds
        guard duration.isFinite, duration > 0 else {
            throw SpeechSessionError.audioFileUnsupportedFormat(NSLocalizedString(
                "the audio track has no finite, positive duration",
                comment: "The media audio track has no usable duration"
            ))
        }
        if let maxDuration, duration > maxDuration {
            throw SpeechSessionError.audioFileTooLong(maximum: maxDuration, actual: duration)
        }
        return track
    }
}

private struct MediaTranscriptionConfigurationError: LocalizedError {
    var errorDescription: String? {
        NSLocalizedString(
            "Media transcription requires the automatic input provider.",
            comment: "Media files cannot use the legacy audio input pipeline"
        )
    }

    var recoverySuggestion: String? {
        NSLocalizedString(
            "Create a SpeechSession with inputProviderPreference set to .automatic.",
            comment: "How to enable media transcription"
        )
    }
}
#endif
