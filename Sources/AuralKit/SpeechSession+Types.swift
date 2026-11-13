import Foundation
import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {

    /// Discrete lifecycle stages for a speech transcription session.
    public enum Status: Equatable, Sendable {
        case idle
        case preparing
        case transcribing
        case paused
        case stopping
    }

    /// Discrete source types currently feeding the analyzer pipeline.
    enum StreamingMode: Equatable, Sendable {
        case inactive
        case liveMicrophone
        case filePlayback
        case screenCapture
        case externalAudio
    }

    enum TranscriptionResultKind: Equatable, Sendable {
        case speech
        case dictation
    }

    enum TranscriptionContinuation {
        case speech(AsyncThrowingStream<SpeechTranscriber.Result, Error>.Continuation)
        case dictation(AsyncThrowingStream<DictationTranscriber.Result, Error>.Continuation)
    }

    /// Preferred source pipeline for feeding audio into `SpeechAnalyzer`.
    public enum InputProviderPreference: Equatable, Sendable {
        /// Use native Speech input sequence providers when the compiler and OS support them.
        case automatic

        /// Always use AuralKit's AVAudioEngine and buffer conversion pipeline.
        case legacy
    }

    /// Responsibility for the application's shared audio session.
    public enum AudioSessionOwnership: Equatable, Sendable {
        /// AuralKit configures and activates the session for microphone capture.
        case managed

        /// The host configures, activates and deactivates the audio session, and
        /// coordinates interruptions. AuralKit may capture with its own engine,
        /// but never changes AVAudioSession or automatically resumes it.
        case external
    }

    struct VoiceActivationConfiguration {
        let detectionOptions: SpeechDetector.DetectionOptions
        let reportResults: Bool
    }

    struct SendablePCMBuffer: @unchecked Sendable {
        let buffer: AVAudioPCMBuffer
    }
}
