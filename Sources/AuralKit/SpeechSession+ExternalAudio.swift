import Foundation
@preconcurrency import AVFoundation
import Speech

/// A single-use input for audio captured by the host application.
///
/// Await `startTranscribing(externalAudio:)` before appending. Calls to `append`
/// are synchronous and serialized, so they can be made from an audio callback
/// without spawning a task for each buffer. The host retains responsibility for
/// its capture engine, permissions, audio session, route changes and interruptions.
@available(iOS 26.0, macOS 26.0, *)
public final class ExternalAudioInput: @unchecked Sendable {
    private let lock = NSLock()
    private let converter = BufferConverter()
    private var destination: AsyncStream<AnalyzerInput>.Continuation?
    private var format: AVAudioFormat?
    private var hasBeenBound = false
    private var finished = false

    public init() {}

    /// Convert and enqueue a copy of this buffer. The caller may reuse its buffer
    /// after this returns. Returns false before startup or after input is closed.
    /// Send buffers in capture order; do not mutate a buffer during this call.
    @discardableResult
    public func append(_ buffer: AVAudioPCMBuffer) throws -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !finished, let destination, let format else { return false }
        guard buffer.frameLength > 0 else { return true }
        let converted = try converter.convertBuffer(buffer, to: format)
        let ownedBuffer: AVAudioPCMBuffer
        if converted === buffer {
            guard let copy = buffer.copy() as? AVAudioPCMBuffer else {
                throw SpeechSessionError.invalidAudioDataType
            }
            ownedBuffer = copy
        } else {
            ownedBuffer = converted
        }
        switch destination.yield(AnalyzerInput(buffer: ownedBuffer)) {
        case .enqueued:
            return true
        case .dropped, .terminated:
            return false
        @unknown default:
            return false
        }
    }

    /// Close this input. Call `session.finishTranscribing()` to finalize and
    /// drain results. This handle cannot be rebound to another transcription.
    public func finish() {
        lock.lock()
        defer { lock.unlock() }
        finished = true
        destination?.finish()
        destination = nil
        format = nil
    }

    func bind(to destination: AsyncStream<AnalyzerInput>.Continuation, format: AVAudioFormat) throws {
        lock.lock()
        defer { lock.unlock() }
        guard !hasBeenBound, !finished else { throw SpeechSessionError.recognitionStreamSetupFailed }
        hasBeenBound = true
        self.destination = destination
        self.format = format
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    /// Prepare recognition for host-owned audio and return when `externalAudio`
    /// is ready to accept buffers. Does not capture microphone audio or configure,
    /// activate or deactivate the audio session. Only speech authorization is requested.
    public func startTranscribing(
        externalAudio: ExternalAudioInput,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async throws -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        guard canStartTranscription else { throw SpeechSessionError.recognitionStreamSetupFailed }
        let (stream, newContinuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        let run = beginLiveTranscription()
        newContinuation.onTermination = { [weak self] termination in
            guard case .cancelled = termination else { return }
            Task { @MainActor [weak self] in
                await self?.handleStreamTermination(generation: run.generation)
            }
        }
        continuation = .speech(newContinuation)
        setStatus(.preparing)
        let setup = Task { @MainActor in
            await self.startSpeechPipeline(
                with: newContinuation, contextualStrings: contextualStrings,
                generation: run.generation, externalAudio: externalAudio
            )
        }
        pipelineTask = setup
        try await awaitLiveSetup(setup, run: run)
        return stream
    }

    /// Dictation/custom-vocabulary counterpart of `startTranscribing(externalAudio:)`.
    public func startDictationTranscribing(
        externalAudio: ExternalAudioInput,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) async throws -> AsyncThrowingStream<DictationTranscriber.Result, Error> {
        guard canStartTranscription else { throw SpeechSessionError.recognitionStreamSetupFailed }
        let (stream, newContinuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()
        let run = beginLiveTranscription()
        newContinuation.onTermination = { [weak self] termination in
            guard case .cancelled = termination else { return }
            Task { @MainActor [weak self] in
                await self?.handleStreamTermination(generation: run.generation)
            }
        }
        continuation = .dictation(newContinuation)
        setStatus(.preparing)
        let setup = Task { @MainActor in
            await self.startDictationPipeline(
                with: newContinuation, contextualStrings: contextualStrings,
                generation: run.generation, externalAudio: externalAudio
            )
        }
        pipelineTask = setup
        try await awaitLiveSetup(setup, run: run)
        return stream
    }

    func authorizeLiveInput(external: Bool) async throws {
        if let authorize = liveTranscriptionHooks?.authorize {
            try await authorize()
        } else if external {
            try await ensureSpeechRecognitionAuthorization(context: "for external audio")
        } else {
            try await ensurePermissions()
        }
    }

    func bindExternalAudio(_ input: ExternalAudioInput?) throws {
        guard let input else { return }
        guard let inputBuilder, let analyzerFormat else { throw SpeechSessionError.invalidAudioDataType }
        try input.bind(to: inputBuilder, format: analyzerFormat)
        externalAudioInput = input
    }
}
