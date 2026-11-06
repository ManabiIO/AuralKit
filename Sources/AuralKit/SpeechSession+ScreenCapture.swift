import AVFoundation
import CoreMedia
import Foundation
import Speech

// swiftlint:disable file_length

/// Internal capture lifetime boundary; production uses ScreenCaptureKit below.
@available(iOS 26.0, macOS 26.0, *)
protocol ScreenCaptureInput: AnyObject {
    @MainActor func start() async throws
    @MainActor func pause() async
    @MainActor func resume() async throws
    @MainActor func stop() async
}

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {
    func pauseScreenCaptureStreamingIfNeeded() async {
        await screenCaptureInputProvider?.pause()
    }

    func resumeScreenCaptureStreamingIfNeeded() async throws {
        guard let provider = screenCaptureInputProvider else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        try await provider.resume()
    }

    func stopScreenCaptureStreamingIfNeeded() async {
        let provider = screenCaptureInputProvider
        screenCaptureInputProvider = nil
        await provider?.stop()
    }
}

#if canImport(ScreenCaptureKit)
@preconcurrency import ScreenCaptureKit

@available(iOS 27.0, macOS 26.0, *)
public extension SpeechSession {
    /// Configuration for transcribing audio captured through ScreenCaptureKit.
    struct ScreenCaptureTranscriptionOptions {
        /// Whether captured audio should exclude audio produced by the current process.
        public var excludesCurrentProcessAudio: Bool

        /// Creates screen capture transcription options.
        public init(
            excludesCurrentProcessAudio: Bool = true
        ) {
            self.excludesCurrentProcessAudio = excludesCurrentProcessAudio
        }
    }

    /// Start transcribing audio from content selected with ScreenCaptureKit's system picker.
    ///
    /// Screen capture transcription is available on platforms where ScreenCaptureKit can stream
    /// audio as `CMSampleBuffer` values. AuralKit configures the stream for audio capture, converts
    /// incoming audio samples into analyzer-compatible `AVAudioPCMBuffer`s, and emits normal
    /// `SpeechTranscriber.Result` values.
    ///
    /// Apps using this API must include `NSScreenCaptureUsageDescription` in their Info.plist.
    ///
    /// - Parameters:
    ///   - options: ScreenCaptureKit picker and stream configuration.
    ///   - contextualStrings: Optional analysis context used to bias recognition.
    /// - Returns: An async throwing stream of transcription results.
    func startTranscribingScreenCapture(
        options: ScreenCaptureTranscriptionOptions = .init(),
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        let (stream, newContinuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()

        guard fileTranscription == nil, liveTranscription == nil, continuation == nil,
              recognizerTask == nil, streamingMode == .inactive else {
            newContinuation.finish(throwing: SpeechSessionError.recognitionStreamSetupFailed)
            return stream
        }

        let run = LiveTranscriptionRun()
        liveTranscription = run
        newContinuation.onTermination = { [weak self, weak run] _ in
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                self.retireLiveTranscription(run, error: nil)
            }
        }

        setStatus(.preparing)
        continuation = .speech(newContinuation)
        run.setupTask = Task { @MainActor [weak self] in
            guard let self else { return }
            await self.startScreenCapturePipeline(
                run: run, with: newContinuation,
                options: options,
                contextualStrings: contextualStrings
            )
        }
        return stream
    }

    /// Start transcribing audio from selected ScreenCaptureKit content with simple contextual words.
    ///
    /// - Parameters:
    ///   - options: ScreenCaptureKit picker and stream configuration.
    ///   - contextualStrings: Contextual words applied to the `.general` analysis context.
    /// - Returns: An async throwing stream of transcription results.
    func startTranscribingScreenCapture(
        options: ScreenCaptureTranscriptionOptions = .init(),
        contextualStrings: [String]
    ) -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        startTranscribingScreenCapture(
            options: options,
            contextualStrings: [.general: contextualStrings]
        )
    }
}

@available(iOS 27.0, macOS 26.0, *)
@MainActor
extension SpeechSession {
    func startScreenCapturePipeline(
        run: LiveTranscriptionRun,
        with streamContinuation: AsyncThrowingStream<SpeechTranscriber.Result, Error>.Continuation,
        options: ScreenCaptureTranscriptionOptions,
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]?
    ) async {
        do {
            if Self.shouldLog(.notice) {
                Self.logger.notice("Starting screen capture pipeline setup")
            }

            try requireCurrentLiveTranscription(run)
            try await ensureSpeechRecognitionAuthorization(context: "for screen capture transcription")
            try requireCurrentLiveTranscription(run)
            let transcriber = try await setUpSpeechTranscriber(contextualStrings: contextualStrings)
            try requireCurrentLiveTranscription(run)
            run.recognizerTask = makeLiveRecognizerTask(
                run: run, results: transcriber.results, streamContinuation: streamContinuation
            )
            recognizerTask = run.recognizerTask
            try await setUpScreenCaptureStreaming(options: options, run: run)
            try requireCurrentLiveTranscription(run)

            streamingMode = .screenCapture
            setStatus(.transcribing)
            activeResultKind = .speech
            if Self.shouldLog(.info) {
                Self.logger.info("Pipeline started (mode: screen capture)")
            }
        } catch {
            retireLiveTranscription(run, error: error)
        }
    }

    func setUpScreenCaptureStreaming(options: ScreenCaptureTranscriptionOptions, run: LiveTranscriptionRun) async throws {
        guard let analyzerFormat, let inputBuilder else {
            throw SpeechSessionError.invalidAudioDataType
        }

        let provider = ScreenCaptureAudioInputProvider(
            options: options,
            targetFormat: analyzerFormat,
            inputContinuation: inputBuilder,
            onFailure: { [weak self, weak run] error in
                Task { @MainActor [weak self, weak run] in
                    guard let self, let run else { return }
                    self.retireLiveTranscription(run, error: error)
                }
            }
        )
        screenCaptureInputProvider = provider
        try await provider.start()
    }
}

@available(iOS 27.0, macOS 26.0, *)
private final class ScreenCaptureAudioInputProvider: NSObject, ScreenCaptureInput, SCStreamOutput, SCStreamDelegate,
    SCContentSharingPickerObserver, @unchecked Sendable {

    private let options: SpeechSession.ScreenCaptureTranscriptionOptions
    private let targetFormat: AVAudioFormat
    private let inputContinuation: AsyncStream<AnalyzerInput>.Continuation
    private let onFailure: @Sendable (Error) -> Void
    private let sampleQueue = DispatchQueue(label: "com.auralkit.screencapture.audio", qos: .userInitiated)
    // Only touched on `sampleQueue`, which serializes access.
    private let bufferConverter = BufferConverter()

    private var stream: SCStream?
    private var selectionContinuation: CheckedContinuation<Void, Error>?
    private var isRunning = false
    private var stopped = false
    @MainActor private var captureTasks: [UUID: Task<Void, Never>] = [:]
    private var intentionallyStoppingStreams = Set<ObjectIdentifier>()

    init(
        options: SpeechSession.ScreenCaptureTranscriptionOptions,
        targetFormat: AVAudioFormat,
        inputContinuation: AsyncStream<AnalyzerInput>.Continuation,
        onFailure: @escaping @Sendable (Error) -> Void
    ) {
        self.options = options
        self.targetFormat = targetFormat
        self.inputContinuation = inputContinuation
        self.onFailure = onFailure
        super.init()
    }

    @MainActor
    func start() async throws {
        try Task.checkCancellation()
        guard !stopped else { throw CancellationError() }
        let picker = SCContentSharingPicker.shared
        let configuration = SCContentSharingPickerConfiguration()
        picker.configuration = configuration
        picker.defaultConfiguration = configuration
        picker.add(self)
        picker.isActive = true

        // The run's terminal task stops this provider before joining setup.
        // stop() resumes pending selection and joins admitted picker updates.
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            selectionContinuation = continuation
            picker.present()
        }
        try Task.checkCancellation()
    }

    @MainActor
    func pause() async {
        guard isRunning, let stream else { return }
        markStreamForIntentionalStop(stream)
        try? await stream.stopCapture()
        isRunning = false
    }

    @MainActor
    func resume() async throws {
        guard !stopped, !isRunning, let stream else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        try await stream.startCapture()
        guard !stopped else {
            markStreamForIntentionalStop(stream)
            try? await stream.stopCapture()
            throw CancellationError()
        }
        isRunning = true
    }

    @MainActor
    func stop() async {
        stopped = true
        let picker = SCContentSharingPicker.shared
        picker.isActive = false
        picker.remove(self)

        if let selectionContinuation {
            selectionContinuation.resume(throwing: SpeechSessionError.screenCaptureSelectionCancelled)
            self.selectionContinuation = nil
        }

        if isRunning, let stream {
            markStreamForIntentionalStop(stream)
            try? await stream.stopCapture()
        }
        stream = nil
        isRunning = false
        // A picker update may still be awaiting startCapture. It must stop any
        // late stream and exit before session retirement releases its analyzer.
        let tasks = Array(captureTasks.values)
        for task in tasks { await task.value }
    }

    func contentSharingPicker(
        _ picker: SCContentSharingPicker,
        didUpdateWith filter: SCContentFilter,
        for stream: SCStream?
    ) {
        nonisolated(unsafe) let selectedFilter = filter
        Task { @MainActor [weak self] in
            guard let self, !self.stopped else { return }
            let id = UUID()
            self.captureTasks[id] = Task { @MainActor in
                defer { self.captureTasks[id] = nil }
                do {
                    try await self.beginCapture(with: selectedFilter)
                    self.resumeSelection()
                } catch {
                    if !self.resumeSelection(throwing: error), !self.stopped {
                        self.onFailure(SpeechSessionError.screenCaptureFailed(error))
                    }
                }
            }
        }
    }

    func contentSharingPicker(_ picker: SCContentSharingPicker, didCancelFor stream: SCStream?) {
        Task { @MainActor [weak self] in
            self?.resumeSelection(throwing: SpeechSessionError.screenCaptureSelectionCancelled)
        }
    }

    func contentSharingPickerStartDidFailWithError(_ error: any Error) {
        Task { @MainActor [weak self] in
            guard let self else { return }
            let wrappedError = SpeechSessionError.screenCaptureFailed(error)
            if !self.resumeSelection(throwing: wrappedError) {
                self.onFailure(wrappedError)
            }
        }
    }

    func stream(_ stream: SCStream, didStopWithError error: any Error) {
        Task { @MainActor [weak self] in
            guard let self, !self.consumeIntentionalStop(for: stream),
                  !self.stopped, self.stream === stream else { return }
            self.isRunning = false
            self.onFailure(SpeechSessionError.screenCaptureFailed(error))
        }
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer, of type: SCStreamOutputType) {
        guard type == .audio, sampleBuffer.isValid, sampleBuffer.numSamples > 0 else { return }
        guard let sourceFormat = makeSourceFormat(from: sampleBuffer) else { return }

        do {
            try sampleBuffer.withAudioBufferList { audioBufferList, _ in
                guard let sourceBuffer = AVAudioPCMBuffer(
                    pcmFormat: sourceFormat,
                    bufferListNoCopy: audioBufferList.unsafePointer
                ) else { return }

                guard let bufferCopy = sourceBuffer.copy() as? AVAudioPCMBuffer else {
                    onFailure(SpeechSessionError.conversionBufferCreationFailed)
                    return
                }

                processAudioBuffer(bufferCopy)
            }
        } catch {
            onFailure(SpeechSessionError.screenCaptureFailed(error))
        }
    }

    @MainActor
    private func beginCapture(with filter: SCContentFilter) async throws {
        guard !stopped else { throw CancellationError() }
        if isRunning, let stream {
            markStreamForIntentionalStop(stream)
            try await stream.stopCapture()
            isRunning = false
        }

        guard !stopped else { throw CancellationError() }
        let streamConfiguration = SCStreamConfiguration()
        streamConfiguration.capturesAudio = true
        streamConfiguration.sampleRate = Int(targetFormat.sampleRate)
        streamConfiguration.channelCount = Int(targetFormat.channelCount)
        streamConfiguration.excludesCurrentProcessAudio = options.excludesCurrentProcessAudio
        streamConfiguration.width = 2
        streamConfiguration.height = 2

        let captureStream = SCStream(filter: filter, configuration: streamConfiguration, delegate: self)
        try captureStream.addStreamOutput(self, type: .audio, sampleHandlerQueue: sampleQueue)
        self.stream = captureStream
        try await captureStream.startCapture()
        guard !stopped else {
            markStreamForIntentionalStop(captureStream)
            try? await captureStream.stopCapture()
            throw CancellationError()
        }
        isRunning = true
    }

    @discardableResult
    @MainActor
    private func resumeSelection(throwing error: Error? = nil) -> Bool {
        guard let selectionContinuation else { return false }
        self.selectionContinuation = nil
        if let error {
            selectionContinuation.resume(throwing: error)
        } else {
            selectionContinuation.resume()
        }
        return true
    }

    private func makeSourceFormat(from sampleBuffer: CMSampleBuffer) -> AVAudioFormat? {
        guard let formatDescription = sampleBuffer.formatDescription,
              var sourceDescription = formatDescription.audioStreamBasicDescription else {
            return nil
        }

        return AVAudioFormat(streamDescription: &sourceDescription)
    }

    /// Convert and forward a captured buffer synchronously on `sampleQueue`.
    ///
    /// Inline conversion preserves capture order for the analyzer; the caller passes an
    /// already-copied buffer, so a passthrough conversion is safe to forward directly.
    private func processAudioBuffer(_ buffer: AVAudioPCMBuffer) {
        do {
            let converted = try bufferConverter.convertBuffer(buffer, to: targetFormat)
            inputContinuation.yield(AnalyzerInput(buffer: converted))
        } catch {
            onFailure(error)
        }
    }

    @MainActor
    private func markStreamForIntentionalStop(_ stream: SCStream) {
        intentionallyStoppingStreams.insert(ObjectIdentifier(stream))
    }

    @MainActor
    private func consumeIntentionalStop(for stream: SCStream) -> Bool {
        intentionallyStoppingStreams.remove(ObjectIdentifier(stream)) != nil
    }
}
#else
@available(iOS 26.0, macOS 26.0, *)
public extension SpeechSession {
    /// Placeholder options used when ScreenCaptureKit is unavailable to the current build target.
    struct ScreenCaptureTranscriptionOptions {
        public init() {}
    }

    /// Screen capture transcription requires ScreenCaptureKit.
    func startTranscribingScreenCapture(
        options: ScreenCaptureTranscriptionOptions = .init(),
        contextualStrings: [AnalysisContext.ContextualStringsTag: [String]]? = nil
    ) -> AsyncThrowingStream<SpeechTranscriber.Result, Error> {
        _ = options
        _ = contextualStrings
        let (stream, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        continuation.finish(throwing: SpeechSessionError.screenCaptureUnavailable)
        return stream
    }
}

#endif
