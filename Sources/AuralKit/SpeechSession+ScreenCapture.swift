import AVFoundation
import CoreMedia
import Foundation
import Speech
import OSLog
import Synchronization

// swiftlint:disable file_length

/// Run-owned capture boundary shared by the native provider and controlled fixtures.
@available(iOS 26.0, macOS 26.0, *)
protocol ScreenCaptureInput: AnyObject {
    @MainActor func start() async throws
    @MainActor func pause() async -> Bool
    @MainActor func resume(onCompletion: @escaping @MainActor (Result<Void, Error>) -> Void) async -> Error?
    @MainActor func stop() -> Task<Void, Error>
}

@available(iOS 26.0, macOS 26.0, *)
extension SpeechSession {
    func pauseScreenCaptureStreamingIfNeeded(run: LiveTranscriptionRun) async {
        guard liveTranscription === run, !run.retired,
              let provider = screenCaptureInputProvider else { return }
        guard await provider.pause(), liveTranscription === run, !run.retired,
              status == .transcribing, streamingMode == .screenCapture else { return }
        setStatus(.paused)
    }

    func resumeScreenCaptureStreamingIfNeeded(run: LiveTranscriptionRun) async throws {
        try requireCurrentLiveTranscription(run)
        guard let provider = screenCaptureInputProvider else {
            throw SpeechSessionError.recognitionStreamSetupFailed
        }
        var captureFailure: Error?
        let terminalFailure = await provider.resume { result in
            guard self.liveTranscription === run, !run.retired,
                  self.status == .paused, self.streamingMode == .screenCapture else { return }
            switch result {
            case .success: self.setStatus(.transcribing)
            case .failure(let error):
                captureFailure = error
                self.prepareForStop()
            }
        }
        if let error = captureFailure ?? terminalFailure {
            // Public resume owns the join; this internal operation may itself
            // be a child that terminal cleanup must join.
            if !run.finishing { retireLiveTranscription(run, error: error) }
            throw error
        }
    }

    func beginStoppingScreenCapture(run: LiveTranscriptionRun? = nil) -> Task<Void, Error>? {
        let run = run ?? liveTranscription
        if let pending = run?.screenStopTask { return pending }
        let provider = screenCaptureInputProvider
        screenCaptureInputProvider = nil
        guard let provider else { return nil }
        let task = provider.stop()
        run?.screenStopTask = task
        return task
    }

    func stopScreenCaptureStreamingIfNeeded() async {
        _ = await beginStoppingScreenCapture()?.result
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

        guard canStartTranscription else {
            newContinuation.finish(throwing: SpeechSessionError.recognitionStreamSetupFailed)
            return stream
        }

        let run = LiveTranscriptionRun()
        liveTranscription = run
        newContinuation.onTermination = { [weak self, weak run] termination in
            guard case .cancelled = termination else { return }
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                self.retireLiveTranscription(run, error: CancellationError())
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
            if let hooks = screenCapturePipelineHooks {
                try await hooks.authorize()
                try requireCurrentLiveTranscription(run)
                let prepared = try await hooks.prepareAnalysis()
                analyzerFormat = prepared.format
                inputBuilder = prepared.inputs
                screenCaptureAnalysisFinalizer = prepared.finalize
            } else {
                try await ensureSpeechRecognitionAuthorization(context: "for screen capture transcription")
                try requireCurrentLiveTranscription(run)
                let transcriber = try await setUpSpeechTranscriber(contextualStrings: contextualStrings)
                try requireCurrentLiveTranscription(run)
                run.recognizerTask = makeLiveRecognizerTask(
                    run: run, results: transcriber.results, streamContinuation: streamContinuation
                )
                recognizerTask = run.recognizerTask
            }
            try requireCurrentLiveTranscription(run)
            try await setUpScreenCaptureStreaming(options: options, run: run)
            try requireCurrentLiveTranscription(run)

            streamingMode = .screenCapture
            run.ready = true
            setStatus(.transcribing)
            activeResultKind = .speech
            if Self.shouldLog(.info) {
                Self.logger.info("Pipeline started (mode: screen capture)")
            }
        } catch {
            finishLivePipelineSetupFailure(error, run: run)
        }
    }

    func setUpScreenCaptureStreaming(
        options: ScreenCaptureTranscriptionOptions, run: LiveTranscriptionRun
    ) async throws {
        guard let analyzerFormat, let inputBuilder else {
            throw SpeechSessionError.invalidAudioDataType
        }

        let onTermination: @Sendable (Error?) -> Task<Void, Never> = { [weak self, weak run] error in
            Task { @MainActor [weak self, weak run] in
                guard let self, let run else { return }
                if let error {
                    await self.retireLiveTranscription(run, error: error)?.value
                } else {
                    await self.finishLiveTranscription(run, sourceEnded: true)
                }
            }
        }
        let provider: any ScreenCaptureInput
        if let makeProvider = screenCapturePipelineHooks?.makeProvider {
            provider = makeProvider(analyzerFormat, inputBuilder, onTermination)
        } else {
            provider = ScreenCaptureAudioInputProvider(
                options: options, targetFormat: analyzerFormat,
                inputContinuation: inputBuilder,
                useNativeConversion: inputProviderPreference == .automatic,
                onTermination: onTermination
            )
        }
        screenCaptureInputProvider = provider
        try await provider.start()
    }
}

// ScreenCaptureKit can call its delegate before the asynchronous stop returns while the
// delegate's main-actor work is still queued. Keep the ingress error until stop observes it.
@available(iOS 26.0, macOS 26.0, *)
final class ScreenCaptureStopErrors: Sendable {
    private struct State {
        var streamID: ObjectIdentifier?
        var error: Error?
    }

    private let state = Mutex(State())

    func start(_ streamID: ObjectIdentifier) {
        state.withLock { $0 = State(streamID: streamID) }
    }

    func record(_ error: Error, for streamID: ObjectIdentifier) {
        state.withLock {
            guard $0.streamID == streamID, $0.error == nil else { return }
            $0.error = error
        }
    }

    @MainActor
    func stop(_ streamID: ObjectIdentifier, nativeStop: () async throws -> Void) async throws {
        let nativeError: Error?
        do {
            try await nativeStop()
            nativeError = nil
        } catch {
            nativeError = error
        }
        let delegateError = state.withLock {
            guard $0.streamID == streamID else { return nil as Error? }
            let error = $0.error
            if error != nil || nativeError == nil { $0 = State() }
            return error
        }
        if let delegateError {
            throw SpeechSessionError.screenCaptureFailed(delegateError)
        }
        if let nativeError { throw nativeError }
    }
}

@available(iOS 27.0, macOS 26.0, *)
@MainActor
private final class NativeScreenCaptureStream: ScreenCaptureStream {
    let stream: SCStream
    private let audio: ScreenCaptureAudioProcessor
    private let stopErrors: ScreenCaptureStopErrors

    init(stream: SCStream, audio: ScreenCaptureAudioProcessor, stopErrors: ScreenCaptureStopErrors) {
        self.stream = stream
        self.audio = audio
        self.stopErrors = stopErrors
    }

    func startCapture() async throws {
        defer { logCaptureState(after: "startCapture") }
        let id = ObjectIdentifier(stream)
        stopErrors.start(id)
        audio.start(id)
        do {
            try await stream.startCapture()
        } catch {
            audio.discard(id)
            throw error
        }
    }

    func stopCapture() async throws {
        defer { logCaptureState(after: "stopCapture") }
        let id = ObjectIdentifier(stream)
        try await stopErrors.stop(id) { try await stream.stopCapture() }
        if let error = audio.drain(id) {
            throw ScreenCaptureDrainFailure(underlyingError: error)
        }
    }

#if os(macOS) || targetEnvironment(macCatalyst)
    func updateContentFilter(_ filter: SCContentFilter) async throws {
        defer { logCaptureState(after: "updateContentFilter") }
        try await stream.updateContentFilter(filter)
    }
#endif

    private func logCaptureState(after operation: String) {
#if compiler(>=6.4)
        guard #available(iOS 27.0, macOS 27.0, macCatalyst 27.0, *),
              SpeechSession.shouldLog(.debug) else { return }
        SpeechSession.logger.debug(
            "SCStream.isCapturing after \(operation, privacy: .public): \(self.stream.isCapturing, privacy: .public)"
        )
#endif
    }
}

@available(iOS 27.0, macOS 26.0, *)
final class ScreenCaptureAudioInputProvider: NSObject, ScreenCaptureInput, SCStreamOutput, SCStreamDelegate,
    SCContentSharingPickerObserver, @unchecked Sendable {

    private let options: SpeechSession.ScreenCaptureTranscriptionOptions
    private let targetFormat: AVAudioFormat
    private let onTermination: @Sendable (Error?) -> Task<Void, Never>
    private let audio: ScreenCaptureAudioProcessor
    private let stopErrors = ScreenCaptureStopErrors()

    @MainActor private let transitions: ScreenCaptureTransitions
    @MainActor private var captureTasks: [UUID: Task<Void, Never>] = [:]
    private let presentPicker: @MainActor (ScreenCaptureAudioInputProvider) -> Void
    private let dismissPicker: @MainActor (any SCContentSharingPickerObserver) -> Void
    private var selectionContinuation: CheckedContinuation<Void, Error>?
    private var stopped = false
    private var pickerUpdateID: UUID?
    private var filterUpdateID: UUID?

    @MainActor
    init(
        options: SpeechSession.ScreenCaptureTranscriptionOptions,
        targetFormat: AVAudioFormat,
        inputContinuation: AsyncStream<AnalyzerInput>.Continuation,
        useNativeConversion: Bool = true,
        onTermination: @escaping @Sendable (Error?) -> Task<Void, Never>,
        transitions: ScreenCaptureTransitions = ScreenCaptureTransitions(),
        presentPicker: @escaping @MainActor (ScreenCaptureAudioInputProvider) -> Void = { provider in
            let picker = SCContentSharingPicker.shared
            let configuration = SCContentSharingPickerConfiguration()
            picker.configuration = configuration
            picker.defaultConfiguration = configuration
            picker.add(provider)
            picker.isActive = true
            picker.present()
        },
        dismissPicker: @escaping @MainActor (any SCContentSharingPickerObserver) -> Void = { observer in
            let picker = SCContentSharingPicker.shared
            picker.isActive = false
            picker.remove(observer)
        }
    ) {
        self.options = options
        self.targetFormat = targetFormat
        self.onTermination = onTermination
        self.audio = ScreenCaptureAudioProcessor(
            targetFormat: targetFormat, useNativeConversion: useNativeConversion, input: inputContinuation
        ) { error in
            _ = onTermination(error)
        }
        self.transitions = transitions
        self.dismissPicker = dismissPicker
        self.presentPicker = presentPicker
        super.init()
    }

    @MainActor
    func start() async throws {
        try Task.checkCancellation()
        guard !stopped else { throw CancellationError() }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            selectionContinuation = continuation
            presentPicker(self)
        }
        try Task.checkCancellation()
    }

    @MainActor
    func pause() async -> Bool {
        do {
            try await transitions.pause().value
            return transitions.desiredState == .paused
        } catch let error as ScreenCaptureDrainFailure {
            if !stopped {
                _ = onTermination(SpeechSessionError.screenCaptureFailed(error.underlyingError))
            }
            return false
        } catch {
            return false
        }
    }

    @MainActor
    func resume(onCompletion: @escaping @MainActor (Result<Void, Error>) -> Void) async -> Error? {
        // Session control is now an owned child. Return its failure before the
        // public caller joins terminal cleanup, which must itself join this child.
        await transitions.resume(onCompletion: onCompletion).value
    }

    @MainActor
    func stop() -> Task<Void, Error> {
        stopped = true
        let operation = transitions.stop()
        dismissPicker(self)
        resumeSelection(throwing: SpeechSessionError.screenCaptureSelectionCancelled)
        let tasks = Array(captureTasks.values)
        return Task {
            let result = await operation.result
            for task in tasks { await task.value }
            do {
                try result.get()
            } catch {
                audio.discard()
                if let drainFailure = error as? ScreenCaptureDrainFailure {
                    throw SpeechSessionError.screenCaptureFailed(drainFailure.underlyingError)
                }
                throw error
            }
        }
    }

    func contentSharingPicker(
        _ picker: SCContentSharingPicker,
        didUpdateWith filter: SCContentFilter,
        for stream: SCStream?
    ) {
        nonisolated(unsafe) let selectedFilter = filter
        nonisolated(unsafe) let updatedStream = stream
        Task { @MainActor [weak self] in
            guard let self, !self.stopped else { return }
            if let updatedStream {
                guard let current = self.transitions.stream as? NativeScreenCaptureStream,
                      current.stream === updatedStream else { return }
#if os(macOS) || targetEnvironment(macCatalyst)
                self.updateStream(current) { try await current.updateContentFilter(selectedFilter) }
                return
#endif
            }
            self.selectStream { [self] in
                let configuration = SCStreamConfiguration()
                configuration.capturesAudio = true
                configuration.sampleRate = Int(targetFormat.sampleRate)
                configuration.channelCount = Int(targetFormat.channelCount)
                configuration.excludesCurrentProcessAudio = options.excludesCurrentProcessAudio
                configuration.width = 2
                configuration.height = 2
                let capture = SCStream(filter: selectedFilter, configuration: configuration, delegate: self)
                try capture.addStreamOutput(self, type: .audio, sampleHandlerQueue: audio.queue)
                return NativeScreenCaptureStream(stream: capture, audio: audio, stopErrors: stopErrors)
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
                _ = self.onTermination(wrappedError)
            }
        }
    }

    func stream(_ stream: SCStream, didStopWithError error: any Error) {
        if !Self.isUserStop(error) {
            stopErrors.record(error, for: ObjectIdentifier(stream))
        }
        Task { @MainActor [weak self] in
            guard let self, let current = self.transitions.stream as? NativeScreenCaptureStream,
                  current.stream === stream else { return }
            self.streamDidStop(current, error: error)
        }
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer, of type: SCStreamOutputType) {
        guard type == .audio else { return }
        audio.receive(sampleBuffer, from: ObjectIdentifier(stream))
    }

    @discardableResult
    @MainActor
    func selectStream(makeStream: @escaping @MainActor () throws -> any ScreenCaptureStream) -> Task<Void, Never> {
        guard !stopped else { return Task {} }
        return observeSelection(transitions.replace(makeStream: makeStream))
    }

    @discardableResult
    @MainActor
    func updateStream(
        _ stream: any ScreenCaptureStream,
        operation: @escaping @MainActor () async throws -> Void
    ) -> Task<Void, Never> {
        guard !stopped, transitions.isCurrent(stream) else { return Task {} }
        let update = transitions.update(stream, operation: operation)
        let id = UUID()
        filterUpdateID = id
        let task = Task { @MainActor in
            defer { self.captureTasks[id] = nil }
            do {
                try await update.value
            } catch {
                guard !self.stopped, self.filterUpdateID == id, self.transitions.isCurrent(stream) else { return }
                _ = self.onTermination(SpeechSessionError.screenCaptureFailed(error))
            }
        }
        captureTasks[id] = task
        return task
    }

    @MainActor
    private func observeSelection(_ operation: Task<Void, Error>) -> Task<Void, Never> {
        let id = UUID()
        pickerUpdateID = id
        let task = Task { @MainActor in
            defer { self.captureTasks[id] = nil }
            do {
                try await operation.value
                guard !self.stopped, self.pickerUpdateID == id else { return }
                self.resumeSelection()
            } catch {
                guard !self.stopped, self.pickerUpdateID == id else { return }
                let drainFailure = error as? ScreenCaptureDrainFailure
                let selectionError = drainFailure.map { SpeechSessionError.screenCaptureFailed($0.underlyingError) }
                if !self.resumeSelection(throwing: selectionError ?? error) {
                    _ = self.onTermination(selectionError ?? SpeechSessionError.screenCaptureFailed(error))
                }
            }
        }
        captureTasks[id] = task
        return task
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

    @MainActor
    func streamDidStop(_ stream: any ScreenCaptureStream, error: Error) {
        let userStopped = Self.isUserStop(error)
        var failure: Error? = userStopped ? nil : SpeechSessionError.screenCaptureFailed(error)
        if let nativeStream = stream as? NativeScreenCaptureStream {
            let id = ObjectIdentifier(nativeStream.stream)
            if userStopped {
                failure = audio.drain(id).map { SpeechSessionError.screenCaptureFailed($0) }
            } else {
                audio.discard(id)
            }
        }
        let isCurrent = transitions.didStop(stream, error: failure)
        guard !stopped, isCurrent else { return }
        stopped = true
        _ = onTermination(failure)
    }

    private static func isUserStop(_ error: Error) -> Bool {
        let nativeError = error as NSError
        return nativeError.domain == SCStreamError.errorDomain
            && nativeError.code == SCStreamError.Code.userStopped.rawValue
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
