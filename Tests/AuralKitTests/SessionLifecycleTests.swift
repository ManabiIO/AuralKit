import Testing
import AVFoundation
import Speech
@testable import AuralKit

@Suite("SpeechSession Lifecycle")
struct SpeechSessionLifecycleTests {

    /// A second `startTranscribing()` while a session is active must fail without tearing
    /// down the active session's state.
    @Test("Rejected concurrent start leaves the active session untouched")
    @MainActor
    func rejectedConcurrentStartLeavesActiveSessionUntouched() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        // Simulate an active live session.
        session.streamingMode = .liveMicrophone
        session.setStatus(.transcribing)

        let stream = session.startTranscribing()

        await #expect(throws: SpeechSessionError.self) {
            for try await _ in stream {}
        }

        // Give any (incorrectly) scheduled termination cleanup a chance to run.
        try await Task.sleep(for: .milliseconds(100))

        #expect(session.streamingMode == .liveMicrophone)
        #expect(session.status == .transcribing)

        session.streamingMode = .inactive
        session.setStatus(.idle)
    }

    /// Owned retirement must release the stored continuation so the
    /// session can be started again later.
    @Test("Stream termination releases the stored continuation")
    @MainActor
    func streamTerminationReleasesContinuation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        let (_, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        session.continuation = .speech(continuation)

        let run = LiveTranscriptionRun()
        session.liveTranscription = run
        await session.retireLiveTranscription(run, error: nil)?.value

        #expect(session.continuation == nil)
        #expect(session.status == .idle)
        #expect(session.streamingMode == .inactive)
    }

    /// A termination handler from an older stream run must not touch the state of a
    /// newer session.
    @Test("Stale stream termination is ignored")
    @MainActor
    func staleStreamTerminationIsIgnored() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        let stale = LiveTranscriptionRun()
        let current = LiveTranscriptionRun()
        session.liveTranscription = current

        let (_, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        session.continuation = .speech(continuation)
        session.streamingMode = .liveMicrophone
        session.setStatus(.transcribing)

        await session.retireLiveTranscription(stale, error: nil)?.value

        #expect(session.continuation != nil)
        #expect(session.streamingMode == .liveMicrophone)
        #expect(session.status == .transcribing)

        continuation.finish()
        session.continuation = nil
        session.streamingMode = .inactive
        session.setStatus(.idle)
    }

    /// Stale cleanup calls (e.g. from a cancelled pipeline of a previous stream) must not
    /// tear down a newer session's state.
    @Test("Stale cleanup is ignored")
    @MainActor
    func staleCleanupIsIgnored() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        let stale = LiveTranscriptionRun()
        let current = LiveTranscriptionRun()
        session.liveTranscription = current

        session.streamingMode = .filePlayback
        session.setStatus(.transcribing)

        await session.retireLiveTranscription(stale, error: nil)?.value

        #expect(session.streamingMode == .filePlayback)
        #expect(session.status == .transcribing)

        session.streamingMode = .inactive
        session.setStatus(.idle)
    }

    /// A teardown belonging to an older stream must never finish the current stream. This
    /// guards paths (such as audio interruption) that retain a run across an await
    /// and request retirement after the session has moved on.
    @Test("Stale finishStream leaves the current stream open")
    @MainActor
    func staleFinishStreamLeavesCurrentStreamOpen() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        let stale = LiveTranscriptionRun()
        let current = LiveTranscriptionRun()
        session.liveTranscription = current

        let (_, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        session.continuation = .speech(continuation)

        await session.retireLiveTranscription(stale, error: SpeechSessionError.invalidAudioDataType)?.value

        #expect(session.continuation != nil)

        // The current run still owns the stream and can finish it.
        await session.retireLiveTranscription(current, error: nil)?.value
        #expect(session.continuation == nil)
    }

    /// Pausing is only meaningful for live capture; during file transcription it must be
    /// refused (previously, resuming a "paused" file session started the microphone).
    @Test("Pause is refused during file transcription")
    @MainActor
    func pauseIsRefusedDuringFileTranscription() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        session.streamingMode = .filePlayback
        session.setStatus(.transcribing)

        await session.pauseTranscribing()

        #expect(session.status == .transcribing)

        session.streamingMode = .inactive
        session.setStatus(.idle)
    }

    /// Resuming must never start the microphone for a non-live streaming mode.
    @Test("Resume is refused during file transcription")
    @MainActor
    func resumeIsRefusedDuringFileTranscription() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        session.streamingMode = .filePlayback
        session.setStatus(.paused)

        try await session.resumeTranscribing()

        #expect(session.status == .paused)
        #expect(session.isAudioStreaming == false)

        session.streamingMode = .inactive
        session.setStatus(.idle)
    }
}

@Suite("BufferConverter")
struct BufferConverterTests {

    private func makeBuffer(sampleRate: Double, channels: AVAudioChannelCount) throws -> AVAudioPCMBuffer {
        let format = try #require(AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: channels))
        let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 1024))
        buffer.frameLength = 1024
        return buffer
    }

    /// The converter must adapt when the input format changes mid-session (e.g. after an
    /// audio route change swaps the capture device).
    @Test("Converter survives input format changes")
    func converterSurvivesInputFormatChanges() throws {
        let converter = BufferConverter()
        let targetFormat = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))

        let first = try converter.convertBuffer(
            try makeBuffer(sampleRate: 44_100, channels: 1),
            to: targetFormat
        )
        #expect(first.format == targetFormat)
        #expect(first.frameLength > 0)

        // Same target, different source format — previously reused the stale converter.
        let second = try converter.convertBuffer(
            try makeBuffer(sampleRate: 48_000, channels: 2),
            to: targetFormat
        )
        #expect(second.format == targetFormat)
        #expect(second.frameLength > 0)
    }

    /// Matching formats pass the buffer through untouched.
    @Test("Converter passes through matching formats")
    func converterPassesThroughMatchingFormats() throws {
        let converter = BufferConverter()
        let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 512))
        buffer.frameLength = 512

        let result = try converter.convertBuffer(buffer, to: format)
        #expect(result === buffer)
    }
}

extension SpeechSessionLifecycleTests {
    @Test("Stale cancelled setup preserves a replacement run", arguments: [false, true])
    @MainActor
    func staleCancelledSetupPreservesReplacementTask(dictation: Bool) async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let old = LiveTranscriptionRun()
        let replacement = LiveTranscriptionRun()
        session.liveTranscription = replacement
        replacement.setupTask = Task<Void, Never> { }
        let stale = Task { @MainActor in
            if dictation {
                let (_, continuation) = AsyncThrowingStream<DictationTranscriber.Result, Error>.makeStream()
                await session.startDictationPipeline(run: old, with: continuation)
            } else {
                let (_, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
                await session.startSpeechPipeline(run: old, with: continuation)
            }
        }
        stale.cancel()
        await stale.value
        #expect(replacement.setupTask != nil)
        #expect(session.liveTranscription === replacement)
        #expect(!replacement.retired)
        await session.stopTranscribing()
    }

    @Test("Stop joins a capture input task installed during retired setup")
    @MainActor
    func stopJoinsLateCaptureInput() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        session.liveTranscription = run
        let (setupGate, setupRelease) = AsyncStream<Void>.makeStream()
        let (inputGate, inputRelease) = AsyncStream<Void>.makeStream()
        let (entered, signalEntered) = AsyncStream<Void>.makeStream()
        var inputCancelled = false
        var stopped = false
        run.setupTask = Task { @MainActor in
            // A separate iterator survives setup cancellation until the fixture releases it.
            let waiter = Task { for await _ in setupGate {} }
            await waiter.value
            run.inputTask = Task { @MainActor in
                signalEntered.yield(())
                signalEntered.finish()
                let inputWaiter = Task { for await _ in inputGate {} }
                await inputWaiter.value
                inputCancelled = Task.isCancelled
            }
        }
        let stop = Task { @MainActor in
            await session.stopTranscribing()
            stopped = true
        }
        // Latch retirement synchronously, so setup is definitely cancelled before release.
        let terminal = session.retireLiveTranscription(run, error: nil)
        setupRelease.finish()
        for await _ in entered {}
        #expect(!stopped)
        #expect(session.liveTranscription === run)
        inputRelease.finish()
        await terminal?.value
        await stop.value
        #expect(stopped)
        #expect(inputCancelled)
        #expect(run.inputTask == nil)
        #expect(session.liveTranscription == nil)
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private final class PendingScreenInput: ScreenCaptureInput {
    let selection: AsyncStream<Void>
    let endSelection: AsyncStream<Void>.Continuation
    var stopped = false
    var pauseEntered: AsyncStream<Void>.Continuation?
    var pauseGate: AsyncStream<Void>?

    init() {
        let pair = AsyncStream<Void>.makeStream()
        selection = pair.stream
        endSelection = pair.continuation
    }

    func start() async throws {
        let waiter = Task { for await _ in selection {} }
        await waiter.value
        try Task.checkCancellation()
    }
    func pause() async {
        pauseEntered?.finish()
        if let pauseGate { for await _ in pauseGate {} }
    }
    func resume() async throws {}
    func stop() async {
        stopped = true
        endSelection.finish()
    }
}

extension SpeechSessionLifecycleTests {
    @Test("Stop dismisses pending screen input before joining its setup")
    @MainActor
    func stopDismissesPendingScreenInput() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        let provider = PendingScreenInput()
        session.liveTranscription = run
        session.screenCaptureInputProvider = provider
        var setupExited = false
        run.setupTask = Task { @MainActor in
            try? await provider.start()
            setupExited = true
        }
        await session.stopTranscribing()
        #expect(provider.stopped)
        #expect(setupExited)
        #expect(session.screenCaptureInputProvider == nil)
        #expect(session.liveTranscription == nil)
    }

    @Test("Late screen pause cannot change a retired session status")
    @MainActor
    func retiredScreenPausePreservesIdle() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        let provider = PendingScreenInput()
        let (entered, signal) = AsyncStream<Void>.makeStream()
        let (gate, release) = AsyncStream<Void>.makeStream()
        provider.pauseEntered = signal
        provider.pauseGate = gate
        session.liveTranscription = run
        session.screenCaptureInputProvider = provider
        session.streamingMode = .screenCapture
        session.setStatus(.transcribing)
        let pause = Task { @MainActor in await session.pauseTranscribing() }
        for await _ in entered {}
        await session.stopTranscribing()
        release.finish()
        await pause.value
        #expect(session.status == .idle)
        #expect(session.streamingMode == .inactive)
    }
}
