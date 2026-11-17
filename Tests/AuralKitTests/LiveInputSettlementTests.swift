import Foundation
import Speech
import Testing
@testable import AuralKit

@Suite("Live input settlement")
@MainActor
struct LiveInputSettlementTests {
    @Test("Native result EOF waits for input finalization", arguments: [false, true])
    func resultsWaitForInput(fails: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withSession { runtime, fixture in
            let release = fixture.gate()
            let results = AsyncThrowingStream<Int, Error>.makeStream()
            let forwarded = AsyncThrowingStream<Int, Error>.makeStream()
            defer { forwarded.continuation.finish() }
            runtime.run.usesNativeInput = true
            let reader = fixture.track(runtime.session.makeLiveRecognizerTask(
                run: runtime.run, results: results.stream, streamContinuation: forwarded.continuation
            ))
            runtime.run.recognizerTask = reader
            runtime.session.recognizerTask = reader
            runtime.run.inputTask = fixture.track(Task { @MainActor in
                do {
                    try await release.wait()
                    if fails { throw SpeechSessionError.invalidAudioDataType }
                    try runtime.session.requireCurrentLiveTranscription(runtime.run)
                    runtime.session.completeLiveInput(runtime.run)
                } catch {
                    runtime.session.retireLiveTranscription(runtime.run, error: error)
                }
            })
            results.continuation.finish()
            await reader.value
            #expect(!runtime.run.retired)
            #expect(runtime.session.continuation != nil)
            release.open()
            if fails {
                await #expect(throws: SpeechSessionError.self) {
                    try await awaitResult(timeout: 3) {
                        for try await _ in runtime.results { Issue.record("Unexpected result") }
                    }
                }
            } else {
                try await awaitResult(timeout: 3) {
                    for try await _ in runtime.results { Issue.record("Unexpected result") }
                }
            }
            #expect(runtime.session.liveTranscription == nil)
            #expect(runtime.session.status == .idle)
        }
    }

    @Test("Native input completion waits for the final result drain")
    func inputWaitsForResults() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withSession { runtime, fixture in
            let results = AsyncThrowingStream<Int, Error>.makeStream()
            let forwarded = AsyncThrowingStream<Int, Error>.makeStream()
            defer { forwarded.continuation.finish() }
            runtime.run.usesNativeInput = true
            let reader = fixture.track(runtime.session.makeLiveRecognizerTask(
                run: runtime.run, results: results.stream, streamContinuation: forwarded.continuation
            ))
            runtime.run.recognizerTask = reader
            runtime.session.recognizerTask = reader
            runtime.session.completeLiveInput(runtime.run)
            #expect(!runtime.run.retired)
            #expect(runtime.session.continuation != nil)
            results.continuation.finish()
            try await awaitResult(timeout: 3) {
                for try await _ in runtime.results { Issue.record("Unexpected result") }
            }
            await reader.value
            #expect(runtime.session.liveTranscription == nil)
        }
    }

    @Test("Self-cancelled result EOF settles as cancellation")
    func selfCancelledResultEOF() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withSession { runtime, fixture in
            let forwarded = AsyncThrowingStream<Int, Error>.makeStream()
            defer { forwarded.continuation.finish() }
            let reader = fixture.track(runtime.session.makeLiveRecognizerTask(
                run: runtime.run, results: SelfCancelledLiveResults(),
                streamContinuation: forwarded.continuation
            ))
            runtime.run.recognizerTask = reader
            runtime.session.recognizerTask = reader
            await #expect(throws: CancellationError.self) {
                try await awaitResult(timeout: 3) {
                    for try await _ in runtime.results { Issue.record("Unexpected result") }
                }
            }
            await reader.value
            #expect(runtime.session.liveTranscription == nil)
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    @MainActor
    private struct Runtime {
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        let results: AsyncThrowingStream<SpeechTranscriber.Result, Error>
        init() {
            let output = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
            results = output.stream
            session.liveTranscription = run
            session.continuation = .speech(output.continuation)
            session.streamingMode = .liveMicrophone
            session.setStatus(.transcribing)
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    private func withSession(_ body: (Runtime, CaptureFixture) async throws -> Void) async throws {
        try await withCaptureFixture { fixture in
            let runtime = Runtime()
            do {
                try await body(runtime, fixture)
            } catch {
                fixture.releaseAll()
                await runtime.session.stopTranscribing()
                throw error
            }
            fixture.releaseAll()
            await runtime.session.stopTranscribing()
        }
    }
}

private struct SelfCancelledLiveResults: AsyncSequence, Sendable {
    typealias Element = Int
    struct AsyncIterator: AsyncIteratorProtocol {
        mutating func next() async -> Int? {
            withUnsafeCurrentTask { $0?.cancel() }
            return nil
        }
    }
    func makeAsyncIterator() -> AsyncIterator { AsyncIterator() }
}
