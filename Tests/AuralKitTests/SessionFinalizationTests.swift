import Testing
import AVFoundation
import Speech
@testable import AuralKit

extension SpeechSessionLifecycleTests {
    @Test("Finish closes a preparing stream and Cancel reports cancellation", arguments: [false, true])
    @MainActor
    func endPreparingTranscription(cancel: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let stream = session.startTranscribing()

        if cancel {
            await session.cancelTranscribing()
            await #expect(throws: CancellationError.self) {
                for try await _ in stream {}
            }
        } else {
            await session.finishTranscribing()
            var iterator = stream.makeAsyncIterator()
            let result = try await iterator.next()
            #expect(result == nil)
        }

        #expect(session.status == .idle)
        #expect(session.continuation == nil)
    }

    @Test("Finish closes a live result stream with no analyzer input")
    @MainActor
    func finishEmptyLiveTranscription() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let (inputs, inputBuilder) = AsyncStream<AnalyzerInput>.makeStream()
        let (results, resultBuilder) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        let run = LiveTranscriptionRun()
        session.liveTranscription = run
        let analyzer = SpeechAnalyzer(modules: [])
        session.inputBuilder = inputBuilder
        session.analyzer = analyzer
        session.analyzerInputTask = Task { try await analyzer.analyzeSequence(inputs) }
        session.continuation = .speech(resultBuilder)
        run.recognizerTask = Task { try? await Task.sleep(for: .seconds(10)) }
        session.recognizerTask = run.recognizerTask
        session.streamingMode = .liveMicrophone
        session.setStatus(.transcribing)

        try await awaitResult(timeout: 3) {
            async let finish: Void = session.finishTranscribing()
            for try await _ in results { Issue.record("Expected no results") }
            await finish
        }
        #expect(session.status == .idle)
        #expect(session.recognizerTask == nil)
    }

    @Test("Input analysis failure closes results before Finish is called")
    @MainActor
    func inputAnalysisFailureClosesResults() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        session.liveTranscription = run
        let (results, resultBuilder) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        session.continuation = .speech(resultBuilder)
        run.recognizerTask = Task { try? await Task.sleep(for: .seconds(10)) }
        session.recognizerTask = run.recognizerTask
        session.streamingMode = .liveMicrophone
        session.setStatus(.transcribing)

        session.startAnalyzingInput(admitSetup: session.captureAnalyzerSetupAdmission()) {
            throw SpeechSessionError.invalidAudioDataType
        }

        do {
            try await awaitResult(timeout: 3) {
                for try await _ in results { Issue.record("Expected no results") }
            }
            Issue.record("Expected input analysis to fail")
        } catch SpeechSessionError.invalidAudioDataType {}
        #expect(session.status == .idle)
        #expect(session.recognizerTask == nil)
    }

}
