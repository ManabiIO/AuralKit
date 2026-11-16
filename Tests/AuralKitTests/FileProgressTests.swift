import Speech
import Testing
@testable import AuralKit

@available(iOS 26.0, macOS 26.0, *)
@Suite("File transcription progress")
struct FileProgressTests {
    @Test("Progress stops with its file session", arguments: ["stopping", "stopped", "replacement", "cancelled"])
    @MainActor
    func progressStopsWithItsFileSession(termination: String) async {
        let session = SpeechSession()
        let run = FileTranscriptionRun()
        session.fileTranscription = run
        let (_, continuation) = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        session.continuation = .speech(continuation)
        session.streamingMode = .filePlayback
        session.setStatus(.transcribing)

        let (progress, progressContinuation) = AsyncStream<Double>.makeStream()
        let handler: @Sendable (Double) -> Void = { progressContinuation.yield($0) }
        session.reportFileProgress(0.25, handler: handler, run: run)

        switch termination {
        case "stopping":
            session.prepareForStop()
        case "stopped":
            await session.cancelTranscribing()
        case "replacement":
            session.fileTranscription = FileTranscriptionRun()
        default:
            break
        }

        let delivery = Task {
            if termination == "cancelled" {
                withUnsafeCurrentTask { $0?.cancel() }
            }
            session.reportFileProgress(0.5, handler: handler, run: run)
            session.reportFileProgress(1.0, handler: handler, run: run)
        }
        await delivery.value
        progressContinuation.finish()
        var received: [Double] = []
        for await value in progress { received.append(value) }
        #expect(received == [0.25])
        await session.cancelTranscribing()
    }
}
