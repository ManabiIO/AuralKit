import Foundation
import AVFoundation
import Speech
@testable import AuralKit

enum LiveProbeError: Error, Equatable { case finalization }

@available(iOS 26.0, macOS 26.0, *)
struct DelayedLiveResults: AsyncSequence, Sendable {
    typealias Element = String
    let gate: FileLifecycleGate

    func makeAsyncIterator() -> Iterator { Iterator(gate: gate) }

    struct Iterator: AsyncIteratorProtocol {
        let gate: FileLifecycleGate
        var hasYielded = false

        mutating func next() async -> String? {
            guard !hasYielded else { return nil }
            hasYielded = true
            await gate.wait()
            return "delayed tail"
        }
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
func withLiveFixture(_ operation: @MainActor (LiveLifecycleFixture) async throws -> Void) async throws {
    let fixture = LiveLifecycleFixture()
    do {
        try await operation(fixture)
        await fixture.cleanup()
    } catch {
        await fixture.cleanup()
        throw error
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class LiveLifecycleFixture {
    let session = SpeechSession()
    let results = AsyncThrowingStream<String, Error>.makeStream()
    let forwarded = AsyncThrowingStream<String, Error>.makeStream()
    var inputs: AsyncStream<AnalyzerInput>?
    var received: [String] = []
    var errors: [Error] = []
    var finalizations = 0
    var cancellations = 0
    var publicConsumer: Task<Void, Never>?
    var forwardingConsumer: Task<Void, Never>?
    private var gates: [FileLifecycleGate] = []
    private var tasks: [Task<Void, Never>] = []

    init() {
        session.liveTranscriptionHooks = LiveTranscriptionHooks(
            authorize: {},
            preparePipeline: { generation in
                let input = AsyncStream<AnalyzerInput>.makeStream()
                self.inputs = input.stream
                self.session.inputBuilder = input.continuation
                self.session.analyzerFormat = AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1)
                self.session.recognizerTask = self.session.createRecognizerTask(
                    label: "Live test reader", results: self.results.stream,
                    streamContinuation: self.forwarded.continuation, generation: generation
                )
            },
            finalizeInput: {
                self.finalizations += 1
                self.results.continuation.yield("final tail")
                self.results.continuation.finish()
            },
            cancelAnalyzer: {
                self.cancellations += 1
                self.results.continuation.finish()
            },
            configureAudioSession: {}
        )
        forwardingConsumer = task {
            do {
                for try await result in self.forwarded.stream { self.received.append(result) }
            } catch { self.errors.append(error) }
        }
    }

    func start() { consume(session.startTranscribing()) }

    func consume<Result: Sendable>(_ stream: AsyncThrowingStream<Result, Error>) {
        publicConsumer = task {
            do { for try await _ in stream {} } catch { self.errors.append(error) }
        }
    }

    func task(_ operation: @escaping @MainActor () async -> Void) -> Task<Void, Never> {
        let task = Task { @MainActor in await operation() }
        tasks.append(task)
        return task
    }

    func gate() -> FileLifecycleGate {
        let gate = FileLifecycleGate()
        gates.append(gate)
        return gate
    }

    func waitUntil(_ predicate: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while !predicate() {
            guard ContinuousClock.now < deadline else { throw TestTimeoutError.timedOut(seconds: 5) }
            try await Task.sleep(for: .milliseconds(1))
        }
    }

    func cleanup() async {
        for gate in gates { gate.open() }
        results.continuation.finish()
        forwarded.continuation.finish()
        await session.cancelTranscribing()
        for task in tasks { await task.value }
        session.liveTranscriptionHooks = nil
        tasks.removeAll()
    }
}
