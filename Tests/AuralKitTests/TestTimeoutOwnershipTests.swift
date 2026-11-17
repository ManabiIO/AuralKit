import Foundation
import Speech
import Testing
@testable import AuralKit

@Suite("Test timeout task ownership")
struct TestTimeoutOwnershipTests {
    @Test("Timeout and caller cancellation join stream consumers and producers", arguments: [false, true])
    @MainActor
    func cancellationJoinsActualTasks(cancelCaller: Bool) async throws {
        let values = AsyncStream<Int>.makeStream()
        let control = AsyncStream<Void>.makeStream()
        let ready = AsyncStream<Void>.makeStream()
        var producerJoined = false
        var consumerJoined = false
        let producer = Task { @MainActor in
            defer {
                values.continuation.finish()
                producerJoined = true
            }
            for await _ in control.stream { values.continuation.yield(1) }
        }
        values.continuation.onTermination = { _ in producer.cancel() }
        let consumer = Task { @MainActor in
            defer { consumerJoined = true }
            var iterator = values.stream.makeAsyncIterator()
            ready.continuation.yield(())
            _ = await iterator.next()
        }
        try await withOwnedTasks([producer, consumer]) {
            try await awaitResult {
                var iterator = ready.stream.makeAsyncIterator()
                _ = await iterator.next()
            }
            let wait = Task { @MainActor in
                try await awaitResult(of: consumer, timeout: cancelCaller ? 60 : 0.01)
            }
            if cancelCaller { wait.cancel() }
            do {
                try await wait.value
                Issue.record("Expected timeout or cancellation")
            } catch {
                if cancelCaller { #expect(error is CancellationError) } else {
                    guard let timeout = error as? TestTimeoutError,
                          case .timedOut(seconds: 0.01) = timeout else {
                        Issue.record("Expected timeout, got \(error)")
                        return
                    }
                }
            }
            #expect(consumerJoined)
            try await awaitResult(of: producer)
        }
        #expect(producerJoined)
        #expect(consumerJoined)
    }

    @Test("Fixture tasks join on successful and throwing operations", arguments: [false, true])
    @MainActor
    func fixtureCleanupJoins(throwsError: Bool) async throws {
        let values = AsyncStream<Int>.makeStream()
        var joined = false
        let consumer = Task { @MainActor in
            defer { joined = true }
            var iterator = values.stream.makeAsyncIterator()
            _ = await iterator.next()
        }
        do {
            try await withOwnedTasks([consumer]) {
                if throwsError { throw VocabularyCompilationTestError.failed }
                values.continuation.yield(1)
                try await awaitResult(of: consumer)
            }
            #expect(!throwsError)
        } catch {
            #expect(throwsError)
            #expect(error as? VocabularyCompilationTestError == .failed)
        }
        #expect(joined)
    }
}
