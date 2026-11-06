import Foundation

enum TestTimeoutError: LocalizedError {
    case timedOut(seconds: TimeInterval)

    var errorDescription: String? {
        switch self {
        case let .timedOut(seconds):
            return "Timed out waiting for async result after \(seconds)s"
        }
    }
}

@MainActor
func awaitResult<T: Sendable>(
    timeout seconds: TimeInterval = 1.0,
    operation: @escaping @MainActor () async throws -> T
) async throws -> T {
    let nanoseconds = UInt64(seconds * 1_000_000_000)

    // Operations must cooperate with cancellation. Use the task overload when
    // awaiting independently owned iterator tasks; cancelling .value alone does
    // not cancel those tasks. Structured scope exit joins both children.
    return try await withThrowingTaskGroup(of: T.self) { group in
        defer { group.cancelAll() }
        group.addTask {
            try await operation()
        }

        group.addTask {
            try await Task.sleep(nanoseconds: nanoseconds)
            throw TestTimeoutError.timedOut(seconds: seconds)
        }

        guard let value = try await group.next() else {
            fatalError("Timeout helper started without any tasks")
        }

        return value
    }
}

/// Owns cancellation of the actual task, then joins it before returning or throwing.
/// This bounds cancellation-cooperative work such as AsyncStream iteration, not
/// arbitrary tasks that ignore cancellation.
@MainActor
func awaitResult<T: Sendable>(
    of task: Task<T, Never>, timeout seconds: TimeInterval = 1.0
) async throws -> T {
    try await awaitResult(timeout: seconds) {
        try await withTaskCancellationHandler {
            let value = await task.value
            try Task.checkCancellation()
            return value
        } onCancel: {
            task.cancel()
        }
    }
}

/// Cleans up every fixture task even if an earlier readiness/result wait fails.
@MainActor
func withOwnedTasks<T: Sendable, Result>(
    _ tasks: [Task<T, Never>], operation: @MainActor () async throws -> Result
) async rethrows -> Result {
    do {
        let result = try await operation()
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
        return result
    } catch {
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
        throw error
    }
}
