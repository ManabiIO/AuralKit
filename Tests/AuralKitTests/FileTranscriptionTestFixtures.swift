import Foundation
import Testing
@testable import AuralKit

enum FileLifecycleProbeError: Error, Equatable { case ingestion, finalization, arrivalTimeout }

@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class FileLifecycleGate {
    private(set) var entered = false
    private var isOpen = false
    private var releases: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        entered = true
        guard !isOpen else { return }
        await withCheckedContinuation { releases.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = releases
        releases.removeAll()
        for continuation in continuations { continuation.resume() }
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class FileLifecycleFixture {
    let session = SpeechSession()
    let audioURL: URL
    var errors: [Error] = []
    var ingestionReturned = false
    private var gates: [FileLifecycleGate] = []
    private var tasks: [Task<Void, Never>] = []

    init(audioURL: URL) { self.audioURL = audioURL }

    func gate() -> FileLifecycleGate {
        let gate = FileLifecycleGate()
        gates.append(gate)
        return gate
    }

    func task(_ operation: @escaping @MainActor () async -> Void) -> Task<Void, Never> {
        let task = Task { @MainActor in await operation() }
        tasks.append(task)
        return task
    }

    func consume() -> Task<Void, Never> {
        // Admit synchronously so tests can immediately inspect the actual run.
        let stream = session.streamTranscription(from: audioURL)
        return task {
            do {
                for try await _ in stream {}
            } catch {
                self.errors.append(error)
            }
        }
    }

    func transcribe() -> Task<Void, Never> {
        task {
            do {
                _ = try await self.session.transcribe(audioFile: self.audioURL)
            } catch {
                self.errors.append(error)
            }
        }
    }

    func waitUntil(_ predicate: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while !predicate() {
            guard ContinuousClock.now < deadline else { throw FileLifecycleProbeError.arrivalTimeout }
            try await Task.sleep(for: .milliseconds(1))
        }
    }

    func expectSuccessfulRetirement(_ run: FileTranscriptionRun) {
        #expect(errors.isEmpty)
        #expect(session.fileTranscription == nil)
        #expect(session.status == .idle)
        #expect(run.ingestionTask == nil)
        #expect(run.recognizerTask == nil)
    }

    func cleanup() async {
        // Open-before-stop also covers late task admission after a failed wait.
        for gate in gates { gate.open() }
        await session.stopTranscribing()
        for task in tasks { await task.value }
        session.fileTranscriptionHooks = nil
        tasks.removeAll()
        try? FileManager.default.removeItem(at: audioURL)
    }
}
