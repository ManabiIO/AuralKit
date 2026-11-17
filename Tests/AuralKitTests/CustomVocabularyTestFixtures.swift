import Foundation
import Speech
import Testing
@testable import AuralKit

enum VocabularyCompilationTestError: Error, Equatable {
    case failed
}

@available(iOS 26.0, macOS 26.0, *)
actor GatedVocabularyCompiler: CustomVocabularyCompiling {
    private let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("vocabulary-ownership-\(UUID().uuidString)", isDirectory: true)
    private var pending: [String: CheckedContinuation<Void, Never>] = [:]
    private(set) var callCount = 0
    private var releasedAll = false

    func compile(descriptor: SpeechSession.CustomVocabulary) async throws -> CustomVocabularyCompilation {
        callCount += 1
        let key = try descriptor.stableCacheKey()
        let output = directory.appendingPathComponent("\(key)-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: output, withIntermediateDirectories: true)
        if !releasedAll {
            await withCheckedContinuation { pending[descriptor.identifier] = $0 }
        }
        if descriptor.identifier == "failed" {
            try? FileManager.default.removeItem(at: output)
            throw VocabularyCompilationTestError.failed
        }
        let model = output.appendingPathComponent("model")
        let vocabulary = output.appendingPathComponent("vocabulary")
        try Data("model".utf8).write(to: model)
        try Data("vocabulary".utf8).write(to: vocabulary)
        return CustomVocabularyCompilation(
            configuration: SFSpeechLanguageModel.Configuration(languageModel: model, vocabulary: vocabulary),
            cacheKey: key,
            outputDirectory: output,
            artifacts: CustomVocabularyArtifacts(directory: output)
        )
    }

    func hasEntered(_ identifier: String) -> Bool { pending[identifier] != nil }

    func release(_ identifier: String) { pending.removeValue(forKey: identifier)?.resume() }

    func releaseAll() {
        releasedAll = true
        let continuations = Array(pending.values)
        pending.removeAll()
        continuations.forEach { $0.resume() }
    }

    func cleanup() { try? FileManager.default.removeItem(at: directory) }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
final class VocabularyFixture {
    let session = SpeechSession(locale: Locale(identifier: "en_US"))
    let compiler = GatedVocabularyCompiler()
    var starts = 0
    var errors: [Error] = []
    var authorizationEntered = false
    var authorizeOpen = false
    private var authorization: CheckedContinuation<Void, Never>?
    private var tasks: [Task<Void, Never>] = []

    init() {
        session.customVocabularyCompiler = compiler
        session.liveTranscriptionHooks = LiveTranscriptionHooks(
            authorize: { [unowned self] in
                authorizationEntered = true
                if !authorizeOpen { await withCheckedContinuation { authorization = $0 } }
            },
            preparePipeline: { _, _ in },
            startAudio: { [unowned self] in starts += 1 }
        )
    }

    func descriptor(_ identifier: String) -> SpeechSession.CustomVocabulary {
        .init(locale: Locale(identifier: "en_US"), identifier: identifier, version: "1")
    }

    func task(_ operation: @escaping @MainActor () async -> Void) -> Task<Void, Never> {
        let task = Task { @MainActor in await operation() }
        tasks.append(task)
        return task
    }

    func configure(_ identifier: String) -> Task<Void, Never> {
        task {
            do {
                try await self.session.configureCustomVocabulary(self.descriptor(identifier))
            } catch { self.errors.append(error) }
        }
    }

    func startVocabulary(_ identifier: String) -> Task<Void, Never> {
        task {
            do {
                let stream = try await self.session.startTranscribing(customVocabulary: self.descriptor(identifier))
                for try await _ in stream {}
            } catch { self.errors.append(error) }
        }
    }

    func startPlainDictation() {
        let stream = session.startDictationTranscribing()
        _ = task {
            do { for try await _ in stream {} } catch { self.errors.append(error) }
        }
    }

    func expectIdleRejection(_ vocabulary: SpeechSession.CustomVocabulary?) async throws {
        var completed = false
        let operation = task {
            do {
                try await self.session.configureCustomVocabulary(vocabulary)
                Issue.record("Expected customVocabularyRequiresIdleSession")
            } catch SpeechSessionError.customVocabularyRequiresIdleSession {
                // The admitted stream retains its configuration.
            } catch { Issue.record("Unexpected error: \(error)") }
            completed = true
        }
        try await waitUntil { completed }
        await operation.value
    }

    func expectStartupIdleRejection() async throws {
        var completed = false
        let operation = task {
            do {
                _ = try await self.session.startTranscribing(customVocabulary: self.descriptor("blocked"))
                Issue.record("Expected customVocabularyRequiresIdleSession")
            } catch SpeechSessionError.customVocabularyRequiresIdleSession {
                // The async wrapper preserves the configuration API's error.
            } catch { Issue.record("Unexpected error: \(error)") }
            completed = true
        }
        try await waitUntil { completed }
        await operation.value
    }

    func waitForCompilation(_ identifier: String) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(2))
        while !(await compiler.hasEntered(identifier)) {
            guard ContinuousClock.now < deadline else { throw TestTimeoutError.timedOut(seconds: 2) }
            await Task.yield()
        }
    }

    func waitUntil(_ predicate: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(2))
        while !predicate() {
            guard ContinuousClock.now < deadline else { throw TestTimeoutError.timedOut(seconds: 2) }
            await Task.yield()
        }
    }

    func cleanup() async {
        authorizeOpen = true
        authorization?.resume()
        authorization = nil
        tasks.forEach { $0.cancel() }
        await compiler.releaseAll()
        await session.stopTranscribing()
        for task in tasks { await task.value }
        try? await session.configureCustomVocabulary(nil)
        await compiler.cleanup()
        session.liveTranscriptionHooks = nil
    }
}

@available(iOS 26.0, macOS 26.0, *)
actor VocabularyPreparationGate {
    private let locations = AsyncStream<URL>.makeStream()
    private var pending: CheckedContinuation<Void, Never>?
    private var released = false

    func enter(_ directory: URL) async {
        locations.continuation.yield(directory)
        locations.continuation.finish()
        if !released { await withCheckedContinuation { pending = $0 } }
    }

    func directory() async -> URL? {
        var iterator = locations.stream.makeAsyncIterator()
        return await iterator.next()
    }

    func release() {
        released = true
        pending?.resume()
        pending = nil
    }
}
