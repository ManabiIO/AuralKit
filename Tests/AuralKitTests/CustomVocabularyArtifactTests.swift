import Foundation
import Speech
import Testing
@testable import AuralKit

extension CustomVocabularyOwnershipTests {
    @Test("Production allocation isolates equal descriptors across sessions", arguments: [false, true])
    @MainActor
    func productionDirectoriesArePrivate(replace: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        // Only Apple model work is substituted. The actual compiler allocates
        // and transfers real Application Support directories for both sessions.
        let compiler = CustomVocabularyCompiler(prepareModel: { _, asset, _ in
            let directory = asset.deletingLastPathComponent()
            try Data("model".utf8).write(to: directory.appendingPathComponent("languageModel.bin"))
            try Data("vocabulary".utf8).write(to: directory.appendingPathComponent("vocabulary.bin"))
            try Data("source".utf8).write(to: asset)
        })
        let first = SpeechSession(locale: Locale(identifier: "en_US"))
        let second = SpeechSession(locale: Locale(identifier: "en_US"))
        first.customVocabularyCompiler = compiler
        second.customVocabularyCompiler = compiler
        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "en_US"), identifier: "private-ownership", version: "1"
        )
        do {
            try await first.configureCustomVocabulary(descriptor)
            try await second.configureCustomVocabulary(descriptor)
            let firstDirectory = try #require(first.customVocabularyOutputDirectory)
            let secondDirectory = try #require(second.customVocabularyOutputDirectory)
            #expect(firstDirectory != secondDirectory)
            #expect(first.customVocabularyCacheKey == second.customVocabularyCacheKey)
            try await second.configureCustomVocabulary(descriptor)
            #expect(second.customVocabularyOutputDirectory == secondDirectory)
            if replace {
                try await first.configureCustomVocabulary(.init(
                    locale: descriptor.locale, identifier: "replacement", version: "1"
                ))
            } else {
                try await first.configureCustomVocabulary(nil)
            }
            #expect(!FileManager.default.fileExists(atPath: firstDirectory.path))
            let model = secondDirectory.appendingPathComponent("languageModel.bin")
            let vocabulary = secondDirectory.appendingPathComponent("vocabulary.bin")
            let source = secondDirectory.appendingPathComponent("source.bin")
            #expect(try Data(contentsOf: model) == Data("model".utf8))
            #expect(try Data(contentsOf: vocabulary) == Data("vocabulary".utf8))
            #expect(!FileManager.default.fileExists(atPath: source.path))
            try await second.configureCustomVocabulary(nil)
            #expect(!FileManager.default.fileExists(atPath: secondDirectory.path))
        } catch {
            try? await first.configureCustomVocabulary(nil)
            try? await second.configureCustomVocabulary(nil)
            throw error
        }
        try await first.configureCustomVocabulary(nil)
    }

    @Test("Releasing idle sessions retires only their own production output")
    @MainActor
    func idleSessionReleaseRetiresPrivateOutput() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let compiler = CustomVocabularyCompiler(prepareModel: { _, asset, _ in
            let directory = asset.deletingLastPathComponent()
            try Data("model".utf8).write(to: directory.appendingPathComponent("languageModel.bin"))
            try Data("vocabulary".utf8).write(to: directory.appendingPathComponent("vocabulary.bin"))
            try Data("source".utf8).write(to: asset)
        })
        var first: SpeechSession? = SpeechSession(locale: Locale(identifier: "en_US"))
        var second: SpeechSession? = SpeechSession(locale: Locale(identifier: "en_US"))
        weak var firstOwner = first
        weak var secondOwner = second
        defer { first = nil; second = nil }
        first?.customVocabularyCompiler = compiler
        second?.customVocabularyCompiler = compiler
        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "en_US"), identifier: "idle-release-ownership", version: "1"
        )
        try await first?.configureCustomVocabulary(descriptor)
        try await second?.configureCustomVocabulary(descriptor)
        let firstDirectory = try #require(first?.customVocabularyOutputDirectory)
        let secondDirectory = try #require(second?.customVocabularyOutputDirectory)
        let cacheKey = try #require(second?.customVocabularyCacheKey)
        #expect(firstDirectory != secondDirectory)
        #expect(first?.customVocabularyCacheKey == cacheKey)

        // No explicit clear, stop or direct filesystem cleanup retires either owner.
        first = nil
        #expect(firstOwner == nil)
        #expect(!FileManager.default.fileExists(atPath: firstDirectory.path))
        try await second?.configureCustomVocabulary(descriptor)
        #expect(second?.customVocabularyOutputDirectory == secondDirectory)
        #expect(second?.customVocabularyCacheKey == cacheKey)
        #expect(second?.customVocabularyConfiguration != nil)
        let model = secondDirectory.appendingPathComponent("languageModel.bin")
        let vocabulary = secondDirectory.appendingPathComponent("vocabulary.bin")
        let source = secondDirectory.appendingPathComponent("source.bin")
        #expect(try Data(contentsOf: model) == Data("model".utf8))
        #expect(try Data(contentsOf: vocabulary) == Data("vocabulary".utf8))
        #expect(!FileManager.default.fileExists(atPath: source.path))

        second = nil
        #expect(secondOwner == nil)
        #expect(!FileManager.default.fileExists(atPath: secondDirectory.path))
    }

    @Test("A queued live result reader retains vocabulary through retirement")
    @MainActor
    func queuedReaderRetainsArtifactOwner() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        var session: SpeechSession? = SpeechSession(locale: Locale(identifier: "en_US"))
        session?.customVocabularyCompiler = CustomVocabularyCompiler(prepareModel: { _, asset, _ in
            try Data("model".utf8).write(
                to: asset.deletingLastPathComponent().appendingPathComponent("languageModel.bin")
            )
        })
        try await session?.configureCustomVocabulary(.init(
            locale: Locale(identifier: "en_US"), identifier: "queued-reader-ownership", version: "1"
        ))
        let directory = try #require(session?.customVocabularyOutputDirectory)
        let run = LiveTranscriptionRun()
        let results = AsyncStream<Int>.makeStream()
        let forwarded = AsyncThrowingStream<Int, Error>.makeStream()
        // Exercise the production reader and terminal join without native Speech work.
        session?.liveTranscription = run
        let reader = try #require(session?.makeLiveRecognizerTask(
            run: run, results: results.stream, streamContinuation: forwarded.continuation
        ))
        run.recognizerTask = reader
        weak var owner = session
        session = nil
        // No suspension since reader creation: it must own the session even
        // before its first actor turn, not only after upgrading a weak capture.
        #expect(owner != nil)
        let model = directory.appendingPathComponent("languageModel.bin")
        #expect(FileManager.default.fileExists(atPath: model.path))
        results.continuation.finish()
        await reader.value
        await run.terminalTask?.value
        #expect(owner == nil)
        #expect(!FileManager.default.fileExists(atPath: directory.path))
        forwarded.continuation.finish()
    }

    @Test(
        "Production compiler cleans failed and rejected output",
        arguments: ["failed", "clear", "cancelled", "replacement"]
    )
    @MainActor
    func productionRejectedOutputIsRemoved(reason: String) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let gate = VocabularyPreparationGate()
        let compiler = rejectingCompiler(reason: reason, gate: gate)
        let session = SpeechSession(locale: Locale(identifier: "en_US"))
        session.customVocabularyCompiler = compiler
        let task = Task { @MainActor in
            try await session.configureCustomVocabulary(.init(
                locale: Locale(identifier: "en_US"), identifier: "rejected", version: "1"
            ))
        }
        do {
            let location = try await awaitResult { await gate.directory() }
            let directory = try #require(location)
            try await applyRejection(reason, to: session, task: task)
            let currentDirectory = session.customVocabularyOutputDirectory
            await gate.release()
            do {
                try await task.value
                Issue.record("Expected rejected compilation")
            } catch {
                if reason == "failed" {
                    guard let error = error as? SpeechSessionError,
                          case .customVocabularyCompilationFailed = error else {
                        Issue.record("Expected compilation failure, got \(error)")
                        return
                    }
                } else { #expect(error is CancellationError) }
            }
            #expect(!FileManager.default.fileExists(atPath: directory.path))
            if reason == "replacement" {
                let current = try #require(currentDirectory)
                #expect(session.customVocabularyOutputDirectory == current)
                let model = current.appendingPathComponent("languageModel.bin")
                #expect(try Data(contentsOf: model) == Data("model".utf8))
            } else {
                #expect(session.customVocabularyConfiguration == nil)
            }
        } catch {
            task.cancel()
            await gate.release()
            _ = await task.result
            try? await session.configureCustomVocabulary(nil)
            throw error
        }
        try await session.configureCustomVocabulary(nil)
    }

    @MainActor
    @available(iOS 26.0, macOS 26.0, *)
    private func rejectingCompiler(
        reason: String, gate: VocabularyPreparationGate
    ) -> CustomVocabularyCompiler {
        return CustomVocabularyCompiler(prepareModel: { descriptor, asset, _ in
            try Data("partial".utf8).write(to: asset)
            let model = asset.deletingLastPathComponent().appendingPathComponent("languageModel.bin")
            try Data("model".utf8).write(to: model)
            if descriptor.identifier == "rejected" {
                await gate.enter(asset.deletingLastPathComponent())
                if reason == "failed" { throw VocabularyCompilationTestError.failed }
            }
        })
    }

    @MainActor
    @available(iOS 26.0, macOS 26.0, *)
    private func applyRejection(
        _ reason: String, to session: SpeechSession, task: Task<Void, Error>
    ) async throws {
        if reason == "clear" { try await session.configureCustomVocabulary(nil) }
        if reason == "cancelled" { task.cancel() }
        if reason == "replacement" {
            try await session.configureCustomVocabulary(.init(
                locale: Locale(identifier: "en_US"), identifier: "current", version: "1"
            ))
        }
    }
}
