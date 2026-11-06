import Foundation
import Speech
import Testing
@testable import AuralKit

@Suite("Custom vocabulary compilation ownership")
struct CustomVocabularyOwnershipTests {
    @Test("Clearing invalidates a suspended compilation")
    @MainActor
    func clearWins() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let old = fixture.configure("old")
            try await fixture.waitForCompilation("old")
            try await fixture.session.configureCustomVocabulary(nil)
            await fixture.compiler.release("old")
            await old.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.customVocabularyDescriptor == nil)
            #expect(fixture.session.customVocabularyConfiguration == nil)
            #expect(fixture.session.customVocabularyOutputDirectory == nil)
        }
    }

    @Test("A late compilation cannot replace or delete the newer vocabulary")
    @MainActor
    func newerCompilationWins() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let old = fixture.configure("old")
            try await fixture.waitForCompilation("old")
            let newer = fixture.configure("new")
            try await fixture.waitForCompilation("new")
            await fixture.compiler.release("new")
            await newer.value
            let directory = try #require(fixture.session.customVocabularyOutputDirectory)
            await fixture.compiler.release("old")
            await old.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.customVocabularyDescriptor == fixture.descriptor("new"))
            #expect(fixture.session.customVocabularyOutputDirectory == directory)
            #expect(FileManager.default.fileExists(atPath: directory.path))
        }
    }

    @Test("Caller cancellation prevents detached compilation publication")
    @MainActor
    func cancelledConfigurationDoesNotPublish() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let task = fixture.configure("cancelled")
            try await fixture.waitForCompilation("cancelled")
            task.cancel()
            await fixture.compiler.release("cancelled")
            await task.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.customVocabularyDescriptor == nil)
            #expect(fixture.session.customVocabularyConfiguration == nil)
        }
    }

    @Test("Public configuration rejects preparing and active dictation", arguments: [false, true])
    @MainActor
    func admittedStreamRejectsConfiguration(active: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            fixture.authorizeOpen = active
            fixture.startPlainDictation()
            try await fixture.waitUntil {
                active ? fixture.session.status == .transcribing : fixture.authorizationEntered
            }
            #expect(fixture.session.status == (active ? .transcribing : .preparing))
            try await fixture.expectIdleRejection(fixture.descriptor("blocked"))
            try await fixture.expectIdleRejection(nil)
            try await fixture.expectStartupIdleRejection()
            #expect(await fixture.compiler.callCount == 0)
        }
    }

    @Test("A compilation admitted while idle cannot publish into a newly preparing stream")
    @MainActor
    func suspendedConfigurationRechecksAdmission() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let task = fixture.configure("old")
            try await fixture.waitForCompilation("old")
            fixture.startPlainDictation()
            try await fixture.waitUntil { fixture.authorizationEntered }
            await fixture.compiler.release("old")
            await task.value
            #expect(fixture.errors.count == 1)
            if let error = fixture.errors.first as? SpeechSessionError {
                if case .customVocabularyRequiresIdleSession = error {} else {
                    Issue.record("Expected idle-session error, got \(error)")
                }
            } else { Issue.record("Expected idle-session error") }
            #expect(fixture.session.customVocabularyDescriptor == nil)
            #expect(fixture.session.status == .preparing)
        }
    }

    @Test("Stopping or cancelling vocabulary startup joins compilation and prevents capture", arguments: [false, true])
    @MainActor
    func cancelledStartupDoesNotStartMicrophone(cancelCaller: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            fixture.authorizeOpen = true
            let startup = fixture.startVocabulary("stopped")
            try await fixture.waitForCompilation("stopped")
            let run = try #require(fixture.session.liveTranscription)
            #expect(fixture.session.status == .preparing)
            var stopped = false
            let stop: Task<Void, Never>?
            if cancelCaller {
                startup.cancel()
                stop = nil
            } else {
                stop = fixture.task {
                    await fixture.session.stopTranscribing()
                    stopped = true
                }
            }
            try await fixture.waitUntil { run.retired }
            #expect(!stopped)
            #expect(fixture.starts == 0)
            await fixture.compiler.release("stopped")
            await startup.value
            await stop?.value
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first is CancellationError)
            #expect(fixture.session.liveTranscription == nil)
            #expect(fixture.session.continuation == nil)
            #expect(fixture.session.customVocabularyDescriptor == nil)
            #expect(fixture.session.status == .idle)
            #expect(fixture.starts == 0)

            let retry = fixture.startVocabulary("retry")
            try await fixture.waitForCompilation("retry")
            await fixture.compiler.release("retry")
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.session.customVocabularyDescriptor == fixture.descriptor("retry"))
            #expect(fixture.starts == 1)
            await fixture.session.stopTranscribing()
            await retry.value
        }
    }

    @Test("An ordinary compiler failure settles admission before immediate retry")
    @MainActor
    func compilerFailureAllowsImmediateRetry() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            fixture.authorizeOpen = true
            let operation = fixture.task {
                do {
                    _ = try await fixture.session.startTranscribing(
                        customVocabulary: fixture.descriptor("failed")
                    )
                    Issue.record("Expected compiler failure")
                } catch {
                    fixture.errors.append(error)
                    #expect(error as? VocabularyCompilationTestError == .failed)
                    #expect(fixture.session.liveTranscription == nil)
                    #expect(fixture.session.continuation == nil)
                    #expect(fixture.session.status == .idle)
                    // Call the public wrapper directly from catch, without yielding to cleanup.
                    do {
                        let stream = try await fixture.session.startTranscribing(
                            customVocabulary: fixture.descriptor("retry")
                        )
                        for try await _ in stream {}
                    } catch { fixture.errors.append(error) }
                }
            }
            try await fixture.waitForCompilation("failed")
            await fixture.compiler.release("failed")
            try await fixture.waitForCompilation("retry")
            #expect(fixture.starts == 0)
            await fixture.compiler.release("retry")
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            #expect(fixture.starts == 1)
            #expect(fixture.errors.count == 1)
            await fixture.session.stopTranscribing()
            await operation.value
            #expect(fixture.errors.count == 1)
        }
    }

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

    @Test("Production compiler cleans failed and rejected output", arguments: ["failed", "clear", "cancelled", "replacement"])
    @MainActor
    func productionRejectedOutputIsRemoved(reason: String) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let gate = VocabularyPreparationGate()
        let compiler = CustomVocabularyCompiler(prepareModel: { descriptor, asset, _ in
            try Data("partial".utf8).write(to: asset)
            let model = asset.deletingLastPathComponent().appendingPathComponent("languageModel.bin")
            try Data("model".utf8).write(to: model)
            if descriptor.identifier == "rejected" {
                await gate.enter(asset.deletingLastPathComponent())
                if reason == "failed" { throw VocabularyCompilationTestError.failed }
            }
        })
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
            if reason == "clear" { try await session.configureCustomVocabulary(nil) }
            if reason == "cancelled" { task.cancel() }
            if reason == "replacement" {
                try await session.configureCustomVocabulary(.init(
                    locale: Locale(identifier: "en_US"), identifier: "current", version: "1"
                ))
            }
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
    private func withFixture(_ operation: @MainActor (VocabularyFixture) async throws -> Void) async throws {
        let fixture = VocabularyFixture()
        do { try await operation(fixture) }
        catch { await fixture.cleanup(); throw error }
        await fixture.cleanup()
    }
}

private enum VocabularyCompilationTestError: Error, Equatable {
    case failed
}

@available(iOS 26.0, macOS 26.0, *)
private actor GatedVocabularyCompiler: CustomVocabularyCompiling {
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
        return CustomVocabularyCompilation(
            configuration: SFSpeechLanguageModel.Configuration(
                languageModel: output.appendingPathComponent("model"),
                vocabulary: output.appendingPathComponent("vocabulary")
            ),
            cacheKey: key,
            outputDirectory: output
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
private final class VocabularyFixture {
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
            do { try await self.session.configureCustomVocabulary(self.descriptor(identifier)) }
            catch { self.errors.append(error) }
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
            do { for try await _ in stream {} }
            catch { self.errors.append(error) }
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
private actor VocabularyPreparationGate {
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
                if cancelCaller { #expect(error is CancellationError) }
                else {
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
