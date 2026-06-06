import Testing
import Speech
@testable import AuralKit

@Suite("SpeechSession State")
struct SpeechSessionStateTests {

    @Test("Model download progress starts nil")
    @MainActor
    func modelDownloadProgressStartsNil() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        #expect(await session.modelDownloadProgress == nil)
    }

    @Test("Stop transcribing without session is a no-op")
    @MainActor
    func stopTranscribingWithoutSessionIsNoOp() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        await session.stopTranscribing()
        #expect(await session.modelDownloadProgress == nil)
        #expect(session.status == .idle)
    }

    @Test("Pause when idle does nothing")
    @MainActor
    func pauseWhenIdleDoesNothing() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        #expect(session.status == .idle)

        await session.pauseTranscribing()

        #expect(session.status == .idle)
    }

    @Test("Resume when idle does nothing")
    @MainActor
    func resumeWhenIdleDoesNothing() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        #expect(session.status == .idle)

        try await session.resumeTranscribing()

        #expect(session.status == .idle)
    }

    @Test("Status stream broadcasts updates to multiple subscribers")
    @MainActor
    func statusStreamBroadcastsToMultipleSubscribers() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let streamA = session.statusStream
        let streamB = session.statusStream
        let readyA = AsyncStream<Void>.makeStream()
        let readyB = AsyncStream<Void>.makeStream()
        let nextA = Task {
            var iterator = streamA.makeAsyncIterator()
            let initial = await iterator.next()
            readyA.continuation.yield(())
            let next = await iterator.next()
            return (initial, next)
        }
        let nextB = Task {
            var iterator = streamB.makeAsyncIterator()
            let initial = await iterator.next()
            readyB.continuation.yield(())
            let next = await iterator.next()
            return (initial, next)
        }
        defer {
            nextA.cancel()
            nextB.cancel()
        }

        try await awaitResult {
            var iterator = readyA.stream.makeAsyncIterator()
            _ = await iterator.next()
        }
        try await awaitResult {
            var iterator = readyB.stream.makeAsyncIterator()
            _ = await iterator.next()
        }

        session.setStatus(.preparing)

        let valueA = try await awaitResult {
            await nextA.value
        }
        let valueB = try await awaitResult {
            await nextB.value
        }
        #expect(valueA.0 == .some(.idle))
        #expect(valueA.1 == .some(.preparing))
        #expect(valueB.0 == .some(.idle))
        #expect(valueB.1 == .some(.preparing))
    }
}

@Suite("SpeechSession Voice Activation")
struct SpeechSessionVoiceActivationTests {

    @Test("Voice activation configuration toggles state")
    @MainActor
    func voiceActivationConfigurationTogglesState() {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        #expect(session.isVoiceActivationEnabled == false)
        #expect(session.speechDetectorResultsStream == nil)
        #expect(session.isSpeechDetected == true)

        session.configureVoiceActivation(reportResults: true)
        #expect(session.isVoiceActivationEnabled == true)
        #expect(session.speechDetectorResultsStream != nil)
        #expect(session.isSpeechDetected == true)

        session.disableVoiceActivation()
        #expect(session.isVoiceActivationEnabled == false)
        #expect(session.speechDetectorResultsStream == nil)
        #expect(session.isSpeechDetected == true)
    }

    @Test("Disabling voice activation resets isSpeechDetected")
    @MainActor
    func disablingVoiceActivationResetsSpeechDetected() {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()

        session.configureVoiceActivation(reportResults: true)
        session.isSpeechDetected = false

        session.disableVoiceActivation()

        #expect(session.isSpeechDetected == true)
    }
}

@Suite("SpeechSession Audio Input Stream")
struct SpeechSessionAudioInputStreamTests {

    @Test("Audio input stream fans out nil updates")
    @MainActor
    func audioInputStreamBroadcastsNil() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let streamA = session.audioInputConfigurationStream
        let streamB = session.audioInputConfigurationStream
        let valueA = Task {
            var iteratorA = streamA.makeAsyncIterator()
            return await iteratorA.next()
        }
        let valueB = Task {
            var iteratorB = streamB.makeAsyncIterator()
            return await iteratorB.next()
        }
        defer {
            valueA.cancel()
            valueB.cancel()
        }

        await Task.yield()
        session.broadcastAudioInputInfo(nil)

        let firstA = try await awaitResult {
            await valueA.value
        }
        let firstB = try await awaitResult {
            await valueB.value
        }
        let flattenedA = firstA?.flatMap { $0 }
        let flattenedB = firstB?.flatMap { $0 }
        #expect(flattenedA == nil)
        #expect(flattenedB == nil)
    }
}

@Suite("SpeechSession Logging")
struct SpeechSessionLoggingTests {

    @Test("Logging level can be configured globally")
    @MainActor
    func loggingLevelRoundTrips() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        for level in SpeechSession.LogLevel.allCases {
            await SpeechSessionLoggingLock.shared.withLock {
                let originalLevel = SpeechSession.logging
                defer { SpeechSession.logging = originalLevel }

                SpeechSession.logging = level

                #expect(SpeechSession.logging == level)
            }
        }
    }
}

@Suite("SpeechSession Custom Vocabulary")
struct SpeechSessionCustomVocabularyTests {

    @Test("Custom vocabulary cache key is stable")
    func customVocabularyCacheKeyStable() throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "en_US"),
            identifier: "test",
            version: "1",
            phrases: [
                .init(text: "AuralKit", count: 5),
                .init(text: "Dictation", count: 2)
            ],
            pronunciations: [
                .init(grapheme: "AuralKit", phonemes: ["ɔː", "r", "əl", "k", "ɪ", "t"])
            ],
            templates: [
                .init(
                    body: "<product> transcription",
                    count: 3,
                    classes: ["product": ["AuralKit", "AuralPlan"]]
                )
            ]
        )

        let first = try descriptor.stableCacheKey()
        let second = try descriptor.stableCacheKey()
        #expect(first == second)
    }

    @Test("Configuring custom vocabulary caches compilation result")
    @MainActor
    func configureCustomVocabularyCachesCompilation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let compiler = MockCustomVocabularyCompiler()
        let session = SpeechSession(locale: Locale(identifier: "en_US"))
        session.customVocabularyCompiler = compiler

        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "en_US"),
            identifier: "medical",
            version: "1"
        )

        try await session.configureCustomVocabulary(descriptor)
        let expectedCacheKey = try descriptor.stableCacheKey()
        let compileCount = await compiler.compileCallCount
        #expect(compileCount == 1)
        #expect(session.customVocabularyDescriptor == descriptor)
        #expect(session.customVocabularyCacheKey == expectedCacheKey)
        #expect(session.customVocabularyConfiguration != nil)

        try await session.configureCustomVocabulary(descriptor)
        let repeatCompileCount = await compiler.compileCallCount
        #expect(repeatCompileCount == 1)

        try await session.configureCustomVocabulary(nil)
        await compiler.cleanup()
    }

    @Test("Configuring custom vocabulary rejects mismatched locale")
    @MainActor
    func configureCustomVocabularyRejectsMismatchedLocale() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let compiler = MockCustomVocabularyCompiler()
        let session = SpeechSession(locale: Locale(identifier: "en_US"))
        session.customVocabularyCompiler = compiler

        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "fr_FR"),
            identifier: "test",
            version: "1"
        )

        do {
            try await session.configureCustomVocabulary(descriptor)
            Issue.record("Expected customVocabularyUnsupportedLocale error")
        } catch SpeechSessionError.customVocabularyUnsupportedLocale {
            // expected path
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        await compiler.cleanup()
    }

    @Test("Configuring custom vocabulary requires idle session")
    @MainActor
    func configureCustomVocabularyRequiresIdleSession() async {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let compiler = MockCustomVocabularyCompiler()
        let session = SpeechSession(locale: Locale(identifier: "en_US"))
        session.customVocabularyCompiler = compiler

        session.streamingMode = .liveMicrophone

        let descriptor = SpeechSession.CustomVocabulary(
            locale: Locale(identifier: "en_US"),
            identifier: "test",
            version: "1"
        )

        do {
            try await session.configureCustomVocabulary(descriptor)
            Issue.record("Expected customVocabularyRequiresIdleSession error")
        } catch SpeechSessionError.customVocabularyRequiresIdleSession {
            // expected path
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        session.streamingMode = .inactive
        await compiler.cleanup()
    }
}

@available(iOS 26.0, macOS 26.0, *)
@available(iOS 26.0, macOS 26.0, *)
actor MockCustomVocabularyCompiler: CustomVocabularyCompiling {

    private let baseDirectory: URL
    private let fileManager: FileManager
    private(set) var compileCallCount = 0
    private(set) var lastDescriptor: SpeechSession.CustomVocabulary?

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
        baseDirectory = fileManager.temporaryDirectory
            .appendingPathComponent("mock-vocabulary-\(UUID().uuidString)", isDirectory: true)
    }

    func compile(descriptor: SpeechSession.CustomVocabulary) async throws -> CustomVocabularyCompilation {
        compileCallCount += 1
        lastDescriptor = descriptor
        let cacheKey = try descriptor.stableCacheKey()

        let outputDirectory = baseDirectory.appendingPathComponent(cacheKey, isDirectory: true)
        try? fileManager.createDirectory(at: outputDirectory, withIntermediateDirectories: true)

        let configuration = SFSpeechLanguageModel.Configuration(
            languageModel: outputDirectory.appendingPathComponent("languageModel.bin"),
            vocabulary: outputDirectory.appendingPathComponent("vocabulary.bin")
        )

        return CustomVocabularyCompilation(
            configuration: configuration,
            cacheKey: cacheKey,
            outputDirectory: outputDirectory
        )
    }

    func cleanup() async {
        try? fileManager.removeItem(at: baseDirectory)
    }
}

private actor SpeechSessionLoggingLock {
    static let shared = SpeechSessionLoggingLock()

    func withLock(_ body: @MainActor @Sendable () throws -> Void) async rethrows {
        try await body()
    }
}
