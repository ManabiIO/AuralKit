import Foundation
import Speech
import Testing
@testable import AuralKit

private enum VocabularyStartupTermination: CaseIterable, Sendable {
    case caller, finish, cancel
}

@Suite("Custom vocabulary startup ownership")
struct CustomVocabularyLifecycleTests {
    @Test("Termination joins vocabulary compilation and never starts late capture",
          arguments: VocabularyStartupTermination.allCases)
    @MainActor
    fileprivate func terminationDuringCompilation(_ termination: VocabularyStartupTermination) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.customVocabularyCompiler = GatedVocabularyCompiler(gate: gate)
            let vocabulary = descriptor(for: fixture.session, version: "new")
            let previous = descriptor(for: fixture.session, version: "previous")
            fixture.session.customVocabularyDescriptor = previous
            var preparations = 0
            fixture.session.liveTranscriptionHooks?.preparePipeline = { _ in preparations += 1 }
            var returned = false
            let startup = fixture.task {
                do {
                    _ = try await fixture.session.startTranscribing(customVocabulary: vocabulary)
                    Issue.record("Cancelled vocabulary startup must throw")
                } catch is CancellationError {} catch { Issue.record(error) }
                returned = true
            }
            try await fixture.waitUntil { gate.entered }
            #expect(fixture.session.status == .preparing)
            #expect(!fixture.session.canStartTranscription)
            await #expect(throws: SpeechSessionError.self) {
                try await fixture.session.configureCustomVocabulary(nil)
            }

            let stop = fixture.task {
                switch termination {
                case .caller: startup.cancel()
                case .finish: await fixture.session.finishTranscribing()
                case .cancel: await fixture.session.cancelTranscribing()
                }
            }
            try await fixture.waitUntil { fixture.session.status == .stopping }
            #expect(!returned)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await startup.value
            await stop.value
            #expect(returned)
            #expect(preparations == 0)
            #expect(fixture.session.customVocabularyDescriptor == previous)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Vocabulary convenience publishes compiled configuration before returning a ready stream")
    @MainActor
    func successfulCompilation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.customVocabularyCompiler = GatedVocabularyCompiler(gate: gate)
            let vocabulary = descriptor(for: fixture.session, version: "ready")
            var returned = false
            let startup = fixture.task {
                do {
                    let stream = try await fixture.session.startTranscribing(customVocabulary: vocabulary)
                    fixture.consume(stream)
                    #expect(fixture.session.status == .transcribing)
                    #expect(fixture.session.customVocabularyDescriptor == vocabulary)
                    #expect(fixture.session.customVocabularyConfiguration != nil)
                    returned = true
                } catch { Issue.record(error) }
            }
            try await fixture.waitUntil { gate.entered }
            #expect(!returned)
            gate.open()
            await startup.value
            #expect(returned)
            await fixture.session.finishTranscribing()
        }
    }

    @Test("Standalone configuration excludes competing starts and abandons cancelled compilation")
    @MainActor
    func configurationAdmissionAndCancellation() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.customVocabularyCompiler = GatedVocabularyCompiler(gate: gate)
            let previous = descriptor(for: fixture.session, version: "previous")
            fixture.session.customVocabularyDescriptor = previous
            let vocabulary = descriptor(for: fixture.session, version: "abandoned")
            var returned = false
            let configure = fixture.task {
                do {
                    try await fixture.session.configureCustomVocabulary(vocabulary)
                    Issue.record("Cancelled configuration must throw")
                } catch is CancellationError {} catch { Issue.record(error) }
                returned = true
            }
            try await fixture.waitUntil { gate.entered }
            let rejected = fixture.session.startTranscribing()
            await #expect(throws: SpeechSessionError.self) {
                for try await _ in rejected {}
            }
            await #expect(throws: SpeechSessionError.self) {
                try await fixture.session.configureCustomVocabulary(nil)
            }
            configure.cancel()
            #expect(!returned)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await configure.value
            #expect(fixture.session.customVocabularyDescriptor == previous)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Configuration cannot replace assets while live setup is preparing")
    @MainActor
    func configurationDuringLiveStartup() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.authorize = { await gate.wait() }
            let previous = descriptor(for: fixture.session, version: "previous")
            fixture.session.customVocabularyDescriptor = previous
            fixture.start()
            try await fixture.waitUntil { gate.entered }
            #expect(fixture.session.streamingMode == .inactive)
            await #expect(throws: SpeechSessionError.self) {
                try await fixture.session.configureCustomVocabulary(nil)
            }
            #expect(fixture.session.customVocabularyDescriptor == previous)
            gate.open()
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    @MainActor
    private func descriptor(for session: SpeechSession, version: String) -> SpeechSession.CustomVocabulary {
        .init(locale: session.locale, identifier: "lifecycle-test", version: version)
    }
}

@available(iOS 26.0, macOS 26.0, *)
private struct GatedVocabularyCompiler: CustomVocabularyCompiling {
    let gate: FileLifecycleGate

    func compile(descriptor: SpeechSession.CustomVocabulary) async throws -> CustomVocabularyCompilation {
        // Deliberately ignore cancellation to model Apple's uncancellable work.
        await gate.wait()
        let output = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        return CustomVocabularyCompilation(
            configuration: SFSpeechLanguageModel.Configuration(
                languageModel: output.appendingPathComponent("languageModel.bin"),
                vocabulary: output.appendingPathComponent("vocabulary.bin")
            ),
            cacheKey: try descriptor.stableCacheKey(),
            outputDirectory: output
        )
    }
}
