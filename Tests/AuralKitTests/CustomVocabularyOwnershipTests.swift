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

    @Test("Outstanding standalone compilation keeps transcription admission closed")
    @MainActor
    func suspendedConfigurationKeepsStartClosed() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFixture { fixture in
            let task = fixture.configure("old")
            try await fixture.waitForCompilation("old")
            let rejected = fixture.session.startDictationTranscribing()
            await #expect(throws: SpeechSessionError.self) {
                for try await _ in rejected { }
            }
            #expect(!fixture.authorizationEntered)
            await fixture.compiler.release("old")
            await task.value
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.customVocabularyDescriptor == fixture.descriptor("old"))
            #expect(fixture.session.canStartTranscription)
            fixture.startPlainDictation()
            try await fixture.waitUntil { fixture.authorizationEntered }
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

    @MainActor
    @available(iOS 26.0, macOS 26.0, *)
    func withFixture(_ operation: @MainActor (VocabularyFixture) async throws -> Void) async throws {
        let fixture = VocabularyFixture()
        do { try await operation(fixture) } catch { await fixture.cleanup(); throw error }
        await fixture.cleanup()
    }
}
