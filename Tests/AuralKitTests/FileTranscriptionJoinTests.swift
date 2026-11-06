import Testing
import AVFoundation
@testable import AuralKit

extension FileTranscriptionTests {
    @Test("stop joins suspended file authorization and prevents late pipeline startup")
    @MainActor
    func stopJoinsSuspendedFileAuthorization() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let authorization = fixture.gate()
            var pipelineStarts = 0
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await authorization.wait() },
                preparePipeline: { _ in pipelineStarts += 1 },
                ingest: { Issue.record("Retired setup must not ingest audio"); return true }
            )
            let consumer = fixture.transcribe()
            try await fixture.waitUntil { authorization.entered }
            let run = try #require(fixture.session.fileTranscription)
            consumer.cancel()
            await consumer.value
            try await fixture.waitUntil { run.retired }
            var stopEntered = false
            var stopCompleted = false
            let stop = fixture.task {
                stopEntered = true
                await fixture.session.stopTranscribing()
                stopCompleted = true
            }
            try await fixture.waitUntil { stopEntered }
            #expect(!stopCompleted)
            #expect(fixture.session.fileTranscription === run)
            authorization.open()
            await stop.value
            await consumer.value
            #expect(stopCompleted)
            #expect(pipelineStarts == 0)
            #expect(fixture.session.fileTranscription == nil)
            #expect(run.setupTask == nil)
            #expect(fixture.session.status == .idle)
        }
    }

    @Test("stop joins late child admitted by suspended analyzer setup")
    @MainActor
    func stopJoinsLatePipelineChild() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let preparation = fixture.gate()
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    await preparation.wait()
                    // Model/analyzer setup may ignore cancellation and admit a
                    // result task before returning to the outer owner check.
                    run.recognizerTask = Task { @MainActor in await results.wait() }
                },
                ingest: { Issue.record("Retired preparation must not ingest audio"); return true }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { preparation.entered }
            let run = try #require(fixture.session.fileTranscription)
            var stopCompleted = false
            let stop = fixture.task { await fixture.session.stopTranscribing(); stopCompleted = true }
            try await fixture.waitUntil { run.retired }
            #expect(!stopCompleted)
            preparation.open()
            try await fixture.waitUntil { results.entered }
            try await fixture.waitUntil { run.recognizerTask?.isCancelled == true }
            #expect(!stopCompleted)
            results.open()
            await stop.value
            await consumer.value
            #expect(stopCompleted)
            #expect(run.recognizerTask == nil)
            #expect(fixture.session.fileTranscription == nil)
        }
    }

    @Test("concurrent stops join file ingestion and recognizer before returning")
    @MainActor
    func concurrentStopsJoinFileProducers() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let ingestion = fixture.gate()
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = Task { @MainActor in
                        await results.wait()
                        fixture.session.retireFileTranscription(run, error: nil)
                    }
                },
                ingest: {
                    await ingestion.wait()
                    await MainActor.run { fixture.ingestionReturned = true }
                    return true
                }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { ingestion.entered && results.entered }
            let run = try #require(fixture.session.fileTranscription)
            var completedStops = 0
            let first = fixture.task { await fixture.session.stopTranscribing(); completedStops += 1 }
            let second = fixture.task { await fixture.session.stopTranscribing(); completedStops += 1 }
            try await fixture.waitUntil { run.retired }
            #expect(completedStops == 0)
            ingestion.open()
            try await fixture.waitUntil { fixture.ingestionReturned }
            #expect(completedStops == 0)
            #expect(fixture.session.status == .stopping)
            #expect(fixture.session.fileTranscription === run)
            results.open()
            await first.value
            await second.value
            await consumer.value
            #expect(completedStops == 2)
            #expect(run.ingestionTask == nil)
            #expect(run.recognizerTask == nil)
            #expect(fixture.session.fileTranscription == nil)
        }
    }

    @Test("recognizer normal finish retires without joining itself and permits retry")
    @MainActor
    func normalFileFinishAndRetryRejectOldRetirement() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let results = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {},
                preparePipeline: { run in
                    run.recognizerTask = Task { @MainActor in
                        await results.wait()
                        fixture.session.retireFileTranscription(run, error: nil)
                    }
                },
                ingest: { await MainActor.run { fixture.ingestionReturned = true }; return true }
            )
            let first = fixture.consume()
            try await fixture.waitUntil { fixture.ingestionReturned && results.entered }
            let oldRun = try #require(fixture.session.fileTranscription)
            results.open()
            await first.value
            await fixture.session.stopTranscribing()
            #expect(fixture.errors.isEmpty)
            #expect(fixture.session.fileTranscription == nil)

            let retryAuthorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await retryAuthorization.wait() },
                preparePipeline: { _ in Issue.record("Stopped retry must not start a pipeline") }
            )
            let retry = fixture.consume()
            try await fixture.waitUntil { retryAuthorization.entered }
            let retryRun = try #require(fixture.session.fileTranscription)
            #expect(retryRun !== oldRun)
            #expect(fixture.session.retireFileTranscription(oldRun, error: nil) == nil)
            #expect(!retryRun.retired)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { retryRun.retired }
            retryAuthorization.open()
            await stop.value
            await retry.value
        }
    }

    @Test("file producer error completes cleanup without awaiting its own task")
    @MainActor
    func fileIngestionFailureDoesNotSelfJoin() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: {}, preparePipeline: { _ in },
                ingest: { throw FileLifecycleProbeError.ingestion }
            )
            let consumer = fixture.consume()
            await consumer.value
            await fixture.session.stopTranscribing()
            #expect(fixture.errors.count == 1)
            #expect(fixture.errors.first as? FileLifecycleProbeError == .ingestion)
            #expect(fixture.session.fileTranscription == nil)
            #expect(fixture.session.status == .idle)
        }
    }

}
