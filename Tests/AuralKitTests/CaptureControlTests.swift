import Foundation
import Testing
@testable import AuralKit

private enum CaptureControlProbeError: Error, Equatable { case resume }

@Suite("Live capture control ownership")
struct CaptureControlTests {
    @Test("Concurrent capture controls join one native operation", arguments: [false, true])
    @MainActor
    func concurrentControls(resume: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.streamingMode = .screenCapture
            let gate = fixture.gate()
            var operations = 0
            fixture.session.liveTranscriptionHooks?.pauseCapture = {
                operations += 1
                await gate.wait()
            }
            fixture.session.liveTranscriptionHooks?.resumeCapture = {
                operations += 1
                // ScreenCaptureKit's provider rejects an overlapping restart.
                guard operations == 1 else { throw SpeechSessionError.recognitionStreamSetupFailed }
                await gate.wait()
            }
            if resume { fixture.session.setStatus(.paused) }
            var completed = 0
            let first = fixture.task {
                do {
                    try await performCaptureControl(fixture, resume: resume)
                    completed += 1
                } catch { Issue.record(error) }
            }
            try await fixture.waitUntil { gate.entered }
            var secondStarted = false
            let second = fixture.task {
                secondStarted = true
                do {
                    try await performCaptureControl(fixture, resume: resume)
                    completed += 1
                } catch { Issue.record(error) }
            }
            try await fixture.waitUntil { secondStarted }
            #expect(operations == 1)
            #expect(completed == 0)
            #expect(fixture.session.status == (resume ? .paused : .transcribing))
            gate.open()
            await first.value
            await second.value
            #expect(completed == 2)
            #expect(fixture.session.status == (resume ? .transcribing : .paused))
            #expect(fixture.session.liveTranscription?.captureControlTask == nil)
            #expect(fixture.cancellations == 0)
        }
    }

    @Test("Finish joins an interrupted resume without cancelling final results", arguments: [false, true])
    @MainActor
    func finishDuringResume(nativeError: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.streamingMode = .screenCapture
            fixture.session.setStatus(.paused)
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.resumeCapture = {
                await gate.wait()
                // A stopped provider can throw cancellation or a native capture error.
                if nativeError { throw CaptureControlProbeError.resume }
                throw CancellationError()
            }
            var resumeReturned = false
            let resume = fixture.task {
                do {
                    try await fixture.session.resumeTranscribing()
                    Issue.record("Interrupted native resume should throw")
                } catch is CancellationError {
                    #expect(!nativeError)
                } catch CaptureControlProbeError.resume {
                    #expect(nativeError)
                } catch { Issue.record(error) }
                resumeReturned = true
            }
            try await fixture.waitUntil { gate.entered }
            var finishReturned = false
            let finish = fixture.task {
                await fixture.session.finishTranscribing()
                finishReturned = true
            }
            try await fixture.waitUntil { fixture.session.status == .stopping }
            #expect(!finishReturned)
            #expect(!resumeReturned)
            #expect(fixture.finalizations == 0)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await finish.value
            await resume.value
            fixture.forwarded.continuation.finish()
            await fixture.forwardingConsumer?.value
            await fixture.publicConsumer?.value
            #expect(fixture.received == ["final tail"])
            #expect(fixture.errors.isEmpty)
            #expect(fixture.finalizations == 1)
            #expect(fixture.cancellations == 1)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Cancel joins a pending pause before admitting another run")
    @MainActor
    func cancelDuringPause() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.streamingMode = .screenCapture
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.pauseCapture = { await gate.wait() }
            let pause = fixture.task { await fixture.session.pauseTranscribing() }
            try await fixture.waitUntil { gate.entered }
            var returned = false
            let cancel = fixture.task {
                await fixture.session.cancelTranscribing()
                returned = true
            }
            try await fixture.waitUntil { fixture.session.status == .stopping }
            #expect(!returned)
            #expect(!fixture.session.canStartTranscription)
            gate.open()
            await cancel.value
            await pause.value
            #expect(returned)
            #expect(fixture.session.status == .idle)
            #expect(fixture.session.canStartTranscription)
        }
    }

    @Test("Resume errors retire the run before reaching the caller without a self-join")
    @MainActor
    func resumeFailure() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.streamingMode = .screenCapture
            fixture.session.setStatus(.paused)
            fixture.session.liveTranscriptionHooks?.resumeCapture = { throw CaptureControlProbeError.resume }
            await #expect(throws: CaptureControlProbeError.resume) {
                try await fixture.session.resumeTranscribing()
            }
            await fixture.publicConsumer?.value
            #expect(fixture.errors.first as? CaptureControlProbeError == .resume)
            #expect(fixture.cancellations == 1)
            #expect(fixture.session.status == .idle)
            #expect(fixture.session.canStartTranscription)
        }
    }
}

extension CaptureControlTests {
    @Test("An opposite control admitted after native publication joins its predecessor")
    @MainActor
    func controlAfterNativePublication() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.pauseCapture = {
                // Native screen transitions may publish before their observer returns.
                fixture.session.setStatus(.paused)
                await gate.wait()
            }
            var resumes = 0
            fixture.session.liveTranscriptionHooks?.resumeCapture = { resumes += 1 }
            let pause = fixture.task { await fixture.session.pauseTranscribing() }
            try await fixture.waitUntil { gate.entered }
            var returned = false
            let resume = fixture.task {
                do { try await fixture.session.resumeTranscribing() } catch { Issue.record(error) }
                returned = true
            }
            let run = try #require(fixture.session.liveTranscription)
            try await fixture.waitUntil { run.captureControlDestination == .transcribing }
            #expect(resumes == 0 && !returned)
            gate.open()
            await pause.value
            await resume.value
            #expect(returned && resumes == 1)
            #expect(fixture.session.status == .transcribing)
            #expect(run.captureControlTask == nil)
        }
    }
}

extension CaptureControlTests {
    @Test("A normally returning control still joins delegate retirement and reports its error")
    @MainActor
    func controlJoinsDelegateRetirement() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withLiveFixture { fixture in
            fixture.start()
            try await fixture.waitUntil { fixture.session.status == .transcribing }
            fixture.session.setStatus(.paused)
            let run = try #require(fixture.session.liveTranscription)
            let gate = fixture.gate()
            fixture.session.liveTranscriptionHooks?.cancelAnalyzer = {
                fixture.cancellations += 1
                await gate.wait()
                fixture.results.continuation.finish()
            }
            fixture.session.liveTranscriptionHooks?.resumeCapture = {
                fixture.session.retireLiveTranscription(run, error: CaptureControlProbeError.resume)
            }
            var returned = false
            let resume = fixture.task {
                await #expect(throws: CaptureControlProbeError.resume) {
                    try await fixture.session.resumeTranscribing()
                }
                returned = true
            }
            try await fixture.waitUntil { gate.entered }
            #expect(!returned && fixture.session.continuation != nil)
            gate.open()
            await resume.value
            await fixture.publicConsumer?.value
            #expect(returned && fixture.session.canStartTranscription)
            #expect(fixture.errors.first as? CaptureControlProbeError == .resume)
        }
    }
}

@available(iOS 26.0, macOS 26.0, *)
@MainActor
private func performCaptureControl(_ fixture: LiveLifecycleFixture, resume: Bool) async throws {
    if resume {
        try await fixture.session.resumeTranscribing()
    } else {
        await fixture.session.pauseTranscribing()
    }
}
