import Testing
import AVFoundation
@testable import AuralKit

@Suite("File transcription")
struct FileTranscriptionTests {

    @Test("rejected file and microphone streams cannot retire an admitted file run")
    @MainActor
    func rejectedStreamsPreserveAdmittedFileRun() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        try await withFileLifecycleFixture { fixture in
            let authorization = fixture.gate()
            fixture.session.fileTranscriptionHooks = FileTranscriptionHooks(
                authorize: { await authorization.wait() }, preparePipeline: { _ in }
            )
            let consumer = fixture.consume()
            try await fixture.waitUntil { authorization.entered }
            let run = try #require(fixture.session.fileTranscription)
            var rejected = [
                fixture.session.streamTranscription(from: fixture.audioURL),
                fixture.session.startTranscribing()
            ]
#if canImport(ScreenCaptureKit)
            if #available(iOS 27.0, macOS 26.0, *) {
                rejected.append(fixture.session.startTranscribingScreenCapture())
            }
#endif
            for stream in rejected {
                await #expect(throws: SpeechSessionError.self) {
                    for try await _ in stream {}
                }
            }
            let dictation = fixture.session.startDictationTranscribing()
            await #expect(throws: SpeechSessionError.self) {
                for try await _ in dictation {}
            }
            // Let queued termination callbacks from all rejected streams run.
            for _ in 0..<32 { await Task.yield() }
            #expect(fixture.session.fileTranscription === run)
            #expect(!run.retired)
            let stop = fixture.task { await fixture.session.stopTranscribing() }
            try await fixture.waitUntil { run.retired }
            authorization.open()
            await stop.value
            await consumer.value
        }
    }

    @available(iOS 26.0, macOS 26.0, *)
    @MainActor
    func withFileLifecycleFixture(
        _ operation: @MainActor (FileLifecycleFixture) async throws -> Void
    ) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let fixture = FileLifecycleFixture(audioURL: try createSilenceAudioFile(duration: 0.05))
        do {
            try await operation(fixture)
        } catch {
            await fixture.cleanup()
            throw error
        }
        await fixture.cleanup()
    }

    @Test("transcribe throws audioFileNotFound for missing file")
    @MainActor
    func transcribeMissingFileThrowsNotFound() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let missingURL = URL(fileURLWithPath: "/tmp/not-real-")

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: missingURL)
        }
    }

    @Test("transcribe rejects audio exceeding maximum duration")
    @MainActor
    func transcribeRejectsAudioExceedingMaximumDuration() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let tempURL = try createSilenceAudioFile(duration: 2.0)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let session = SpeechSession()
        let options = FileTranscriptionOptions(maxDuration: 1.0)

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: tempURL, options: options)
        }
    }

    private func createSilenceAudioFile(duration: TimeInterval, sampleRate: Double = 44_100) throws -> URL {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension("caf")

        guard let format = AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: 1) else {
            throw NSError(
                domain: "FileTranscriptionTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Failed to create audio format"]
            )
        }

        let frameCount = AVAudioFrameCount(duration * sampleRate)
        guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frameCount) else {
            throw NSError(
                domain: "FileTranscriptionTests",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "Failed to create buffer"]
            )
        }

        buffer.frameLength = frameCount
        if let channelData = buffer.floatChannelData {
            for channel in 0..<Int(buffer.format.channelCount) {
                channelData[channel].initialize(repeating: 0, count: Int(buffer.frameLength))
            }
        }

        let audioFile = try AVAudioFile(forWriting: tempURL, settings: format.settings)
        try audioFile.write(from: buffer)

        return tempURL
    }

    @Test("transcribe throws audioFileInvalidURL for non-file URL")
    @MainActor
    func transcribeNonFileURLThrowsInvalidURL() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let httpURL = URL(string: "https://example.com/audio.wav")!

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: httpURL)
        }
    }

    @Test("transcribe throws audioFileOutsideAllowedDirectories for restricted path")
    @MainActor
    func transcribeOutsideAllowedDirectoriesThrows() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let tempURL = try createSilenceAudioFile(duration: 0.5)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let session = SpeechSession()
        let restrictedOptions = FileTranscriptionOptions(
            allowedDirectories: [URL(fileURLWithPath: "/nonexistent/path")]
        )

        await #expect(throws: SpeechSessionError.self) {
            _ = try await session.transcribe(audioFile: tempURL, options: restrictedOptions)
        }
    }
}

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
