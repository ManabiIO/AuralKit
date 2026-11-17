import AVFoundation
import Foundation
import Speech
import Testing
@testable import AuralKit

@Suite("Microphone input ownership")
@MainActor
struct MicrophoneAudioIngressTests {
    @Test("Borrowed tap buffers retain their samples and order after reuse")
    func borrowedBuffersAreCopied() throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let format = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let output = PCMOutputProbe()
        let ingress = MicrophoneAudioIngress(
            format: format, deliver: { output.append($0) }, onError: { output.fail($0) }
        )
        defer { ingress.close() }
        let buffer = try pcm(format: format, frames: 160, value: 0.25)
        ingress.receive(buffer)
        buffer.floatChannelData?.pointee.initialize(repeating: 0.5, count: 160)
        ingress.receive(buffer)
        buffer.floatChannelData?.pointee.initialize(repeating: -1, count: 160)
        #expect(output.values == [0.25, 0.5])
        #expect(!output.hasErrors)
        ingress.close()
        ingress.receive(buffer)
        #expect(output.values == [0.25, 0.5])
    }

    @Test("Conversion delivers the captured analyzer format", arguments: [44_100.0, 48_000.0])
    func convertsNativePCM(rate: Double) throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let source = try #require(AVAudioFormat(standardFormatWithSampleRate: rate, channels: 1))
        let target = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let output = PCMOutputProbe()
        let ingress = MicrophoneAudioIngress(
            format: target, deliver: { output.append($0) }, onError: { output.fail($0) }
        )
        defer { ingress.close() }
        ingress.receive(try pcm(format: source, frames: 4_096, value: 0.25))
        let delivered = try #require(output.buffers.first)
        #expect(output.buffers.count == 1)
        #expect(delivered.format == target)
        #expect(delivered.frameLength > 0)
        #expect(!output.hasErrors)
    }

    @Test("Public Finish drains admitted input and closes its producer")
    func finishDrainsAcceptedPCM() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        let inputs = AsyncStream<AnalyzerInput>.makeStream()
        let results = AsyncThrowingStream<SpeechTranscriber.Result, Error>.makeStream()
        let format = try #require(AVAudioFormat(
            commonFormat: .pcmFormatInt16, sampleRate: 16_000, channels: 1, interleaved: true
        ))
        let ingress = MicrophoneAudioIngress(
            format: format, deliver: { inputs.continuation.yield(AnalyzerInput(buffer: $0)) },
            onError: { _ in Issue.record("Unexpected microphone conversion failure") }
        )
        session.liveTranscription = run
        run.microphoneInput = ingress
        session.inputBuilder = inputs.continuation
        session.continuation = .speech(results.continuation)
        session.streamingMode = .liveMicrophone
        session.setStatus(.transcribing)
        var received = 0
        run.recognizerTask = Task { @MainActor in
            for await _ in inputs.stream { received += 1 }
            session.retireLiveTranscription(run, error: nil)
        }
        session.recognizerTask = run.recognizerTask
        let buffer = try pcm(format: format, frames: 160, value: 0.25)
        ingress.receive(buffer)
        await session.finishTranscribing()
        var iterator = results.stream.makeAsyncIterator()
        #expect(try await iterator.next() == nil)
        #expect(received == 1)
        #expect(session.liveTranscription == nil)
        ingress.receive(buffer)
        #expect(received == 1)
    }

    private func pcm(format: AVAudioFormat, frames: AVAudioFrameCount, value: Float) throws -> AVAudioPCMBuffer {
        let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames))
        buffer.frameLength = frames
        buffer.floatChannelData?.pointee.initialize(repeating: value, count: Int(frames))
        buffer.int16ChannelData?.pointee.initialize(repeating: Int16(value * 32_767), count: Int(frames))
        return buffer
    }
}

private final class PCMOutputProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [AVAudioPCMBuffer] = []
    private var errors: [Error] = []

    func append(_ buffer: AVAudioPCMBuffer) { lock.withLock { storage.append(buffer) } }
    func fail(_ error: Error) { lock.withLock { errors.append(error) } }
    var buffers: [AVAudioPCMBuffer] { lock.withLock { storage } }
    var values: [Float] { lock.withLock { storage.map { $0.floatChannelData?.pointee[0] ?? .nan } } }
    var hasErrors: Bool { lock.withLock { !errors.isEmpty } }
}
