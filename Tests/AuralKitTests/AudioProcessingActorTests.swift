import Testing
import AVFoundation
@testable import AuralKit

@Suite("Audio processing input formats")
@MainActor
struct AudioProcessingActorTests {
    @Test("a reused processor accepts a new input sample rate", arguments: [44_100.0, 22_050.0])
    func inputSampleRateChanges(sampleRate: Double) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let first = try makeBuffer(sampleRate: 48_000, channels: 1)
        _ = try await processor.makeAnalyzerInput(from: .init(buffer: first), analyzerFormat: output)

        let next = try makeBuffer(sampleRate: sampleRate, channels: 1, phase: 19)
        let actual = try await processor.makeAnalyzerInput(from: .init(buffer: next), analyzerFormat: output)
        let expected = try BufferConverter().convertBuffer(next, to: output)
        try expectSameAudio(actual.buffer, expected)
    }

    @Test("a reused processor accepts a new channel layout at the same sample rate")
    func inputChannelCountChanges() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let first = try makeBuffer(sampleRate: 48_000, channels: 1)
        _ = try await processor.makeAnalyzerInput(from: .init(buffer: first), analyzerFormat: output)

        let stereo = try makeBuffer(sampleRate: 48_000, channels: 2, phase: 31)
        let actual = try await processor.makeAnalyzerInput(from: .init(buffer: stereo), analyzerFormat: output)
        let expected = try BufferConverter().convertBuffer(stereo, to: output)
        try expectSameAudio(actual.buffer, expected)
    }

    @Test("unchanged input formats preserve converter continuity between buffers")
    func unchangedFormatPreservesContinuity() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let reference = BufferConverter()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        for phase in [0, 29, 71] {
            let buffer = try makeBuffer(sampleRate: 44_100, channels: 1, phase: phase)
            let expected = try reference.convertBuffer(buffer, to: output)
            let actual = try await processor.makeAnalyzerInput(from: .init(buffer: buffer), analyzerFormat: output)
            try expectSameAudio(actual.buffer, expected)
        }
    }

    @Test("passthrough input does not reconnect an earlier resampler's state")
    func passthroughRetiresPreviousConversion() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let first = try makeBuffer(sampleRate: 44_100, channels: 1)
        _ = try await processor.makeAnalyzerInput(from: .init(buffer: first), analyzerFormat: output)

        let passthrough = try makeBuffer(sampleRate: 16_000, channels: 1, phase: 17)
        let passed = try await processor.makeAnalyzerInput(from: .init(buffer: passthrough), analyzerFormat: output)
        try expectSameAudio(passed.buffer, passthrough)

        let next = try makeBuffer(sampleRate: 44_100, channels: 1, phase: 67)
        let actual = try await processor.makeAnalyzerInput(from: .init(buffer: next), analyzerFormat: output)
        let expected = try BufferConverter().convertBuffer(next, to: output)
        try expectSameAudio(actual.buffer, expected)
    }

    @Test("destination format changes remain supported")
    func outputFormatChanges() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let buffer = try makeBuffer(sampleRate: 48_000, channels: 1)
        for sampleRate in [16_000.0, 22_050.0, 48_000.0] {
            let output = try #require(AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: 1))
            let actual = try await processor.makeAnalyzerInput(from: .init(buffer: buffer), analyzerFormat: output)
            let expected = try BufferConverter().convertBuffer(buffer, to: output)
            try expectSameAudio(actual.buffer, expected)
        }
    }

    private func makeBuffer(
        sampleRate: Double, channels: AVAudioChannelCount, phase: Int = 0
    ) throws -> AVAudioPCMBuffer {
        let format = try #require(AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: channels))
        let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 4_096))
        buffer.frameLength = buffer.frameCapacity
        let data = try #require(buffer.floatChannelData)
        for channel in 0..<Int(channels) {
            for frame in 0..<Int(buffer.frameLength) {
                data[channel][frame] = Float((frame + phase + channel * 53) % 257) / 512 - 0.25
            }
        }
        return buffer
    }

    private func expectSameAudio(_ actual: AVAudioPCMBuffer, _ expected: AVAudioPCMBuffer) throws {
        #expect(actual.format == expected.format)
        try #require(actual.frameLength == expected.frameLength)
        try #require(actual.format.channelCount == expected.format.channelCount)
        #expect(actual.frameLength > 0)
        let actualData = try #require(actual.floatChannelData)
        let expectedData = try #require(expected.floatChannelData)
        for channel in 0..<Int(expected.format.channelCount) {
            let count = Int(expected.frameLength)
            let actualSamples = UnsafeBufferPointer(start: actualData[channel], count: count)
            let expectedSamples = UnsafeBufferPointer(start: expectedData[channel], count: count)
            #expect(zip(actualSamples, expectedSamples).allSatisfy { abs($0 - $1) < 0.000_001 })
        }
    }
}
