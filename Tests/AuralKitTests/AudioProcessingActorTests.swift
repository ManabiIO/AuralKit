import Testing
import Foundation
import AVFoundation
import Speech
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
        _ = try await processor.convertBuffer(from: .init(buffer: first), analyzerFormat: output)

        let next = try makeBuffer(sampleRate: sampleRate, channels: 1, phase: 19)
        let actual = try await processor.convertBuffer(from: .init(buffer: next), analyzerFormat: output)
        let expected = try BufferConverter().convertBuffer(next, to: output)
        try expectSameAudio(actual.buffer, expected)
    }

    @Test("a reused processor accepts a new channel layout at the same sample rate")
    func inputChannelCountChanges() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let first = try makeBuffer(sampleRate: 48_000, channels: 1)
        _ = try await processor.convertBuffer(from: .init(buffer: first), analyzerFormat: output)

        let stereo = try makeBuffer(sampleRate: 48_000, channels: 2, phase: 31)
        let actual = try await processor.convertBuffer(from: .init(buffer: stereo), analyzerFormat: output)
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
            let actual = try await processor.convertBuffer(from: .init(buffer: buffer), analyzerFormat: output)
            try expectSameAudio(actual.buffer, expected)
        }
    }

    @Test("passthrough input does not reconnect an earlier resampler's state")
    func passthroughRetiresPreviousConversion() async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let processor = AudioProcessingActor()
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let first = try makeBuffer(sampleRate: 44_100, channels: 1)
        _ = try await processor.convertBuffer(from: .init(buffer: first), analyzerFormat: output)

        let passthrough = try makeBuffer(sampleRate: 16_000, channels: 1, phase: 17)
        let passed = try await processor.convertBuffer(from: .init(buffer: passthrough), analyzerFormat: output)
        try expectSameAudio(passed.buffer, passthrough)

        let next = try makeBuffer(sampleRate: 44_100, channels: 1, phase: 67)
        let actual = try await processor.convertBuffer(from: .init(buffer: next), analyzerFormat: output)
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
            let actual = try await processor.convertBuffer(from: .init(buffer: buffer), analyzerFormat: output)
            let expected = try BufferConverter().convertBuffer(buffer, to: output)
            try expectSameAudio(actual.buffer, expected)
        }
    }

    @Test("the production consumer wraps and retains audio in a Speech-selected format", arguments: [false, true])
    func consumerWrapsSupportedAudio(live: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let transcriber = SpeechTranscriber(
            locale: Locale(identifier: "en-US"),
            transcriptionOptions: [], reportingOptions: [], attributeOptions: []
        )
        // Match production selection. No guessed PCM fallback or asset download:
        // unavailable assets must leave this native qualification visibly failing.
        let output = try #require(
            await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith: [transcriber]),
            "Supported Speech assets for en-US are required to qualify the real AnalyzerInput boundary"
        )
        let session = SpeechSession(locale: Locale(identifier: "en-US"))
        let (stream, continuation) = AsyncStream<AnalyzerInput>.makeStream()
        let run = LiveTranscriptionRun()
        session.liveTranscription = live ? run : nil
        session.analyzerFormat = output
        session.inputBuilder = continuation
        defer {
            continuation.finish()
            session.inputBuilder = nil
            session.analyzerFormat = nil
            session.liveTranscription = nil
        }

        let reference = BufferConverter()
        var expected: [AVAudioPCMBuffer] = []
        for phase in [19, 71] {
            // Each source leaves scope before the stream is consumed. A subsequent
            // conversion must not reuse or overwrite an earlier input's storage.
            let source = try makeBuffer(sampleRate: 48_000, channels: 1, phase: phase)
            let referenceBuffer = try reference.convertBuffer(source, to: output)
            expected.append(try #require(referenceBuffer.copy() as? AVAudioPCMBuffer))
            try await session.processAudioBuffer(.init(buffer: source), liveRun: live ? run : nil)
        }
        continuation.finish()
        var iterator = stream.makeAsyncIterator()
        for buffer in expected {
            let input = try #require(await iterator.next())
            try expectSamePCM(input.buffer, buffer)
        }
        #expect(await iterator.next() == nil)
    }

    @Test("retired and replaced sources cannot reach the Speech consumer", arguments: [false, true])
    func consumerRejectsObsoleteRun(replaced: Bool) async throws {
        guard #available(iOS 26.0, macOS 26.0, *) else { return }
        let session = SpeechSession()
        let run = LiveTranscriptionRun()
        run.retired = !replaced
        session.liveTranscription = replaced ? LiveTranscriptionRun() : run
        let (stream, continuation) = AsyncStream<AnalyzerInput>.makeStream()
        session.inputBuilder = continuation
        session.analyzerFormat = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        defer {
            continuation.finish()
            session.inputBuilder = nil
            session.analyzerFormat = nil
            session.liveTranscription = nil
        }
        let source = try makeBuffer(sampleRate: 48_000, channels: 1)
        do {
            try await session.processAudioBuffer(.init(buffer: source), liveRun: run)
            Issue.record("An obsolete source must be rejected before conversion or Speech wrapping")
        } catch is CancellationError {
            // Expected: the consumer stream receives no audio from the obsolete run.
        }
        continuation.finish()
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == nil)
    }

    private func expectSamePCM(_ actual: AVAudioPCMBuffer, _ expected: AVAudioPCMBuffer) throws {
        #expect(actual.format == expected.format)
        try #require(actual.frameLength == expected.frameLength)
        #expect(actual.frameLength > 0)
        let actualBuffers = UnsafeMutableAudioBufferListPointer(actual.mutableAudioBufferList)
        let expectedBuffers = UnsafeMutableAudioBufferListPointer(expected.mutableAudioBufferList)
        try #require(actualBuffers.count == expectedBuffers.count)
        for (actualBuffer, expectedBuffer) in zip(actualBuffers, expectedBuffers) {
            #expect(actualBuffer.mNumberChannels == expectedBuffer.mNumberChannels)
            try #require(actualBuffer.mDataByteSize == expectedBuffer.mDataByteSize)
            let actualData = try #require(actualBuffer.mData)
            let expectedData = try #require(expectedBuffer.mData)
            #expect(
                Data(bytes: actualData, count: Int(actualBuffer.mDataByteSize)) ==
                Data(bytes: expectedData, count: Int(expectedBuffer.mDataByteSize))
            )
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
