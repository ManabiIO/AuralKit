import Testing
import AVFoundation
@testable import AuralKit

extension BufferConverterTests {
    @Test("Input format changes behave like a fresh audio converter")
    func converterRecreatesForChangedInputFormat() throws {
        func input(rate: Double) throws -> AVAudioPCMBuffer {
            let format = try #require(AVAudioFormat(standardFormatWithSampleRate: rate, channels: 1))
            let buffer = try #require(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 1_024))
            buffer.frameLength = 1_024
            let samples = try #require(buffer.floatChannelData?[0])
            for frame in 0..<Int(buffer.frameLength) { samples[frame] = 0.25 }
            return buffer
        }
        let output = try #require(AVAudioFormat(standardFormatWithSampleRate: 16_000, channels: 1))
        let reused = BufferConverter()
        _ = try reused.convertBuffer(input(rate: 44_100), to: output)
        let changed = try reused.convertBuffer(input(rate: 48_000), to: output)
        let fresh = try BufferConverter().convertBuffer(input(rate: 48_000), to: output)
        #expect(changed.format == output)
        #expect(changed.frameLength > 0)
        #expect(changed.frameLength == fresh.frameLength)
    }
}
