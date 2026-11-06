import Foundation
import AVFoundation

@available(iOS 26.0, macOS 26.0, *)
actor AudioProcessingActor {
    private var converter = BufferConverter()
    private var inputFormat: AVAudioFormat?

    func convertBuffer(
        from buffer: SpeechSession.SendablePCMBuffer,
        analyzerFormat: AVAudioFormat
    ) throws -> SpeechSession.SendablePCMBuffer {
        // Keep resampling state only while the input format is continuous.
        // Upstream's converter keys both formats, but passthrough returns before
        // updating that cache. Reset here across passthrough transitions too.
        if inputFormat != buffer.buffer.format {
            converter = BufferConverter()
            inputFormat = buffer.buffer.format
        }
        let converted = try converter.convertBuffer(buffer.buffer, to: analyzerFormat)
        // Conversion creates a new buffer, or passes through the already-owned input.
        // Neither the converter nor the caller may mutate it after this handoff.
        return SpeechSession.SendablePCMBuffer(buffer: converted)
    }
}
