import Foundation
import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
actor AudioProcessingActor {
    private var converter = BufferConverter()
    private var inputFormat: AVAudioFormat?

    func makeAnalyzerInput(
        from buffer: SpeechSession.SendablePCMBuffer,
        analyzerFormat: AVAudioFormat
    ) throws -> AnalyzerInput {
        // The sample converter only keys its cache by output format. A reused
        // session or a route change can supply a different source format while
        // the analyzer format stays the same. Keep resampling state only while
        // the input format is continuous, including across passthrough buffers.
        if inputFormat != buffer.buffer.format {
            converter = BufferConverter()
            inputFormat = buffer.buffer.format
        }
        let converted = try converter.convertBuffer(buffer.buffer, to: analyzerFormat)
        return AnalyzerInput(buffer: converted)
    }
}
