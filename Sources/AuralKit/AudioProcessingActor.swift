import Foundation
import AVFoundation
import Speech

@available(iOS 26.0, macOS 26.0, *)
actor AudioProcessingActor {
    private let converter = BufferConverter()

    func makeAnalyzerInput(
        from buffer: SpeechSession.SendablePCMBuffer,
        analyzerFormat: AVAudioFormat
    ) throws -> AnalyzerInput {
        let converted = try converter.convertBuffer(buffer.buffer, to: analyzerFormat)
        return AnalyzerInput(buffer: converted)
    }
}
