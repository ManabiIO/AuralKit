import AVFoundation
import Foundation
import Speech

/// One installed tap owns its converter and delivery continuation. Closing joins
/// admitted callbacks before the session finishes analyzer input or releases leases.
@available(iOS 26.0, macOS 26.0, *)
final class MicrophoneAudioIngress: @unchecked Sendable {
    private let queue = DispatchQueue(label: "com.auralkit.microphone.input", qos: .userInitiated)
    private let converter = BufferConverter()
    private let format: AVAudioFormat
    private let onError: @Sendable (Error) -> Void
    private var deliver: (@Sendable (AVAudioPCMBuffer) -> Void)?

    init(
        format: AVAudioFormat,
        deliver: @escaping @Sendable (AVAudioPCMBuffer) -> Void,
        onError: @escaping @Sendable (Error) -> Void
    ) {
        self.format = format
        self.deliver = deliver
        self.onError = onError
    }

    func receive(_ buffer: AVAudioPCMBuffer, ownsBuffer: Bool = false) {
        queue.sync {
            guard let deliver else { return }
            do {
                let converted = try converter.convertBuffer(buffer, to: format)
                if converted === buffer, !ownsBuffer {
                    guard let copy = buffer.copy() as? AVAudioPCMBuffer else {
                        throw SpeechSessionError.conversionBufferCreationFailed
                    }
                    deliver(copy)
                } else {
                    deliver(converted)
                }
            } catch {
                onError(error)
            }
        }
    }

    func close() {
        queue.sync { deliver = nil }
    }
}
