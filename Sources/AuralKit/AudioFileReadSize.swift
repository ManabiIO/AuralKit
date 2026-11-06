/// AVAudioFile reports a signed 64-bit frame length, while each read accepts
/// an unsigned 32-bit count. Bound the remaining length before narrowing it;
/// a long file still needs only one buffer's worth of frames per iteration.
enum AudioFileReadSize {
    static func next(remaining: Int64, capacity: UInt32) -> UInt32 {
        UInt32(min(max(remaining, 0), Int64(capacity)))
    }
}
