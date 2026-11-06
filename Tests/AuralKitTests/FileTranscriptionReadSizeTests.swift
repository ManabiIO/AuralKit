import Testing
@testable import AuralKit

@Suite("File transcription read sizes")
struct FileTranscriptionReadSizeTests {
    static let boundaries: [(remaining: Int64, expected: UInt32)] = [
        (remaining: Int64.min, expected: UInt32(0)),
        (remaining: -1, expected: 0),
        (remaining: 0, expected: 0),
        (remaining: 1, expected: 1),
        (remaining: 4_095, expected: 4_095),
        (remaining: 4_096, expected: 4_096),
        (remaining: 4_097, expected: 4_096),
        (remaining: Int64(UInt32.max), expected: 4_096),
        (remaining: Int64(UInt32.max) + 1, expected: 4_096),
        (remaining: Int64.max, expected: 4_096)
    ]

    @Test("bound file positions before converting to a per-read frame count", arguments: boundaries)
    func fileLengthBoundaries(remaining: Int64, expected: UInt32) {
        #expect(AudioFileReadSize.next(remaining: remaining, capacity: 4_096) == expected)
    }

    @Test("the supplied buffer capacity is the read ceiling", arguments: [
        UInt32(0), 1, 4_096, UInt32.max
    ])
    func respectsCapacity(_ capacity: UInt32) {
        #expect(AudioFileReadSize.next(remaining: Int64.max, capacity: capacity) == capacity)
    }

    @Test("full batches and the final short read preserve the entire file length")
    func finalShortRead() {
        let length: Int64 = 8_209
        var processed: Int64 = 0
        var counts: [UInt32] = []
        while processed < length {
            let count = AudioFileReadSize.next(remaining: length - processed, capacity: 4_096)
            guard count > 0 else {
                Issue.record("A nonempty file must make progress")
                return
            }
            counts.append(count)
            processed += Int64(count)
        }
        #expect(counts == [4_096, 4_096, 17])
        #expect(processed == length)
        #expect(AudioFileReadSize.next(remaining: length - processed, capacity: 4_096) == 0)
    }
}
