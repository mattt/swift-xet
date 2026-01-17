import Foundation
import Testing

@testable import Xet

@Suite("Xorb Tests")
struct XorbTests {

    // MARK: - Header Parsing

    @Test func decoderParsesNoneCompression() async throws {
        let payload = Data("hello world".utf8)

        var xorb = Data()
        xorb.append(
            encodeChunkHeader(
                compressedLength: payload.count,
                scheme: 0,  // none
                uncompressedLength: payload.count
            )
        )
        xorb.append(payload)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks.count == 1)
        #expect(chunks[0] == payload)
    }

    @Test func decoderParsesMultipleChunks() async throws {
        let chunk0 = Data("hello".utf8)
        let chunk1 = Data("world".utf8)

        var xorb = Data()
        xorb.append(
            encodeChunkHeader(
                compressedLength: chunk0.count,
                scheme: 0,
                uncompressedLength: chunk0.count
            )
        )
        xorb.append(chunk0)
        xorb.append(
            encodeChunkHeader(
                compressedLength: chunk1.count,
                scheme: 0,
                uncompressedLength: chunk1.count
            )
        )
        xorb.append(chunk1)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks == [chunk0, chunk1])
    }

    @Test func decoderHandlesEmptyStream() async throws {
        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(Data())) {
            chunks.append(chunk)
        }
        #expect(chunks.isEmpty)
    }

    @Test func decoderThrowsOnUnsupportedVersion() async throws {
        var xorb = encodeChunkHeader(
            version: 1,  // unsupported
            compressedLength: 5,
            scheme: 0,
            uncompressedLength: 5
        )
        xorb.append(Data("hello".utf8))

        await #expect(throws: XorbError.self) {
            for try await _ in Xorb.decode(bytes: makeAsyncStream(xorb)) {}
        }
    }

    @Test func decoderThrowsOnUnsupportedCompressionScheme() async throws {
        var xorb = encodeChunkHeader(
            compressedLength: 5,
            scheme: 99,
            uncompressedLength: 5
        )
        xorb.append(Data("hello".utf8))

        await #expect(throws: XorbError.self) {
            for try await _ in Xorb.decode(bytes: makeAsyncStream(xorb)) {}
        }
    }

    @Test func decoderThrowsOnTruncatedHeader() async throws {
        // Only 4 bytes when header needs 8
        let xorb = Data([0x00, 0x05, 0x00, 0x00])

        await #expect(throws: XorbError.truncatedStream) {
            for try await _ in Xorb.decode(bytes: makeAsyncStream(xorb)) {}
        }
    }

    @Test func decoderThrowsOnTruncatedPayload() async throws {
        // Header says 10 bytes but only 5 provided
        var xorb = encodeChunkHeader(compressedLength: 10, scheme: 0, uncompressedLength: 10)
        xorb.append(Data("hello".utf8))  // only 5 bytes

        await #expect(throws: XorbError.truncatedStream) {
            for try await _ in Xorb.decode(bytes: makeAsyncStream(xorb)) {}
        }
    }

    @Test func decoderThrowsOnLengthMismatch() async throws {
        // Uncompressed scheme but compressed != uncompressed length
        let payload = Data("hello".utf8)
        var xorb = encodeChunkHeader(
            compressedLength: payload.count,
            scheme: 0,
            uncompressedLength: payload.count + 5  // mismatch!
        )
        xorb.append(payload)

        await #expect(throws: XorbError.self) {
            for try await _ in Xorb.decode(bytes: makeAsyncStream(xorb)) {}
        }
    }

    // MARK: - LZ4 Compression (scheme 1)

    @Test func decoderHandlesLZ4Compression() async throws {
        // Create a simple LZ4 compressed payload
        // "hello" as literals only: token 0x50, then 5 bytes
        let lz4Payload = Data([0x50, 0x68, 0x65, 0x6C, 0x6C, 0x6F])

        var xorb = encodeChunkHeader(
            compressedLength: lz4Payload.count,
            scheme: 1,  // lz4
            uncompressedLength: 5
        )
        xorb.append(lz4Payload)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks.count == 1)
        #expect(chunks[0] == Data("hello".utf8))
    }

    // MARK: - BG4+LZ4 Compression (scheme 2)

    @Test func decoderHandlesBG4LZ4Compression() async throws {
        // Original data: [0,1,2,3,4,5,6]
        // After BG4 grouping: [0,4,1,5,2,6,3]
        // We compress the grouped version
        let grouped = Data([0, 4, 1, 5, 2, 6, 3])

        // LZ4 literals-only encoding for 7 bytes
        var lz4Payload = Data([0x70])  // 7 literals, 0 match
        lz4Payload.append(grouped)

        var xorb = encodeChunkHeader(
            compressedLength: lz4Payload.count,
            scheme: 2,  // byteGrouping4LZ4
            uncompressedLength: 7
        )
        xorb.append(lz4Payload)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks.count == 1)
        #expect(chunks[0] == Data([0, 1, 2, 3, 4, 5, 6]))
    }

    // MARK: - Large Payloads

    @Test func decoderHandlesLargeUncompressedChunk() async throws {
        let size = 64 * 1024  // 64KB
        let payload = Data(repeating: 0x42, count: size)

        var xorb = encodeChunkHeader(
            compressedLength: size,
            scheme: 0,
            uncompressedLength: size
        )
        xorb.append(payload)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks.count == 1)
        #expect(chunks[0].count == size)
        #expect(chunks[0] == payload)
    }

    // MARK: - Byte-by-byte Streaming

    @Test func decoderWorksWithSlowStream() async throws {
        let payload = Data("test".utf8)
        var xorb = encodeChunkHeader(
            compressedLength: payload.count,
            scheme: 0,
            uncompressedLength: payload.count
        )
        xorb.append(payload)

        var chunks: [Data] = []
        for try await chunk in Xorb.decode(bytes: makeAsyncStream(xorb)) {
            chunks.append(chunk)
        }

        #expect(chunks == [payload])
    }
}

// MARK: -

private func encodeChunkHeader(
    version: UInt8 = 0,
    compressedLength: Int,
    scheme: UInt8,
    uncompressedLength: Int
) -> Data {
    precondition((0 ..< 1 << 24).contains(compressedLength))
    precondition((0 ..< 1 << 24).contains(uncompressedLength))
    var b = [UInt8](repeating: 0, count: 8)
    b[0] = version
    b[1] = UInt8(compressedLength & 0xFF)
    b[2] = UInt8((compressedLength >> 8) & 0xFF)
    b[3] = UInt8((compressedLength >> 16) & 0xFF)
    b[4] = scheme
    b[5] = UInt8(uncompressedLength & 0xFF)
    b[6] = UInt8((uncompressedLength >> 8) & 0xFF)
    b[7] = UInt8((uncompressedLength >> 16) & 0xFF)
    return Data(b)
}

private func makeAsyncStream(_ data: Data) -> AsyncStream<UInt8> {
    AsyncStream<UInt8> { cont in
        for b in data { cont.yield(b) }
        cont.finish()
    }
}
