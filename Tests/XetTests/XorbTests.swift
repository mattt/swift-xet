import Foundation
import NIOCore
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

        let chunks = try decodeAllChunks(from: xorb)
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

        let chunks = try decodeAllChunks(from: xorb)
        #expect(chunks == [chunk0, chunk1])
    }

    @Test func decoderHandlesEmptyStream() async throws {
        let chunks = try decodeAllChunks(from: Data())
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

        #expect(throws: XorbError.self) {
            _ = try decodeAllChunks(from: xorb)
        }
    }

    @Test func decoderThrowsOnUnsupportedCompressionScheme() async throws {
        var xorb = encodeChunkHeader(
            compressedLength: 5,
            scheme: 99,
            uncompressedLength: 5
        )
        xorb.append(Data("hello".utf8))

        #expect(throws: XorbError.self) {
            _ = try decodeAllChunks(from: xorb)
        }
    }

    @Test func decoderThrowsOnTruncatedHeader() async throws {
        let xorb = Data([0x00, 0x05, 0x00, 0x00])

        #expect(throws: XorbError.truncatedStream) {
            _ = try decodeAllChunks(from: xorb, expectComplete: true)
        }
    }

    @Test func decoderThrowsOnTruncatedPayload() async throws {
        var xorb = encodeChunkHeader(compressedLength: 10, scheme: 0, uncompressedLength: 10)
        xorb.append(Data("hello".utf8))

        #expect(throws: XorbError.truncatedStream) {
            _ = try decodeAllChunks(from: xorb, expectComplete: true)
        }
    }

    @Test func decoderThrowsOnLengthMismatch() async throws {
        let payload = Data("hello".utf8)
        var xorb = encodeChunkHeader(
            compressedLength: payload.count,
            scheme: 0,
            uncompressedLength: payload.count + 5
        )
        xorb.append(payload)

        #expect(throws: XorbError.self) {
            _ = try decodeAllChunks(from: xorb)
        }
    }

    // MARK: - LZ4 Compression (scheme 1)

    @Test func decoderHandlesLZ4Compression() async throws {
        let lz4Payload = Data([0x50, 0x68, 0x65, 0x6C, 0x6C, 0x6F])

        var xorb = encodeChunkHeader(
            compressedLength: lz4Payload.count,
            scheme: 1,
            uncompressedLength: 5
        )
        xorb.append(lz4Payload)

        let chunks = try decodeAllChunks(from: xorb)
        #expect(chunks.count == 1)
        #expect(chunks[0] == Data("hello".utf8))
    }

    // MARK: - BG4+LZ4 Compression (scheme 2)

    @Test func decoderHandlesBG4LZ4Compression() async throws {
        let grouped = Data([0, 4, 1, 5, 2, 6, 3])
        var lz4Payload = Data([0x70])
        lz4Payload.append(grouped)

        var xorb = encodeChunkHeader(
            compressedLength: lz4Payload.count,
            scheme: 2,
            uncompressedLength: 7
        )
        xorb.append(lz4Payload)

        let chunks = try decodeAllChunks(from: xorb)
        #expect(chunks.count == 1)
        #expect(chunks[0] == Data([0, 1, 2, 3, 4, 5, 6]))
    }

    // MARK: - Large Payloads

    @Test func decoderHandlesLargeUncompressedChunk() async throws {
        let size = 64 * 1024
        let payload = Data(repeating: 0x42, count: size)

        var xorb = encodeChunkHeader(
            compressedLength: size,
            scheme: 0,
            uncompressedLength: size
        )
        xorb.append(payload)

        let chunks = try decodeAllChunks(from: xorb)
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

        let chunks = try decodeAllChunks(from: xorb)
        #expect(chunks == [payload])
    }
}

// MARK: - Helpers

private func decodeAllChunks(from data: Data, expectComplete: Bool = true) throws -> [Data] {
    var buffer = ByteBuffer(data: data)
    var chunks: [Data] = []

    while true {
        if let chunk = try Xorb.decodeNextChunk(from: &buffer) {
            chunks.append(chunk)
        } else {
            break
        }
    }

    if expectComplete && buffer.readableBytes > 0 {
        throw XorbError.truncatedStream
    }

    return chunks
}

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
