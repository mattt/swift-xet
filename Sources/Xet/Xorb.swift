import Foundation
import NIOCore

// MARK: - Xorb

/// Decodes xorb byte streams into uncompressed chunk data.
///
/// A xorb (Xet Orb) is a serialized sequence of compressed chunks.
/// Each chunk consists of an 8-byte header followed by compressed data.
///
/// ## Chunk Header Format
///
/// | Bytes |         Field        |              Description                 |
/// |------:|:---------------------|:-----------------------------------------|
/// |   0   | Version              | Protocol version (currently 0)           |
/// | 1-3   | Compressed Size      | Little-endian 24-bit integer             |
/// |   4   | Compression Type     | 0=none, 1=LZ4, 2=BG4+LZ4                 |
/// | 5-7   | Uncompressed Size    | Little-endian 24-bit integer             |
///
/// ## Usage
///
/// ```swift
/// for try await chunk in Xorb.decode(bytes: asyncByteStream) {
///     // Process uncompressed chunk data
/// }
/// ```
public enum Xorb {
    /// Compression schemes supported by the xorb format.
    enum CompressionScheme: UInt8, Sendable {
        /// No compression; data stored as-is.
        case none = 0

        /// Standard LZ4 block compression.
        case lz4 = 1

        /// Byte Grouping 4 preprocessing followed by LZ4 compression.
        ///
        /// Optimized for floating-point and structured data where
        /// grouping bytes by position improves compression ratios.
        case byteGrouping4LZ4 = 2
    }

    /// Parsed chunk header containing size and compression metadata.
    struct Header: Sendable, Equatable {
        let version: UInt8
        let compressedLength: Int
        let compressionScheme: CompressionScheme
        let uncompressedLength: Int
    }

    /// Parses an 8-byte chunk header.
    ///
    /// - Parameter bytes: Exactly 8 bytes of header data.
    /// - Returns: The parsed header.
    /// - Throws: ``XorbError`` if the header is invalid.
    static func parseHeader(_ bytes: Data) throws -> Header {
        guard bytes.count == 8 else { throw XorbError.invalidLength }
        return try bytes.withUnsafeBytes { raw in
            try parseHeader(raw)
        }
    }

    static func parseHeader(_ bytes: UnsafeRawBufferPointer) throws -> Header {
        guard bytes.count >= 8 else { throw XorbError.invalidLength }

        let version = bytes[0]
        if version != 0 {
            throw XorbError.unsupportedVersion(version)
        }

        let compressedLength = Int(bytes[1]) | (Int(bytes[2]) << 8) | (Int(bytes[3]) << 16)
        let schemeRaw = bytes[4]
        let uncompressedLength = Int(bytes[5]) | (Int(bytes[6]) << 8) | (Int(bytes[7]) << 16)

        guard let scheme = CompressionScheme(rawValue: schemeRaw) else {
            throw XorbError.unsupportedCompressionScheme(schemeRaw)
        }

        return Header(
            version: version,
            compressedLength: compressedLength,
            compressionScheme: scheme,
            uncompressedLength: uncompressedLength
        )
    }

    /// Decompresses chunk payload data according to the header's compression scheme.
    ///
    /// - Parameters:
    ///   - compressed: The compressed payload bytes.
    ///   - header: The parsed chunk header.
    /// - Returns: The uncompressed chunk data.
    /// - Throws: ``XorbError`` if decompression fails.
    static func decodePayload(compressed: Data, header: Header) throws -> Data {
        switch header.compressionScheme {
        case .none:
            guard compressed.count == header.uncompressedLength else {
                throw XorbError.lengthMismatch(
                    expected: header.uncompressedLength,
                    actual: compressed.count
                )
            }
            return compressed

        case .lz4:
            do {
                return try LZ4.decompressBlock(
                    compressed,
                    uncompressedLength: header.uncompressedLength
                )
            } catch {
                throw XorbError.decompressionFailed
            }

        case .byteGrouping4LZ4:
            let decompressed: Data
            do {
                decompressed = try LZ4.decompressBlock(
                    compressed,
                    uncompressedLength: header.uncompressedLength
                )
            } catch {
                throw XorbError.decompressionFailed
            }
            return BG4.regroup(decompressed)
        }
    }

    static func decodePayload(compressed: UnsafeRawBufferPointer, header: Header) throws -> Data {
        switch header.compressionScheme {
        case .none:
            guard compressed.count == header.uncompressedLength else {
                throw XorbError.lengthMismatch(
                    expected: header.uncompressedLength,
                    actual: compressed.count
                )
            }
            if compressed.count == 0 {
                return Data()
            }
            return Data(bytes: compressed.baseAddress!, count: compressed.count)

        case .lz4:
            do {
                return try LZ4.decompressBlock(
                    compressed,
                    uncompressedLength: header.uncompressedLength
                )
            } catch {
                throw XorbError.decompressionFailed
            }

        case .byteGrouping4LZ4:
            let decompressed: Data
            do {
                decompressed = try LZ4.decompressBlock(
                    compressed,
                    uncompressedLength: header.uncompressedLength
                )
            } catch {
                throw XorbError.decompressionFailed
            }
            return BG4.regroup(decompressed)
        }
    }

    static func decodeNextChunk(from buffer: inout ByteBuffer) throws -> Data? {
        var consumeCount = 0
        let chunk = try buffer.withUnsafeReadableBytes { raw -> Data? in
            guard raw.count >= 8 else { return nil }
            let header = try parseHeader(UnsafeRawBufferPointer(raw))
            let totalLength = 8 + header.compressedLength
            guard raw.count >= totalLength else { return nil }
            guard let base = raw.baseAddress else { return nil }
            let payloadStart = base.advanced(by: 8)
            let payload = UnsafeRawBufferPointer(start: payloadStart, count: header.compressedLength)
            consumeCount = totalLength
            return try decodePayload(compressed: payload, header: header)
        }
        if chunk != nil {
            buffer.moveReaderIndex(forwardBy: consumeCount)
        }
        return chunk
    }

    /// Decodes the next chunk directly into an output buffer.
    ///
    /// This variant avoids allocating a new `Data` for each chunk by writing
    /// directly into the caller's pre-allocated buffer.
    ///
    /// - Parameters:
    ///   - buffer: The ByteBuffer containing compressed xorb data.
    ///   - output: The destination buffer to write decompressed data into.
    ///   - offset: The byte offset within `output` to start writing.
    ///   - scratch: Optional scratch buffer for BG4 regrouping (required only
    ///     for `.byteGrouping4LZ4` chunks, must be >= chunk uncompressed size).
    ///
    /// - Returns: The number of bytes written, or `nil` if not enough data available.
    ///
    /// - Throws: ``XorbError`` if decompression fails.
    static func decodeNextChunk(
        from buffer: inout ByteBuffer,
        into output: UnsafeMutableRawBufferPointer,
        at offset: Int,
        scratch: UnsafeMutableRawBufferPointer?
    ) throws -> Int? {
        var consumeCount = 0

        let bytesWritten: Int? = try buffer.withUnsafeReadableBytes { raw -> Int? in
            guard raw.count >= 8 else { return nil }
            let h = try parseHeader(UnsafeRawBufferPointer(raw))
            let totalLength = 8 + h.compressedLength
            guard raw.count >= totalLength else { return nil }
            guard let base = raw.baseAddress else { return nil }
            let payloadStart = base.advanced(by: 8)
            let payload = UnsafeRawBufferPointer(start: payloadStart, count: h.compressedLength)
            consumeCount = totalLength

            guard offset + h.uncompressedLength <= output.count else {
                throw XorbError.lengthMismatch(
                    expected: h.uncompressedLength,
                    actual: output.count - offset
                )
            }

            guard let destBase = output.baseAddress?.advanced(by: offset) else {
                throw XorbError.decompressionFailed
            }

            switch h.compressionScheme {
            case .none:
                guard payload.count == h.uncompressedLength else {
                    throw XorbError.lengthMismatch(
                        expected: h.uncompressedLength,
                        actual: payload.count
                    )
                }
                if let src = payload.baseAddress {
                    memcpy(destBase, src, payload.count)
                }
                return h.uncompressedLength

            case .lz4:
                let written = try LZ4.decompressBlock(
                    payload,
                    into: destBase,
                    maxOutputSize: h.uncompressedLength
                )
                guard written == h.uncompressedLength else {
                    throw XorbError.decompressionFailed
                }
                return written

            case .byteGrouping4LZ4:
                guard let scratchBase = scratch?.baseAddress,
                    scratch!.count >= h.uncompressedLength
                else {
                    throw XorbError.decompressionFailed
                }
                let written = try LZ4.decompressBlock(
                    payload,
                    into: scratchBase,
                    maxOutputSize: h.uncompressedLength
                )
                guard written == h.uncompressedLength else {
                    throw XorbError.decompressionFailed
                }
                let srcBuffer = UnsafeRawBufferPointer(start: scratchBase, count: written)
                let dstBuffer = UnsafeMutableRawBufferPointer(start: destBase, count: written)
                BG4.regroup(from: srcBuffer, into: dstBuffer)
                return written
            }
        }

        if bytesWritten != nil {
            buffer.moveReaderIndex(forwardBy: consumeCount)
        }
        return bytesWritten
    }
}



// MARK: - ByteCursor

/// A byte buffer with cursor-based reading and automatic compaction.
///
/// Provides efficient streaming reads without repeatedly copying data.
/// The buffer compacts itself when the consumed prefix grows large.
struct ByteCursor {
    private var buffer = Data()
    private var startIndex: Int = 0

    /// The number of unread bytes in the buffer.
    var count: Int { buffer.count - startIndex }

    /// Provides unsafe access to the unread portion of the buffer.
    func withUnsafeReadableBytes<T>(_ body: (UnsafeRawBufferPointer) throws -> T) rethrows -> T {
        try buffer.withUnsafeBytes { raw in
            let readableCount = max(0, raw.count - startIndex)
            let start = raw.baseAddress?.advanced(by: startIndex)
            let readable = UnsafeRawBufferPointer(start: start, count: readableCount)
            return try body(readable)
        }
    }

    /// Appends raw bytes to the buffer.
    mutating func append(_ bytes: UnsafeRawBufferPointer) {
        let typed = bytes.bindMemory(to: UInt8.self)
        buffer.append(contentsOf: typed)
    }

    /// Consumes the next `n` bytes.
    mutating func consume(count n: Int) {
        startIndex += n
        compactIfNeeded()
    }

    /// Removes consumed bytes when the prefix is large enough to warrant it.
    private mutating func compactIfNeeded() {
        if startIndex == buffer.count {
            buffer.removeAll(keepingCapacity: true)
            startIndex = 0
            return
        }

        if startIndex > 4096, startIndex * 2 > buffer.count {
            buffer.removeSubrange(0 ..< startIndex)
            startIndex = 0
        }
    }
}

extension Xorb {
    static func decodeNextChunk(from cursor: inout ByteCursor) throws -> Data? {
        var consumeCount = 0
        let chunk = try cursor.withUnsafeReadableBytes { raw -> Data? in
            guard raw.count >= 8 else { return nil }
            let header = try parseHeader(raw)
            let totalLength = 8 + header.compressedLength
            guard raw.count >= totalLength else { return nil }
            guard let base = raw.baseAddress else { return nil }
            let payloadStart = base.advanced(by: 8)
            let payload = UnsafeRawBufferPointer(start: payloadStart, count: header.compressedLength)
            consumeCount = totalLength
            return try decodePayload(compressed: payload, header: header)
        }
        if chunk != nil {
            cursor.consume(count: consumeCount)
        }
        return chunk
    }
}

// MARK: - XorbError

/// Errors that can occur during xorb chunk decoding.
public enum XorbError: Error, Hashable, Sendable {
    /// The chunk header specifies an unsupported version.
    case unsupportedVersion(UInt8)

    /// The chunk header specifies an unknown compression scheme.
    case unsupportedCompressionScheme(UInt8)

    /// The header data is not exactly 8 bytes.
    case invalidLength

    /// The byte stream ended before a complete chunk was received.
    case truncatedStream

    /// LZ4 or BG4 decompression failed.
    case decompressionFailed

    /// The uncompressed data length doesn't match the header.
    case lengthMismatch(expected: Int, actual: Int)
}

extension XorbError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .unsupportedVersion(version):
            return "Unsupported xorb chunk version: \(version)"
        case let .unsupportedCompressionScheme(scheme):
            return "Unsupported compression scheme: \(scheme)"
        case .invalidLength:
            return "Invalid chunk header length."
        case .truncatedStream:
            return "Unexpected end of xorb stream."
        case .decompressionFailed:
            return "Chunk decompression failed."
        case let .lengthMismatch(expected, actual):
            return "Decompressed length mismatch: expected \(expected), got \(actual)."
        }
    }
}


