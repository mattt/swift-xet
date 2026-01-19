import Foundation

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
    /// Decodes an async byte sequence into uncompressed chunks.
    ///
    /// - Parameter bytes: An async sequence of bytes representing xorb data.
    /// - Returns: An async sequence yielding uncompressed `Data` for each chunk.
    public static func decode<S: AsyncSequence>(bytes: S) -> ChunkSequence<S>
    where S.Element == UInt8 {
        ChunkSequence(
            source: bytes,
            appendElement: { byte, cursor in
                cursor.append(byte)
            }
        )
    }

    /// Decodes a contiguous xorb buffer into uncompressed chunks.
    ///
    /// - Parameter data: The complete xorb payload.
    /// - Returns: All decoded chunks as decompressed `Data`, in order.
    /// - Throws: ``XorbError`` if the stream is malformed.
    public static func decode(_ data: Data) throws -> [Data] {
        try data.withUnsafeBytes { raw in
            try decode(raw)
        }
    }

    /// Decodes a contiguous xorb buffer into uncompressed chunks.
    ///
    /// - Parameter buffer: The complete xorb payload.
    /// - Returns: All decoded chunks as decompressed `Data`, in order.
    /// - Throws: ``XorbError`` if the stream is malformed.
    public static func decode(_ buffer: UnsafeRawBufferPointer) throws -> [Data] {
        var cursor = ByteCursor()
        cursor.append(contentsOf: buffer)
        var chunks: [Data] = []
        while true {
            if let chunk = try decodeNextChunk(from: &cursor) {
                chunks.append(chunk)
                continue
            }
            if cursor.count == 0 {
                return chunks
            }
            throw XorbError.truncatedStream
        }
    }

    /// Compression schemes supported by the xorb format.
    public enum CompressionScheme: UInt8, Sendable {
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
    public struct Header: Sendable, Equatable {
        /// Protocol version byte.
        public let version: UInt8
        /// Compressed payload size in bytes.
        public let compressedLength: Int
        /// Compression scheme for the payload.
        public let compressionScheme: CompressionScheme
        /// Uncompressed payload size in bytes.
        public let uncompressedLength: Int
    }

    /// Parses an 8-byte chunk header.
    ///
    /// - Parameter bytes: Exactly 8 bytes of header data.
    /// - Returns: The parsed header.
    /// - Throws: ``XorbError`` if the header is invalid.
    public static func parseHeader(_ data: Data) throws -> Header {
        guard data.count == 8 else { throw XorbError.invalidLength }
        return try data.withUnsafeBytes { raw in
            try parseHeader(raw)
        }
    }

    /// Parses an 8-byte chunk header from a raw buffer.
    ///
    /// - Parameter bytes: Buffer containing at least 8 bytes of header data.
    /// - Returns: The parsed header.
    /// - Throws: ``XorbError`` if the header is invalid.
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
    static func decodePayload(_ compressed: Data, header: Header) throws -> Data {
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

    /// Decompresses a raw payload buffer according to the header.
    ///
    /// - Parameters:
    ///   - compressed: The compressed payload bytes.
    ///   - header: The parsed chunk header.
    /// - Returns: The uncompressed chunk data.
    /// - Throws: ``XorbError`` if decompression fails.
    static func decodePayload(_ compressed: UnsafeRawBufferPointer, header: Header) throws -> Data {
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

    /// Decodes the next chunk in a cursor, if available.
    ///
    /// - Parameter cursor: The byte cursor to read from.
    /// - Returns: The next uncompressed chunk, or `nil` if more data is needed.
    /// - Throws: ``XorbError`` if the chunk is malformed.
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
            return try decodePayload(payload, header: header)
        }
        if chunk != nil {
            cursor.consume(count: consumeCount)
        }
        return chunk
    }
}

// MARK: - Xorb.ChunkSequence

extension Xorb {
    /// An async sequence that yields uncompressed chunks from xorb data.
    ///
    /// Wraps a source byte or data stream and parses/decompresses chunks on demand.
    /// Each iteration yields the decompressed `Data` for one chunk.
    public struct ChunkSequence<S: AsyncSequence>: AsyncSequence {
        public typealias Element = Data

        fileprivate let source: S
        fileprivate let appendElement: (S.Element, inout ByteCursor) -> Void

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(source: source, appendElement: appendElement)
        }

        /// The async iterator for xorb chunk decoding.
        public struct AsyncIterator: AsyncIteratorProtocol {
            private var iterator: S.AsyncIterator
            private var cursor = ByteCursor()
            private var reachedEOF = false
            private let appendElement: (S.Element, inout ByteCursor) -> Void

            fileprivate init(
                source: S,
                appendElement: @escaping (S.Element, inout ByteCursor) -> Void
            ) {
                self.iterator = source.makeAsyncIterator()
                self.appendElement = appendElement
            }

            /// Returns the next uncompressed chunk, or `nil` at end of stream.
            public mutating func next() async throws -> Data? {
                while true {
                    if let chunk = try Xorb.decodeNextChunk(from: &cursor) {
                        return chunk
                    }
                    if reachedEOF {
                        if cursor.count == 0 {
                            return nil
                        }
                        throw XorbError.truncatedStream
                    }

                    if let next = try await iterator.next() {
                        appendElement(next, &cursor)
                    } else {
                        reachedEOF = true
                    }
                }
            }
        }
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

// MARK: - ByteCursor

/// A byte buffer with cursor-based reading and automatic compaction.
///
/// Provides efficient streaming reads without repeatedly copying data.
/// The buffer compacts itself when the consumed prefix grows large.
struct ByteCursor {
    /// Minimum consumed bytes before attempting compaction.
    private static let compactThreshold = 4096

    /// The buffer to store the bytes.
    private var buffer = Data()

    /// The index of the first unread byte.
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

    /// Appends data to the buffer.
    mutating func append(_ data: Data) {
        buffer.append(data)
    }

    /// Appends bytes to the buffer.
    mutating func append<S: Sequence>(_ bytes: S) where S.Element == UInt8 {
        buffer.append(contentsOf: bytes)
    }

    /// Appends raw bytes to the buffer.
    mutating func append(contentsOf source: UnsafeRawBufferPointer) {
        let typed = source.bindMemory(to: UInt8.self)
        buffer.append(contentsOf: typed)
    }

    /// Appends a single byte to the buffer.
    mutating func append(_ byte: UInt8) {
        buffer.append(byte)
    }

    /// Peeks at the next `n` bytes without consuming them.
    ///
    /// - Parameter n: Number of bytes to peek.
    /// - Returns: The bytes, or `nil` if fewer than `n` bytes available.
    func peek(count n: Int) -> Data? {
        guard count >= n else { return nil }
        return buffer.subdata(in: startIndex ..< (startIndex + n))
    }

    /// Consumes and returns the next `n` bytes.
    ///
    /// - Parameter n: Number of bytes to consume.
    /// - Returns: The bytes, or `nil` if fewer than `n` bytes available.
    mutating func take(count n: Int) -> Data? {
        guard count >= n else { return nil }
        let head = buffer.subdata(in: startIndex ..< (startIndex + n))
        startIndex += n
        compactIfNeeded()
        return head
    }

    /// Skips the next `n` bytes without allocating a new buffer.
    ///
    /// - Parameter n: Number of bytes to skip.
    /// - Returns: `true` if skipped, `false` if insufficient bytes available.
    mutating func skip(count n: Int) -> Bool {
        guard count >= n else { return false }
        consume(count: n)
        return true
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

        if startIndex > Self.compactThreshold,
            startIndex * 2 > buffer.count
        {
            buffer.removeSubrange(0 ..< startIndex)
            startIndex = 0
        }
    }
}
