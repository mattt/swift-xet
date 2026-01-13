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
        ChunkSequence(source: bytes)
    }

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

        let b = [UInt8](bytes)
        let version = b[0]
        if version != 0 {
            throw XorbError.unsupportedVersion(version)
        }

        let compressedLength = Int(b[1]) | (Int(b[2]) << 8) | (Int(b[3]) << 16)
        let schemeRaw = b[4]
        let uncompressedLength = Int(b[5]) | (Int(b[6]) << 8) | (Int(b[7]) << 16)

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
}

// MARK: - Xorb.ChunkSequence

extension Xorb {
    /// An async sequence that yields uncompressed chunks from xorb data.
    ///
    /// Wraps a source byte stream and parses/decompresses chunks on demand.
    /// Each iteration yields the decompressed `Data` for one chunk.
    public struct ChunkSequence<S: AsyncSequence>: AsyncSequence where S.Element == UInt8 {
        public typealias Element = Data

        fileprivate let source: S

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(source: source)
        }

        /// The async iterator for xorb chunk decoding.
        public struct AsyncIterator: AsyncIteratorProtocol {
            private var iterator: S.AsyncIterator
            private var cursor = ByteCursor()
            private var reachedEOF = false

            fileprivate init(source: S) {
                self.iterator = source.makeAsyncIterator()
            }

            /// Returns the next uncompressed chunk, or `nil` at end of stream.
            public mutating func next() async throws -> Data? {
                while true {
                    if let headerBytes = cursor.peek(count: 8) {
                        let header = try Xorb.parseHeader(headerBytes)
                        if cursor.count >= 8 + header.compressedLength {
                            _ = cursor.take(count: 8)
                            guard let compressed = cursor.take(count: header.compressedLength) else {
                                throw XorbError.truncatedStream
                            }
                            return try Xorb.decodePayload(
                                compressed: compressed,
                                header: header
                            )
                        } else if reachedEOF {
                            throw XorbError.truncatedStream
                        }
                    } else if reachedEOF {
                        if cursor.count == 0 {
                            return nil
                        }
                        throw XorbError.truncatedStream
                    }

                    if let b = try await iterator.next() {
                        cursor.append(b)
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
    private var buffer = Data()
    private var startIndex: Int = 0

    /// The number of unread bytes in the buffer.
    var count: Int { buffer.count - startIndex }

    /// Appends data to the buffer.
    mutating func append(_ data: Data) {
        buffer.append(data)
    }

    /// Appends bytes to the buffer.
    mutating func append<S: Sequence>(_ bytes: S) where S.Element == UInt8 {
        buffer.append(contentsOf: bytes)
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
