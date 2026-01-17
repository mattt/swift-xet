import Foundation

#if canImport(Compression)
    import Compression
#endif

/// LZ4 decompression for raw blocks and standard frames.
///
/// LZ4 is a fast lossless compression algorithm optimized for speed.
/// This implementation supports:
/// - Raw LZ4 blocks (no framing)
/// - Standard LZ4 frames (magic number `0x184D2204`)
///
/// The xorb format uses raw LZ4 blocks for chunk compression.
///
/// - Note: Apple's Compression framework is not used here because it wraps LZ4 data
///   in a proprietary framing format (magic bytes `0x62 0x76 0x34 0x31`) that is
///   incompatible with both raw LZ4 blocks and the standard LZ4 frame format.
/// - SeeAlso: [LZ4 Block Format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md)
/// - SeeAlso: [LZ4 Frame Format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)
public enum LZ4 {
    /// Decompresses an LZ4 block with a known uncompressed size.
    ///
    /// Automatically detects whether the input is a raw block
    /// or a standard frame by checking for the frame magic number.
    ///
    /// - Parameters:
    ///   - compressed: The compressed data.
    ///   - uncompressedLength: The expected size after decompression.
    ///
    /// - Returns: The decompressed data.
    ///
    /// - Throws: ``LZ4Error`` if decompression fails or size doesn't match.
    public static func decompressBlock(_ compressed: Data, uncompressedLength: Int) throws -> Data {
        precondition(uncompressedLength >= 0)
        if uncompressedLength == 0 {
            return Data()
        }

        if compressed.count >= 4,
            compressed[compressed.startIndex] == 0x04,
            compressed[compressed.startIndex.advanced(by: 1)] == 0x22,
            compressed[compressed.startIndex.advanced(by: 2)] == 0x4D,
            compressed[compressed.startIndex.advanced(by: 3)] == 0x18
        {
            let framed = try decompressStandardFrame(compressed, expectedSize: uncompressedLength)
            guard framed.count == uncompressedLength else { throw LZ4Error.decompressionFailed }
            return framed
        }

        let raw = try decompressRawBlock(compressed, maxOutputSize: uncompressedLength)
        guard raw.count == uncompressedLength else { throw LZ4Error.decompressionFailed }
        return raw
    }

    public static func decompressBlock(
        _ compressed: UnsafeRawBufferPointer,
        uncompressedLength: Int
    ) throws -> Data {
        precondition(uncompressedLength >= 0)
        if uncompressedLength == 0 {
            return Data()
        }

        if compressed.count >= 4,
            let base = compressed.bindMemory(to: UInt8.self).baseAddress,
            base[0] == 0x04,
            base[1] == 0x22,
            base[2] == 0x4D,
            base[3] == 0x18
        {
            let data = Data(bytes: base, count: compressed.count)
            let framed = try decompressStandardFrame(data, expectedSize: uncompressedLength)
            guard framed.count == uncompressedLength else { throw LZ4Error.decompressionFailed }
            return framed
        }

        let raw = try decompressRawBlock(compressed, maxOutputSize: uncompressedLength)
        guard raw.count == uncompressedLength else { throw LZ4Error.decompressionFailed }
        return raw
    }

    // MARK: - Raw LZ4 Block Decoding

    /// Decompresses a raw LZ4 block without frame headers.
    ///
    /// The raw block format consists of a sequence of tokens,
    /// each containing literal bytes followed by a match copy operation.
    ///
    /// - Parameters:
    ///   - compressed: The raw compressed block data.
    ///   - maxOutputSize: Maximum bytes to decompress (prevents runaway).
    ///
    /// - Returns: The decompressed data.
    ///
    /// - Throws: ``LZ4Error/invalidFrame`` if the block is malformed.
    public static func decompressRawBlock(_ compressed: Data, maxOutputSize: Int) throws -> Data {
        if maxOutputSize == 0 { return Data() }

        #if canImport(Compression)
            if !compressed.isEmpty {
                guard let dst = malloc(maxOutputSize) else {
                    throw LZ4Error.decompressionFailed
                }
                let decodedCount = compressed.withUnsafeBytes { srcBuffer -> Int in
                    guard let srcBase = srcBuffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                        return 0
                    }
                    return compression_decode_buffer(
                        dst.assumingMemoryBound(to: UInt8.self),
                        maxOutputSize,
                        srcBase,
                        compressed.count,
                        nil,
                        COMPRESSION_LZ4_RAW
                    )
                }
                if decodedCount > 0 {
                    return Data(bytesNoCopy: dst, count: decodedCount, deallocator: .free)
                }
                free(dst)
            }
        #endif

        let src = [UInt8](compressed)
        var i = 0

        func need(_ n: Int) throws {
            if src.count - i < n { throw LZ4Error.invalidFrame }
        }

        func readU8() throws -> UInt8 {
            try need(1)
            defer { i += 1 }
            return src[i]
        }

        func readLen(_ initial: Int) throws -> Int {
            var len = initial
            if len == 15 {
                while true {
                    let b = Int(try readU8())
                    len += b
                    if b != 255 { break }
                }
            }
            return len
        }

        var out: [UInt8] = []
        out.reserveCapacity(min(maxOutputSize, max(256, maxOutputSize)))

        while i < src.count {
            let token = try readU8()

            let litLen = try readLen(Int(token >> 4))
            try need(litLen)
            if out.count + litLen > maxOutputSize { throw LZ4Error.decompressionFailed }
            if litLen > 0 {
                out.append(contentsOf: src[i ..< (i + litLen)])
                i += litLen
            }

            if i >= src.count { break }

            try need(2)
            let offset = Int(src[i]) | (Int(src[i + 1]) << 8)
            i += 2
            if offset <= 0 || offset > out.count { throw LZ4Error.invalidFrame }

            let matchLen = try readLen(Int(token & 0x0F)) + 4
            if out.count + matchLen > maxOutputSize { throw LZ4Error.decompressionFailed }

            for _ in 0 ..< matchLen {
                let b = out[out.count - offset]
                out.append(b)
            }
        }

        return Data(out)
    }

    public static func decompressRawBlock(
        _ compressed: UnsafeRawBufferPointer,
        maxOutputSize: Int
    ) throws -> Data {
        if maxOutputSize == 0 { return Data() }

        #if canImport(Compression)
            if compressed.count > 0, let base = compressed.bindMemory(to: UInt8.self).baseAddress {
                guard let dst = malloc(maxOutputSize) else {
                    throw LZ4Error.decompressionFailed
                }
                let decodedCount = compression_decode_buffer(
                    dst.assumingMemoryBound(to: UInt8.self),
                    maxOutputSize,
                    base,
                    compressed.count,
                    nil,
                    COMPRESSION_LZ4_RAW
                )
                if decodedCount > 0 {
                    return Data(bytesNoCopy: dst, count: decodedCount, deallocator: .free)
                }
                free(dst)
            }
        #endif

        let data: Data
        if let base = compressed.baseAddress, compressed.count > 0 {
            data = Data(bytes: base, count: compressed.count)
        } else {
            data = Data()
        }
        return try decompressRawBlock(data, maxOutputSize: maxOutputSize)
    }

    // MARK: - Standard LZ4 Frame Decoding

    /// Decompresses a standard LZ4 frame.
    ///
    /// Handles the complete frame format including:
    /// - Magic number validation
    /// - Frame descriptor parsing
    /// - Multiple compressed blocks
    /// - Optional checksums
    ///
    /// - Parameters:
    ///   - data: The framed compressed data.
    ///   - expectedSize: Hint for output buffer allocation.
    ///
    /// - Returns: The decompressed data.
    private static func decompressStandardFrame(_ data: Data, expectedSize: Int) throws -> Data {
        var i = 0

        func need(_ n: Int) throws {
            if data.count - i < n { throw LZ4Error.invalidFrame }
        }
        func readU8() throws -> UInt8 {
            try need(1)
            defer { i += 1 }
            return data[i]
        }
        func readU32LE() throws -> UInt32 {
            try need(4)
            let v =
                UInt32(data[i])
                | (UInt32(data[i + 1]) << 8)
                | (UInt32(data[i + 2]) << 16)
                | (UInt32(data[i + 3]) << 24)
            i += 4
            return v
        }
        func readU64LE() throws -> UInt64 {
            try need(8)
            var v: UInt64 = 0
            for b in 0 ..< 8 {
                v |= (UInt64(data[i + b]) << UInt64(8 * b))
            }
            i += 8
            return v
        }
        func readData(_ n: Int) throws -> Data {
            try need(n)
            let sub = data.subdata(in: i ..< (i + n))
            i += n
            return sub
        }

        let magic = try readU32LE()
        guard magic == 0x184D_2204 else { throw LZ4Error.invalidFrame }

        let flg = try readU8()
        let bd = try readU8()

        let version = (flg >> 6) & 0x03
        guard version == 0x01 else { throw LZ4Error.invalidFrame }

        let blockChecksum = (flg & 0x10) != 0
        let contentSizePresent = (flg & 0x08) != 0
        let contentChecksum = (flg & 0x04) != 0
        let dictIDPresent = (flg & 0x01) != 0

        let bdBlockMax = (bd >> 4) & 0x07
        let maxBlockSize: Int =
            switch bdBlockMax {
            case 4: 64 * 1024
            case 5: 256 * 1024
            case 6: 1 * 1024 * 1024
            case 7: 4 * 1024 * 1024
            default: 4 * 1024 * 1024
            }

        if contentSizePresent { _ = try readU64LE() }
        if dictIDPresent { _ = try readU32LE() }

        _ = try readU8()

        var out = Data()
        out.reserveCapacity(expectedSize)

        while true {
            let rawSize = try readU32LE()
            if rawSize == 0 { break }

            let isUncompressed = (rawSize & 0x8000_0000) != 0
            let blockSize = Int(rawSize & 0x7FFF_FFFF)
            let block = try readData(blockSize)

            if isUncompressed {
                out.append(block)
            } else {
                let remaining = max(0, expectedSize - out.count)
                let maxOut = min(maxBlockSize, remaining == 0 ? maxBlockSize : remaining)
                let decoded = try decompressRawBlock(block, maxOutputSize: maxOut)
                out.append(decoded)
            }

            if blockChecksum { _ = try readU32LE() }
        }

        if contentChecksum { _ = try readU32LE() }
        return out
    }
}

/// Errors that can occur during LZ4 decompression.
public enum LZ4Error: Swift.Error, Sendable, Equatable {
    /// Decompression failed or output size doesn't match expected.
    case decompressionFailed

    /// The compressed data is malformed.
    case invalidFrame
}

extension LZ4Error: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .decompressionFailed:
            return "LZ4 decompression failed or output size mismatch."
        case .invalidFrame:
            return "Invalid or malformed LZ4 data."
        }
    }
}
