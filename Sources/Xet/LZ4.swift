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
/// On Apple platforms, raw block decompression uses the Compression framework's
/// `COMPRESSION_LZ4_RAW` algorithm for optimal performance.
/// A pure Swift fallback is used when Compression is unavailable
/// or for edge cases requiring precise bounds checking.
///
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
    /// - Throws: ``LZ4Error`` if decompression fails or size doesn't match.
    public static func decompressBlock(_ compressed: Data, uncompressedLength: Int) throws -> Data {
        guard uncompressedLength > 0 else { return Data() }

        return try compressed.withUnsafeBytes { srcBuffer in
            try decompressBlock(srcBuffer, uncompressedLength: uncompressedLength)
        }
    }

    /// Decompresses an LZ4 block with a known uncompressed size.
    ///
    /// - Parameters:
    ///   - compressed: The compressed data as an unsafe buffer pointer.
    ///   - uncompressedLength: The expected size after decompression.
    ///
    /// - Returns: The decompressed data.
    /// - Throws: ``LZ4Error`` if decompression fails or size doesn't match.
    public static func decompressBlock(
        _ compressed: UnsafeRawBufferPointer,
        uncompressedLength: Int
    ) throws -> Data {
        guard uncompressedLength > 0 else { return Data() }

        guard let dst = calloc(uncompressedLength, 1) else {
            throw LZ4Error.decompressionFailed
        }

        do {
            let output = UnsafeMutableRawBufferPointer(start: dst, count: uncompressedLength)
            let written = try decompressBlock(
                compressed,
                uncompressedLength: uncompressedLength,
                output: output
            )
            guard written == uncompressedLength else {
                free(dst)
                throw LZ4Error.decompressionFailed
            }
            return Data(bytesNoCopy: dst, count: uncompressedLength, deallocator: .free)
        } catch {
            free(dst)
            throw error
        }
    }

    /// Decompresses an LZ4 block into a pre-allocated output buffer.
    ///
    /// This is the primary decompression method. Automatically detects whether
    /// the input is a raw block or a standard frame by checking for the frame magic number.
    ///
    /// - Parameters:
    ///   - compressed: The compressed source buffer.
    ///   - uncompressedLength: The expected size after decompression.
    ///   - output: Pre-allocated output buffer (must be >= uncompressedLength).
    ///
    /// - Returns: The number of bytes written to output.
    /// - Throws: ``LZ4Error`` if decompression fails.
    public static func decompressBlock(
        _ compressed: UnsafeRawBufferPointer,
        uncompressedLength: Int,
        output: UnsafeMutableRawBufferPointer
    ) throws -> Int {
        guard uncompressedLength > 0 else { return 0 }
        guard output.count >= uncompressedLength else {
            throw LZ4Error.decompressionFailed
        }

        if isStandardFrame(compressed) {
            guard let baseAddress = compressed.baseAddress else {
                throw LZ4Error.decompressionFailed
            }
            let data = Data(bytes: baseAddress, count: compressed.count)
            let framed = try _decompressFrame(data, expectedSize: uncompressedLength)
            guard framed.count == uncompressedLength else {
                throw LZ4Error.decompressionFailed
            }
            framed.withUnsafeBytes { src in
                if let srcBase = src.baseAddress, let dstBase = output.baseAddress {
                    memcpy(dstBase, srcBase, framed.count)
                }
            }
            return framed.count
        }

        let written = try decompressRawBlock(compressed, output: output)
        guard written == uncompressedLength else {
            throw LZ4Error.decompressionFailed
        }
        return written
    }

    // MARK: - Raw Block Decompression

    /// Decompresses a raw LZ4 block without frame headers.
    ///
    /// - Parameters:
    ///   - compressed: The raw compressed block data.
    ///   - maxOutputSize: Maximum bytes to decompress.
    ///
    /// - Returns: The decompressed data.
    /// - Throws: ``LZ4Error`` if decompression fails.
    public static func decompressRawBlock(_ compressed: Data, maxOutputSize: Int) throws -> Data {
        guard maxOutputSize > 0 else { return Data() }

        return try compressed.withUnsafeBytes { srcBuffer in
            try decompressRawBlock(srcBuffer, maxOutputSize: maxOutputSize)
        }
    }

    /// Decompresses a raw LZ4 block into a pre-allocated output buffer.
    ///
    /// This is the core decompression routine. Uses Apple's Compression framework
    /// when available for optimal performance.
    ///
    /// - Parameters:
    ///   - compressed: The raw compressed source buffer.
    ///   - output: Pre-allocated output buffer.
    ///
    /// - Returns: The number of bytes written to output.
    /// - Throws: ``LZ4Error`` if decompression fails.
    public static func decompressRawBlock(
        _ compressed: UnsafeRawBufferPointer,
        output: UnsafeMutableRawBufferPointer
    ) throws -> Int {
        let maxOutputSize = output.count
        guard maxOutputSize > 0 else { return 0 }

        #if canImport(Compression)
            if compressed.count > 0,
                let srcBase = compressed.bindMemory(to: UInt8.self).baseAddress,
                let dstBase = output.baseAddress?.assumingMemoryBound(to: UInt8.self)
            {
                let decodedCount = compression_decode_buffer(
                    dstBase,
                    maxOutputSize,
                    srcBase,
                    compressed.count,
                    nil,
                    COMPRESSION_LZ4_RAW
                )
                // Keep this strict (< maxOutputSize).
                // Relaxing it to <= would require extra validation in the fast path,
                // which defeats the performance advantage.
                // Exact-sized outputs fall back safely.
                if decodedCount > 0, decodedCount < maxOutputSize {
                    return decodedCount
                }
            }
        #endif

        let decompressed = try _decompressRawBlock(compressed, maxOutputSize: maxOutputSize)
        guard decompressed.count <= maxOutputSize else {
            throw LZ4Error.decompressionFailed
        }
        decompressed.withUnsafeBytes { src in
            if let srcBase = src.baseAddress, let dstBase = output.baseAddress {
                memcpy(dstBase, srcBase, decompressed.count)
            }
        }
        return decompressed.count
    }

    /// Decompresses a raw LZ4 block without frame headers.
    ///
    /// - Parameters:
    ///   - compressed: The raw compressed block data.
    ///   - maxOutputSize: Maximum bytes to decompress.
    ///
    /// - Returns: The decompressed data.
    /// - Throws: ``LZ4Error`` if decompression fails.
    public static func decompressRawBlock(
        _ compressed: UnsafeRawBufferPointer,
        maxOutputSize: Int
    ) throws -> Data {
        guard maxOutputSize > 0 else { return Data() }

        guard let dst = malloc(maxOutputSize) else {
            throw LZ4Error.decompressionFailed
        }

        do {
            let output = UnsafeMutableRawBufferPointer(start: dst, count: maxOutputSize)
            let written = try decompressRawBlock(compressed, output: output)
            return Data(bytesNoCopy: dst, count: written, deallocator: .free)
        } catch {
            free(dst)
            throw error
        }
    }

    // MARK: - Private Helpers

    /// Checks if the buffer starts with the standard LZ4 frame magic number.
    private static func isStandardFrame(_ buffer: UnsafeRawBufferPointer) -> Bool {
        guard buffer.count >= 4,
            let base = buffer.bindMemory(to: UInt8.self).baseAddress
        else { return false }
        return base[0] == 0x04 && base[1] == 0x22 && base[2] == 0x4D && base[3] == 0x18
    }

    /// Pure Swift implementation of raw LZ4 block decompression.
    /// Used as fallback when Compression framework is unavailable or fails.
    private static func _decompressRawBlock(
        _ compressed: UnsafeRawBufferPointer,
        maxOutputSize: Int
    ) throws -> Data {
        guard compressed.count > 0, let base = compressed.baseAddress else {
            return Data()
        }

        let src = UnsafeBufferPointer(start: base.assumingMemoryBound(to: UInt8.self), count: compressed.count)
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

    /// Decompresses a standard LZ4 frame.
    private static func _decompressFrame(_ data: Data, expectedSize: Int) throws -> Data {
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

// MARK: - Errors

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
