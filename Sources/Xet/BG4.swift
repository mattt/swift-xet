import Foundation

#if canImport(Accelerate)
    import Accelerate
#endif

/// Byte Grouping 4 (BG4) deinterleaving for improved LZ4 compression.
///
/// BG4 is a preprocessing step that reorganizes data to improve compression.
/// It groups bytes by their position within 4-byte units,
/// creating runs of similar byte values that compress better.
///
/// Given input bytes `[A1, A2, A3, A4, B1, B2, B3, B4, ...]`:
/// - Group 0 contains all first bytes: `[A1, B1, C1, ...]`
/// - Group 1 contains all second bytes: `[A2, B2, C2, ...]`
/// - Group 2 contains all third bytes: `[A3, B3, C3, ...]`
/// - Group 3 contains all fourth bytes: `[A4, B4, C4, ...]`
///
/// The encoder concatenates these groups: `Group0 || Group1 || Group2 || Group3`
///
/// This is effective for floating-point numbers, integers, and other structured
/// data where bytes at the same position often have similar values
/// (e.g., exponent bytes cluster together).
///
/// The xorb format uses BG4 with the `byteGrouping4LZ4` compression scheme.
/// Decoding applies LZ4 decompression first,
/// then ``regroup(_:)`` to restore the original byte order.
public enum BG4 {
    /// Restores interleaved byte order from BG4-grouped data.
    ///
    /// Reverses the BG4 split operation:
    /// takes `Group0 || Group1 || Group2 || Group3`
    /// and restores `[A1, A2, A3, A4, B1, B2, B3, B4, ...]`.
    ///
    /// Uses Accelerate framework on Apple platforms for SIMD optimization
    /// when processing 256 or more bytes.
    ///
    /// - Parameter grouped: Data with bytes grouped by position mod 4.
    /// - Returns: Data with original interleaved byte order restored.
    public static func regroup(_ grouped: Data) -> Data {
        let n = grouped.count
        guard n > 0 else { return Data() }

        var out = Data(count: n)
        out.withUnsafeMutableBytes { outRaw in
            grouped.withUnsafeBytes { inRaw in
                regroup(inRaw, into: outRaw)
            }
        }
        return out
    }

    /// Restores interleaved byte order from BG4-grouped data in-place.
    ///
    /// - Parameters:
    ///   - grouped: Source buffer with bytes grouped by position mod 4.
    ///   - output: Destination buffer (must be at least as large as source).
    public static func regroup(
        _ grouped: UnsafeRawBufferPointer,
        into output: UnsafeMutableRawBufferPointer
    ) {
        let n = grouped.count
        guard n > 0, output.count >= n else { return }

        let split = n / 4
        let rem = n % 4

        let g1Pos = split + (rem >= 1 ? 1 : 0)
        let g2Pos = g1Pos + split + (rem >= 2 ? 1 : 0)
        let g3Pos = g2Pos + split + (rem == 3 ? 1 : 0)

        guard
            let outPtr = output.baseAddress?.assumingMemoryBound(to: UInt8.self),
            let inPtr = grouped.baseAddress?.assumingMemoryBound(to: UInt8.self)
        else { return }

        #if canImport(Accelerate)
            if split > 0, n >= 256 {
                var a = vImage_Buffer(
                    data: UnsafeMutableRawPointer(mutating: inPtr),
                    height: 1,
                    width: vImagePixelCount(split),
                    rowBytes: split
                )
                var r = vImage_Buffer(
                    data: UnsafeMutableRawPointer(mutating: inPtr.advanced(by: g1Pos)),
                    height: 1,
                    width: vImagePixelCount(split),
                    rowBytes: split
                )
                var g = vImage_Buffer(
                    data: UnsafeMutableRawPointer(mutating: inPtr.advanced(by: g2Pos)),
                    height: 1,
                    width: vImagePixelCount(split),
                    rowBytes: split
                )
                var b = vImage_Buffer(
                    data: UnsafeMutableRawPointer(mutating: inPtr.advanced(by: g3Pos)),
                    height: 1,
                    width: vImagePixelCount(split),
                    rowBytes: split
                )
                var dest = vImage_Buffer(
                    data: UnsafeMutableRawPointer(outPtr),
                    height: 1,
                    width: vImagePixelCount(split),
                    rowBytes: split * 4
                )

                let err = vImageConvert_Planar8toARGB8888(
                    &a,
                    &r,
                    &g,
                    &b,
                    &dest,
                    vImage_Flags(kvImageNoFlags)
                )
                if err == kvImageNoError {
                    let base = split * 4
                    if rem >= 1 { outPtr[base] = inPtr[split] }
                    if rem >= 2 { outPtr[base + 1] = inPtr[g1Pos + split] }
                    if rem == 3 { outPtr[base + 2] = inPtr[g2Pos + split] }
                    return
                }
            }
        #endif

        var j = 0
        var i = 0
        while i < n {
            outPtr[i] = inPtr[j]
            i += 4
            j += 1
        }

        j = g1Pos
        i = 1
        while i < n {
            outPtr[i] = inPtr[j]
            i += 4
            j += 1
        }

        j = g2Pos
        i = 2
        while i < n {
            outPtr[i] = inPtr[j]
            i += 4
            j += 1
        }

        j = g3Pos
        i = 3
        while i < n {
            outPtr[i] = inPtr[j]
            i += 4
            j += 1
        }
    }
}
