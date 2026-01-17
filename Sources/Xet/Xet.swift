import AsyncHTTPClient
import Darwin
import Foundation
import NIOCore
import NIOHTTP1

/// Downloads files from Hugging Face's content-addressable storage (CAS)
/// using the Xet protocol.
///
/// `XetDownloader` orchestrates the complete file download workflow:
/// 1. Obtains CAS access credentials from the Hugging Face Hub
/// 2. Fetches reconstruction metadata via the CAS API
/// 3. Downloads and decompresses xorb chunks
/// 4. Reassembles chunks into the original file
///
/// ## Usage
///
/// Download a file to memory:
///
/// ```swift
/// let downloader = XetDownloader(
///     refreshURL: tokenURL,
///     hubToken: "hf_..."
/// )
/// let data = try await downloader.data(for: fileID)
/// ```
///
/// Download a file to disk:
///
/// ```swift
/// try await downloader.download(fileID, to: destinationURL)
/// ```
///
/// Both methods support partial downloads via the `byteRange` parameter.
/// The downloader handles chunk-level alignment automatically,
/// skipping bytes at the start and truncating at the end as needed.
public struct XetDownloader: Sendable {
    private let refreshURL: URL
    private let hubToken: String?
    private let tokenProvider: TokenProvider
    private let casClient: CASClient
    private let httpClientPool: HTTPClientPool
    private let maxConcurrentFetches: Int
    private let enableTaskMetricsLogging: Bool
    private let httpClientRequestTimeout: TimeAmount
    private let maxConcurrentWrites: Int
    private let maxConcurrentDecodes: Int
    private let maxInflightBuffers: Int

    /// Whether to allow insecure (non-HTTPS) connections.
    ///
    /// By default, the downloader requires HTTPS for all CAS and fetch URLs.
    /// Set this to `true` only for local development or testing with
    /// non-production servers.
    ///
    /// - Warning: Enabling insecure connections in production is a security risk.
    ///   Tokens and file contents may be transmitted in plaintext.
    public var allowsInsecureConnections: Bool = false

    /// Configuration for tuning downloader performance.
    public struct Configuration: Sendable {
        public var maxConcurrentFetches: Int = 64
        public var maxConcurrentDecodes: Int = Configuration.recommendedDecodeConcurrency
        public var maxConcurrentWrites: Int = 64
        public var maxInflightBuffers: Int = 8
        public var connectionsPerHost: Int = 32
        public var prewarmedConnections: Int = 8
        public var poolSize: Int = 2
        public var forceHTTP1: Bool = true
        public var connectTimeout: TimeInterval = 10
        public var readTimeout: TimeInterval = 60
        public var autoScaleFetchConcurrency: Bool = true
        public var enableTaskMetricsLogging: Bool = false
        public var waitsForConnectivity: Bool = true

        public static var recommendedDecodeConcurrency: Int {
            max(1, ProcessInfo.processInfo.activeProcessorCount)
        }

        public static let `default` = Configuration()

        public static let highThroughput: Configuration = {
            var config = Configuration()
            config.maxConcurrentFetches = 512
            config.maxConcurrentDecodes = Configuration.recommendedDecodeConcurrency
            config.maxConcurrentWrites = 512
            config.maxInflightBuffers = 16
            config.connectionsPerHost = 256
            config.prewarmedConnections = 64
            config.poolSize = 6
            config.connectTimeout = 5
            config.readTimeout = 120
            return config
        }()
    }

    /// Creates a downloader configured for a specific repository.
    ///
    /// - Parameters:
    ///   - refreshURL: The Hugging Face Hub URL for obtaining CAS tokens.
    ///     Format: `https://huggingface.co/api/{type}s/{repo}/xet-read-token/{ref}`
    ///   - hubToken: Optional Hugging Face Hub authentication token.
    ///     Required for private repositories.
    ///   - configuration: Downloader configuration.
    public init(
        refreshURL: URL,
        hubToken: String? = nil,
        configuration: Configuration = .highThroughput
    ) {
        self.refreshURL = refreshURL
        self.hubToken = hubToken
        self.tokenProvider = TokenProvider(
            urlSession: .shared,
            enableDebugLogging: configuration.enableTaskMetricsLogging
        )
        self.casClient = CASClient(urlSession: .shared)
        let effectiveMaxConcurrentFetches = max(1, configuration.maxConcurrentFetches)
        let httpConfiguration = XetDownloader.makeHTTPClientConfiguration(
            connectionsPerHost: configuration.connectionsPerHost,
            prewarmedConnections: configuration.prewarmedConnections,
            forceHTTP1: configuration.forceHTTP1,
            connectTimeout: configuration.connectTimeout,
            readTimeout: configuration.readTimeout,
            waitsForConnectivity: configuration.waitsForConnectivity
        )
        self.httpClientPool = HTTPClientPool(
            configuration: httpConfiguration,
            size: configuration.poolSize
        )
        if configuration.autoScaleFetchConcurrency {
            let poolSize = max(1, configuration.poolSize)
            let target = poolSize * max(1, configuration.connectionsPerHost)
            self.maxConcurrentFetches = max(effectiveMaxConcurrentFetches, target)
        } else {
            self.maxConcurrentFetches = effectiveMaxConcurrentFetches
        }
        self.enableTaskMetricsLogging = configuration.enableTaskMetricsLogging
        self.httpClientRequestTimeout = .seconds(Int64(max(1, configuration.readTimeout)))
        self.maxConcurrentWrites = max(1, configuration.maxConcurrentWrites)
        self.maxConcurrentDecodes = max(1, configuration.maxConcurrentDecodes)
        self.maxInflightBuffers = max(1, configuration.maxInflightBuffers)
    }

    private func logDebug(_ message: @autoclosure () -> String) {
        guard enableTaskMetricsLogging else {
            return
        }
        print(message())
    }

    // MARK: - Public API

    /// Downloads a file and returns its contents as `Data`.
    ///
    /// - Parameters:
    ///   - fileID: The 64-character hex file identifier (Merkle hash).
    ///   - byteRange: Optional byte range for partial downloads.
    ///     The range is half-open: `start..<end`.
    ///     An empty range (where `lowerBound == upperBound`) returns
    ///     an empty `Data` immediately without making any network requests.
    ///
    /// - Returns: The file contents, or the requested byte range.
    ///
    /// - Throws: ``XetDownloaderError`` for protocol-level failures,
    ///   ``XorbError`` for malformed chunk data,
    ///   ``LZ4Error`` for decompression failures,
    ///   or `URLError` for network failures.
    ///
    /// - Important: This method loads the entire file (or range) into memory.
    ///   For large files, use ``download(_:byteRange:to:)``
    ///   to write directly to disk instead.
    public func data(
        for fileID: String,
        byteRange: Range<UInt64>? = nil
    ) async throws -> Data {
        if let byteRange, byteRange.isEmpty {
            return Data()
        }
        let writer = DataOutputWriter()
        _ = try await download(
            fileID: fileID,
            byteRange: byteRange,
            writer: writer
        )
        return await writer.data
    }

    /// Downloads a file and writes it to disk.
    ///
    /// - Parameters:
    ///   - fileID: The 64-character hex file identifier (Merkle hash).
    ///   - byteRange: Optional byte range for partial downloads.
    ///     The range is half-open: `start..<end`.
    ///     An empty range (where `lowerBound == upperBound`) creates
    ///     an empty file at the destination and returns `0` without
    ///     making any network requests.
    ///   - destinationURL: The file URL where contents will be written.
    ///     If a file exists at this path, it will be replaced.
    ///   - fileManager: The file manager to use for file operations.
    ///     Defaults to `.default`.
    ///
    /// - Returns: The number of bytes written.
    ///
    /// - Throws: ``XetDownloaderError`` for protocol-level failures,
    ///   ``XorbError`` for malformed chunk data,
    ///   ``LZ4Error`` for decompression failures,
    ///   `URLError` for network failures,
    ///   or file system errors if writing to disk fails.
    @discardableResult
    public func download(
        _ fileID: String,
        byteRange: Range<UInt64>? = nil,
        to destinationURL: URL,
        fileManager: FileManager = .default
    ) async throws -> Int64 {
        if fileManager.fileExists(atPath: destinationURL.path) {
            try fileManager.removeItem(at: destinationURL)
        }
        fileManager.createFile(atPath: destinationURL.path, contents: nil)

        if let byteRange, byteRange.isEmpty {
            return 0
        }
        let writer = try PwriteFileWriter(destinationURL: destinationURL)
        let written = try await download(
            fileID: fileID,
            byteRange: byteRange,
            writer: writer
        )
        try await writer.close()
        return written
    }

    // MARK: -

    /// Core download implementation that writes to any ``OutputWriter``.
    ///
    /// Processes reconstruction terms in order, fetching xorb data and
    /// decompressing chunks. Implements caching for xorbs referenced by
    /// multiple terms to avoid redundant downloads.
    private func download(
        fileID: String,
        byteRange: Range<UInt64>?,
        writer: some OutputWriter
    ) async throws -> Int64 {
        logDebug(
            "XET DEBUG: download start fileID=\(fileID) byteRange=\(byteRange.map { "\($0.lowerBound)..<\($0.upperBound)" } ?? "nil")"
        )
        // Validate file ID
        guard fileID.count == 64,
            fileID.allSatisfy({ $0.isHexDigit })
        else {
            throw XetDownloaderError.invalidFileID(fileID)
        }

        let conn = try await tokenProvider.connectionInfo(
            for: refreshURL,
            hubToken: hubToken
        )
        logDebug(
            "XET DEBUG: connection acquired casURL=\(conn.casURL.absoluteString) accessTokenPrefix=\(conn.accessToken.prefix(12))"
        )

        // Validate CAS URL uses HTTPS unless insecure connections are allowed
        if !allowsInsecureConnections && conn.casURL.scheme != "https" {
            throw XetDownloaderError.insecureURL(conn.casURL)
        }

        let reconstruction = try await casClient.reconstruction(
            of: fileID,
            casURL: conn.casURL,
            accessToken: conn.accessToken,
            byteRange: byteRange
        )
        logDebug(
            "XET DEBUG: reconstruction terms=\(reconstruction.terms.count) fetchGroups=\(reconstruction.fetchInfo.count) offsetIntoFirstRange=\(reconstruction.offsetIntoFirstRange)"
        )

        let maxBytesToWrite: UInt64? = byteRange.map { UInt64($0.count) }
        var remainingBytesToWrite = maxBytesToWrite

        var bytesToSkipInFirstTerm = reconstruction.offsetIntoFirstRange

        var xorbUsageCount: [String: Int] = [:]
        for term in reconstruction.terms {
            xorbUsageCount[term.hash, default: 0] += 1
        }

        var termContexts: [TermContext] = []
        termContexts.reserveCapacity(reconstruction.terms.count)
        var expectedUnpackedBytesByKey: [FetchRangeKey: Int] = [:]
        for term in reconstruction.terms {
            guard let fetchInfos = reconstruction.fetchInfo[term.hash] else {
                throw XetDownloaderError.invalidReconstruction
            }
            guard
                let fetchInfo = fetchInfos.first(where: {
                    $0.range.lowerBound <= term.range.lowerBound
                        && $0.range.upperBound >= term.range.upperBound
                })
            else {
                throw XetDownloaderError.invalidReconstruction
            }

            guard let fetchURL = URL(string: fetchInfo.url) else {
                throw XetDownloaderError.invalidFetchURL(fetchInfo.url)
            }
            logDebug("XET DEBUG: fetchURL=\(fetchURL.absoluteString)")

            // Validate fetch URL uses HTTPS unless insecure connections are allowed
            if !allowsInsecureConnections && fetchURL.scheme != "https" {
                throw XetDownloaderError.insecureURL(fetchURL)
            }

            var request = URLRequest(url: fetchURL)
            request.httpMethod = "GET"
            request.setValue(fetchInfo.urlRangeHeaderValue, forHTTPHeaderField: "Range")
            logDebug("XET DEBUG: request Range=\(fetchInfo.urlRangeHeaderValue)")

            let key = FetchRangeKey(
                hash: term.hash,
                start: fetchInfo.range.lowerBound,
                end: fetchInfo.range.upperBound,
                urlRangeStart: fetchInfo.urlRange.lowerBound,
                urlRangeEnd: fetchInfo.urlRange.upperBound
            )
            expectedUnpackedBytesByKey[key, default: 0] += Int(term.unpackedLength)

            termContexts.append(
                TermContext(
                    term: term,
                    fetchInfo: fetchInfo,
                    key: key,
                    request: request
                )
            )
        }

        var chunkCache: [FetchRangeKey: FetchedXorb] = [:]

        var totalWritten: Int64 = 0
        var writeOffset: Int64 = 0
        let fetchSemaphore = AsyncSemaphore(value: maxConcurrentFetches)
        var inflightFetches: [FetchRangeKey: Task<FetchedXorb, Error>] = [:]
        let randomAccessWriter = writer as? RandomAccessOutputWriter

        func termRange(from fetched: FetchedXorb, for term: CASClient.ReconstructionResponse.Term) throws -> Range<Int>
        {
            let startIndex = term.range.lowerBound - fetched.chunkRange.lowerBound
            let endIndex = term.range.upperBound - fetched.chunkRange.lowerBound
            guard startIndex >= 0, endIndex >= startIndex, endIndex < fetched.chunkByteIndices.count else {
                throw XetDownloaderError.invalidReconstruction
            }
            let startByte = fetched.chunkByteIndices[startIndex]
            let endByte = fetched.chunkByteIndices[endIndex]
            if startByte >= endByte {
                return startByte ..< startByte
            }
            return startByte ..< endByte
        }

        func writeTermData(base: Data, range: Range<Int>) async throws {
            var lower = range.lowerBound
            var upper = range.upperBound
            if lower >= upper {
                return
            }

            if bytesToSkipInFirstTerm > 0 {
                let available = upper - lower
                let skip = min(UInt64(available), bytesToSkipInFirstTerm)
                lower += Int(skip)
                bytesToSkipInFirstTerm -= skip
                if lower >= upper {
                    return
                }
            }

            if let remaining = remainingBytesToWrite {
                if remaining == 0 {
                    return
                }
                let available = upper - lower
                if UInt64(available) > remaining {
                    upper = lower + Int(remaining)
                }
                remainingBytesToWrite = remaining - UInt64(upper - lower)
            }

            let offset = writeOffset
            writeOffset += Int64(upper - lower)

            if let randomWriter = randomAccessWriter {
                try base.withUnsafeBytes { raw in
                    guard let baseAddress = raw.baseAddress else {
                        throw XetDownloaderError.invalidReconstruction
                    }
                    let start = baseAddress.advanced(by: lower)
                    let slice = UnsafeRawBufferPointer(start: start, count: upper - lower)
                    try randomWriter.writeRaw(slice, at: offset)
                }
            } else {
                let chunk = base.subdata(in: lower ..< upper)
                try await writer.write(chunk)
            }

            totalWritten += Int64(upper - lower)
        }

        func ensureFetchTask(for context: TermContext) {
            let term = context.term
            let key = context.key
            let shouldCacheAllForXorb = (xorbUsageCount[term.hash] ?? 0) > 1
            let expectedUnpackedLength = expectedUnpackedBytesByKey[key]

            if inflightFetches[key] != nil {
                return
            }
            if shouldCacheAllForXorb, chunkCache[key] != nil {
                return
            }

            inflightFetches[key] = Task {
                await fetchSemaphore.acquire()
                defer { Task { await fetchSemaphore.release() } }
                return try await fetchXorbChunks(
                    termHash: term.hash,
                    fetchInfo: context.fetchInfo,
                    request: context.request,
                    expectedUnpackedLength: expectedUnpackedLength
                )
            }
        }

        for (termIndex, context) in termContexts.enumerated() {
            let term = context.term
            let fetchInfo = context.fetchInfo
            let key = context.key
            if let remainingBytesToWrite, remainingBytesToWrite == 0 {
                logDebug("XET DEBUG: remainingBytesToWrite=0, stopping")
                break
            }

            if let cached = chunkCache[key] {
                logDebug(
                    "XET DEBUG: using cached chunks xorbHash=\(term.hash.prefix(12)) range=\(term.range.lowerBound)..<\(term.range.upperBound)"
                )
                let range = try termRange(from: cached, for: term)
                try await writeTermData(base: cached.data, range: range)
                continue
            }

            logDebug(
                "XET DEBUG: term xorbHash=\(term.hash.prefix(12)) termRange=\(term.range.lowerBound)..<\(term.range.upperBound) fetchRange=\(fetchInfo.range.lowerBound)..<\(fetchInfo.range.upperBound) urlRange=\(fetchInfo.urlRange.lowerBound)..<\(fetchInfo.urlRange.upperBound)"
            )

            let shouldCacheAllForXorb = (xorbUsageCount[term.hash] ?? 0) > 1
            let prefetchLimit = min(termContexts.count, termIndex + maxConcurrentFetches)
            for prefetchIndex in termIndex ..< prefetchLimit {
                ensureFetchTask(for: termContexts[prefetchIndex])
            }
            ensureFetchTask(for: context)
            guard let fetchTask = inflightFetches[key] else {
                continue
            }

            let fetchedChunks = try await fetchTask.value
            inflightFetches[key] = nil

            if shouldCacheAllForXorb {
                chunkCache[key] = fetchedChunks
            }
            let range = try termRange(from: fetchedChunks, for: term)
            try await writeTermData(base: fetchedChunks.data, range: range)
            logDebug("XET DEBUG: finished term write for xorbHash=\(term.hash.prefix(12))")
        }

        logDebug("XET DEBUG: download complete totalWritten=\(totalWritten)")
        return totalWritten
    }

    private func fetchXorbChunks(
        termHash: String,
        fetchInfo: CASClient.ReconstructionResponse.FetchInfo,
        request: URLRequest,
        expectedUnpackedLength: Int?
    ) async throws -> FetchedXorb {
        logDebug(
            "XET DEBUG: fetch start xorbHash=\(termHash.prefix(12)) range=\(fetchInfo.range.lowerBound)..<\(fetchInfo.range.upperBound)"
        )
        guard let url = request.url else {
            throw XetDownloaderError.fetchFailed(statusCode: nil, url: URL(fileURLWithPath: "/"))
        }
        let client = await httpClientPool.nextClient()
        var httpRequest = HTTPClientRequest(url: url.absoluteString)
        httpRequest.method = .GET
        if let headers = request.allHTTPHeaderFields {
            for (name, value) in headers {
                httpRequest.headers.add(name: name, value: value)
            }
        }
        let response = try await client.execute(
            httpRequest,
            timeout: httpClientRequestTimeout
        )
        let statusCode = Int(response.status.code)
        guard (200 ..< 300).contains(statusCode) || statusCode == 206 else {
            throw XetDownloaderError.fetchFailed(statusCode: statusCode, url: url)
        }
        logDebug(
            "XET DEBUG: response status=\(statusCode) version=\(response.version) contentLength=\(response.headers.first(name: "Content-Length") ?? "nil")"
        )
        let bufferSlots = max(2, min(maxInflightBuffers, maxConcurrentDecodes))
        let bufferSemaphore = AsyncSemaphore(value: bufferSlots)
        let stream = AsyncThrowingStream<ByteBuffer, Error> { continuation in
            let task = Task {
                do {
                    for try await buffer in response.body {
                        if buffer.readableBytes == 0 {
                            continue
                        }
                        await bufferSemaphore.acquire()
                        continuation.yield(buffer)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
        let decoded = try await decodeXorbStream(
            stream: stream,
            bufferSemaphore: bufferSemaphore,
            expectedUnpackedLength: expectedUnpackedLength
        )
        logDebug(
            "XET DEBUG: finished fetch loop for xorbHash=\(termHash.prefix(12)) chunks=\(decoded.chunkByteIndices.count - 1)"
        )
        return FetchedXorb(
            data: decoded.data,
            chunkByteIndices: decoded.chunkByteIndices,
            chunkRange: fetchInfo.range
        )
    }

    private func decodeXorbStream(
        stream: AsyncThrowingStream<ByteBuffer, Error>,
        bufferSemaphore: AsyncSemaphore,
        expectedUnpackedLength: Int?
    ) async throws -> (data: Data, chunkByteIndices: [Int]) {
        if let expectedUnpackedLength, expectedUnpackedLength > 0 {
            return try await decodeXorbStreamPipelined(
                stream: stream,
                bufferSemaphore: bufferSemaphore,
                totalOutputSize: expectedUnpackedLength
            )
        }

        var cursor = ByteCursor()
        var data = Data()
        var chunkByteIndices: [Int] = [0]

        func drainCursor(isEOF: Bool) throws {
            while true {
                if let uncompressed = try Xorb.decodeNextChunk(from: &cursor) {
                    data.append(uncompressed)
                    chunkByteIndices.append(data.count)
                    continue
                }
                if isEOF {
                    if cursor.count == 0 {
                        return
                    }
                    throw XorbError.truncatedStream
                }
                break
            }
        }

        for try await buffer in stream {
            if buffer.readableBytes > 0 {
                buffer.withUnsafeReadableBytes { raw in
                    cursor.append(raw)
                }
            }
            await bufferSemaphore.release()
            try drainCursor(isEOF: false)
        }

        try drainCursor(isEOF: true)
        return (data: data, chunkByteIndices: chunkByteIndices)
    }

    private func decodeXorbStreamPipelined(
        stream: AsyncThrowingStream<ByteBuffer, Error>,
        bufferSemaphore: AsyncSemaphore,
        totalOutputSize: Int
    ) async throws -> (data: Data, chunkByteIndices: [Int]) {
        let outputBuffer = UnsafeMutableRawPointer.allocate(
            byteCount: totalOutputSize,
            alignment: 16
        )
        let bufferWrapper = SendableBufferWrapper(baseAddress: outputBuffer, count: totalOutputSize)
        let resultCollector = DecodeResultCollector()

        let (jobStream, jobContinuation) = AsyncStream<(DecodeJob, Int)>.makeStream(
            bufferingPolicy: .bufferingNewest(maxConcurrentDecodes * 2)
        )

        var cursor = ByteCursor()
        var chunkIndex = 0
        var writeOffset = 0

        async let workersDone: Void = withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0 ..< self.maxConcurrentDecodes {
                group.addTask { [bufferWrapper, resultCollector] in
                    for await (job, offset) in jobStream {
                        do {
                            let outputSlice = UnsafeMutableRawBufferPointer(
                                start: bufferWrapper.baseAddress.advanced(by: offset),
                                count: job.uncompressedLength
                            )
                            try Self.decodeChunkInto(job: job, output: outputSlice)
                            await resultCollector.recordSuccess(
                                index: job.index,
                                endOffset: offset + job.uncompressedLength
                            )
                        } catch {
                            await resultCollector.recordError(error)
                        }
                    }
                }
            }
            for try await _ in group {}
        }

        for try await buffer in stream {
            if buffer.readableBytes > 0 {
                buffer.withUnsafeReadableBytes { raw in
                    cursor.append(raw)
                }
            }
            await bufferSemaphore.release()

            while let job = try extractNextDecodeJob(from: &cursor, chunkIndex: chunkIndex) {
                let jobWriteOffset = writeOffset
                writeOffset += job.uncompressedLength
                chunkIndex += 1
                jobContinuation.yield((job, jobWriteOffset))
            }
        }

        while let job = try extractNextDecodeJob(from: &cursor, chunkIndex: chunkIndex, isEOF: true) {
            let jobWriteOffset = writeOffset
            writeOffset += job.uncompressedLength
            chunkIndex += 1
            jobContinuation.yield((job, jobWriteOffset))
        }

        if cursor.count > 0 {
            outputBuffer.deallocate()
            throw XorbError.truncatedStream
        }

        jobContinuation.finish()
        try await workersDone

        if let error = await resultCollector.firstError {
            outputBuffer.deallocate()
            throw error
        }

        var chunkByteIndices: [Int] = [0]
        let sortedOffsets = await resultCollector.sortedEndOffsets
        for offset in sortedOffsets {
            chunkByteIndices.append(offset)
        }

        let data = Data(
            bytesNoCopy: outputBuffer,
            count: writeOffset,
            deallocator: .custom { ptr, _ in ptr.deallocate() }
        )
        return (data: data, chunkByteIndices: chunkByteIndices)
    }

    private struct SendableBufferWrapper: @unchecked Sendable {
        let baseAddress: UnsafeMutableRawPointer
        let count: Int
    }

    private struct DecodeJob: Sendable {
        let index: Int
        let compressedData: Data
        let compressionScheme: Xorb.CompressionScheme
        let uncompressedLength: Int
    }

    private func extractNextDecodeJob(
        from cursor: inout ByteCursor,
        chunkIndex: Int,
        isEOF: Bool = false
    ) throws -> DecodeJob? {
        guard cursor.count >= 8 else {
            if isEOF && cursor.count > 0 {
                throw XorbError.truncatedStream
            }
            return nil
        }

        guard let headerBytes = cursor.peek(count: 8) else {
            return nil
        }

        let header = try headerBytes.withUnsafeBytes { raw in
            try Xorb.parseHeader(raw)
        }

        let totalLength = 8 + header.compressedLength
        guard cursor.count >= totalLength else {
            if isEOF {
                throw XorbError.truncatedStream
            }
            return nil
        }

        _ = cursor.skip(count: 8)
        guard let payload = cursor.take(count: header.compressedLength) else {
            throw XorbError.truncatedStream
        }

        return DecodeJob(
            index: chunkIndex,
            compressedData: payload,
            compressionScheme: header.compressionScheme,
            uncompressedLength: header.uncompressedLength
        )
    }

    private static func decodeChunkInto(job: DecodeJob, output: UnsafeMutableRawBufferPointer) throws {
        switch job.compressionScheme {
        case .none:
            guard job.compressedData.count == job.uncompressedLength else {
                throw XorbError.lengthMismatch(
                    expected: job.uncompressedLength,
                    actual: job.compressedData.count
                )
            }
            job.compressedData.withUnsafeBytes { src in
                if let srcBase = src.baseAddress, let dstBase = output.baseAddress {
                    memcpy(dstBase, srcBase, job.compressedData.count)
                }
            }

        case .lz4:
            try job.compressedData.withUnsafeBytes { compressed in
                _ = try LZ4.decompressBlockInto(
                    compressed,
                    uncompressedLength: job.uncompressedLength,
                    output: output
                )
            }

        case .byteGrouping4LZ4:
            let scratchSize = job.uncompressedLength
            let scratch = UnsafeMutableRawBufferPointer.allocate(byteCount: scratchSize, alignment: 16)
            defer { scratch.deallocate() }

            try job.compressedData.withUnsafeBytes { compressed in
                _ = try LZ4.decompressBlockInto(
                    compressed,
                    uncompressedLength: job.uncompressedLength,
                    output: scratch
                )
            }

            let scratchRead = UnsafeRawBufferPointer(scratch)
            BG4.regroupInto(from: scratchRead, to: output)
        }
    }

    private struct TermContext {
        let term: CASClient.ReconstructionResponse.Term
        let fetchInfo: CASClient.ReconstructionResponse.FetchInfo
        let key: FetchRangeKey
        let request: URLRequest
    }
}

private actor AsyncSemaphore {
    private var available: Int
    private var waiters: [CheckedContinuation<Void, Never>] = []

    init(value: Int) {
        self.available = max(0, value)
    }

    func acquire() async {
        if available > 0 {
            available -= 1
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release() {
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.resume()
        } else {
            available += 1
        }
    }
}

private actor DecodeResultCollector {
    private var endOffsets: [(index: Int, offset: Int)] = []
    private(set) var firstError: Error?
    private var completedCount = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var expectedCount: Int?

    func recordSuccess(index: Int, endOffset: Int) {
        endOffsets.append((index: index, offset: endOffset))
        completedCount += 1
        checkCompletion()
    }

    func recordError(_ error: Error) {
        if firstError == nil {
            firstError = error
        }
        completedCount += 1
        checkCompletion()
    }

    func waitForAll(count: Int) async {
        if completedCount >= count {
            return
        }
        expectedCount = count
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
            checkCompletion()
        }
    }

    private func checkCompletion() {
        guard let expected = expectedCount, completedCount >= expected else { return }
        for waiter in waiters {
            waiter.resume()
        }
        waiters.removeAll()
    }

    var sortedEndOffsets: [Int] {
        endOffsets.sorted { $0.index < $1.index }.map(\.offset)
    }
}

private actor HTTPClientPool {
    private let clients: [HTTPClient]
    private var nextIndex = 0

    init(configuration: HTTPClient.Configuration, size: Int) {
        let poolSize = max(1, size)
        var created: [HTTPClient] = []
        created.reserveCapacity(poolSize)
        for _ in 0 ..< poolSize {
            created.append(
                HTTPClient(
                    eventLoopGroupProvider: .singleton,
                    configuration: configuration
                )
            )
        }
        self.clients = created
    }

    func nextClient() -> HTTPClient {
        let client = clients[nextIndex]
        nextIndex = (nextIndex + 1) % clients.count
        return client
    }

    func shutdown() async throws {
        for client in clients {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                client.shutdown(queue: .global()) { error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }
}

// MARK: - XetDownloaderError

/// Errors that can occur during Xet file downloads.
public enum XetDownloaderError: Error, Sendable {
    /// The token refresh request returned an invalid response.
    case invalidTokenResponse

    /// The token refresh request failed with an HTTP error.
    case tokenRequestFailed(statusCode: Int, body: Data)

    /// The CAS URL in the token response could not be parsed.
    case invalidCASURL(String)

    /// The CAS reconstruction request returned an invalid response.
    case invalidReconstructionResponse

    /// The CAS reconstruction request failed with an HTTP error.
    case reconstructionRequestFailed(statusCode: Int, body: Data)

    /// Failed to decode the reconstruction response JSON.
    case reconstructionDecodingFailed(Error)

    /// The reconstruction response is malformed or missing required fetch info.
    case invalidReconstruction

    /// The HTTP request to fetch xorb data failed.
    case fetchFailed(statusCode: Int?, url: URL)

    /// The fetch info URL could not be parsed.
    case invalidFetchURL(String)

    /// The file ID is not a valid 64-character hex string.
    case invalidFileID(String)

    /// A URL does not use HTTPS and insecure connections are not allowed.
    case insecureURL(URL)
}

extension XetDownloaderError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .invalidTokenResponse:
            return "Token endpoint returned an invalid response."
        case let .tokenRequestFailed(statusCode, _):
            return "Token request failed with HTTP status \(statusCode)."
        case let .invalidCASURL(url):
            return "Invalid or insecure CAS URL: \(url)"
        case .invalidReconstructionResponse:
            return "Reconstruction endpoint returned an invalid response."
        case let .reconstructionRequestFailed(statusCode, _):
            return "Reconstruction request failed with HTTP status \(statusCode)."
        case let .reconstructionDecodingFailed(error):
            return "Failed to decode reconstruction response: \(error.localizedDescription)"
        case .invalidReconstruction:
            return "Reconstruction response is malformed or missing required data."
        case let .fetchFailed(statusCode, url):
            if let code = statusCode {
                return "Failed to fetch xorb data from \(url.host ?? "unknown"): HTTP \(code)"
            }
            return "Failed to fetch xorb data from \(url.host ?? "unknown")."
        case let .invalidFetchURL(url):
            return "Invalid fetch URL: \(url)"
        case let .invalidFileID(id):
            return "Invalid file ID (expected 64 hex characters): \(id.prefix(20))..."
        case let .insecureURL(url):
            return "Insecure URL not allowed: \(url). Set allowsInsecureConnections to true for local development."
        }
    }
}

// MARK: - TokenProvider

extension XetDownloader {
    /// Manages CAS access tokens with caching and coalesced refresh.
    ///
    /// Tokens are cached by refresh URL and Hub token combination.
    /// Concurrent requests for the same token are coalesced into a single
    /// network request.
    actor TokenProvider {
        private let urlSession: URLSession
        private let safetyWindow: TimeInterval
        private let enableDebugLogging: Bool

        private struct CacheKey: Hashable {
            let refreshURL: URL
            let hubToken: String?
        }

        private var cache: [CacheKey: ConnectionInfo] = [:]
        private var inflight: [CacheKey: Task<ConnectionInfo, Swift.Error>] = [:]

        /// Creates a token provider.
        ///
        /// - Parameters:
        ///   - urlSession: The URL session for token requests.
        ///   - safetyWindow: Seconds before expiration to consider a token stale.
        ///     Defaults to 60 seconds.
        init(
            urlSession: URLSession = .shared,
            safetyWindow: TimeInterval = 60,
            enableDebugLogging: Bool = false
        ) {
            self.urlSession = urlSession
            self.safetyWindow = safetyWindow
            self.enableDebugLogging = enableDebugLogging
        }

        /// Obtains CAS connection info, using cached tokens when valid.
        ///
        /// - Parameters:
        ///   - refreshURL: The Hugging Face Hub token endpoint.
        ///   - hubToken: Optional Hub authentication token.
        ///
        /// - Returns: Connection info with CAS URL and access token.
        func connectionInfo(for refreshURL: URL, hubToken: String?) async throws -> ConnectionInfo {
            let key = CacheKey(refreshURL: refreshURL, hubToken: hubToken)

            if let cached = cache[key], cached.expiresAt > Date().addingTimeInterval(safetyWindow) {
                if enableDebugLogging {
                    print(
                        "XET DEBUG: token cache hit refreshURL=\(refreshURL.absoluteString) expiresAt=\(cached.expiresAt)"
                    )
                }
                return cached
            }

            if let existing = inflight[key] {
                if enableDebugLogging {
                    print("XET DEBUG: token request already inflight refreshURL=\(refreshURL.absoluteString)")
                }
                return try await existing.value
            }

            let debugLogging = enableDebugLogging
            let task = Task { [urlSession] () throws -> ConnectionInfo in
                if debugLogging {
                    print("XET DEBUG: token request start refreshURL=\(refreshURL.absoluteString)")
                }
                var request = URLRequest(url: refreshURL)
                request.httpMethod = "GET"
                request.cachePolicy = .reloadIgnoringLocalCacheData
                if let hubToken {
                    request.setValue("Bearer \(hubToken)", forHTTPHeaderField: "Authorization")
                    if debugLogging {
                        print("XET DEBUG: token request has auth header tokenPrefix=\(hubToken.prefix(8))")
                    }
                }

                let (data, response) = try await urlSession.data(for: request)
                guard let http = response as? HTTPURLResponse else {
                    if debugLogging {
                        print("XET DEBUG: token response not HTTPURLResponse")
                    }
                    throw XetDownloaderError.invalidTokenResponse
                }
                if debugLogging {
                    print("XET DEBUG: token response status=\(http.statusCode) bytes=\(data.count)")
                }
                guard (200 ..< 300).contains(http.statusCode) else {
                    throw XetDownloaderError.tokenRequestFailed(
                        statusCode: http.statusCode,
                        body: data
                    )
                }

                let decoded: TokenResponse
                do {
                    decoded = try JSONDecoder().decode(TokenResponse.self, from: data)
                } catch {
                    if debugLogging {
                        print("XET DEBUG: token decode failed error=\(error.localizedDescription)")
                    }
                    throw XetDownloaderError.invalidTokenResponse
                }
                guard let casURL = URL(string: decoded.casUrl) else {
                    throw XetDownloaderError.invalidCASURL(decoded.casUrl)
                }

                let expiresAt = Date(timeIntervalSince1970: TimeInterval(decoded.exp))
                if debugLogging {
                    print(
                        "XET DEBUG: token decoded casURL=\(casURL.absoluteString) expiresAt=\(expiresAt) accessTokenPrefix=\(decoded.accessToken.prefix(12))"
                    )
                }
                return ConnectionInfo(
                    casURL: casURL,
                    accessToken: decoded.accessToken,
                    expiresAt: expiresAt
                )
            }

            inflight[key] = task
            do {
                let value = try await task.value
                inflight[key] = nil
                cache[key] = value
                return value
            } catch {
                inflight[key] = nil
                throw error
            }
        }
    }

    /// CAS connection details obtained from the Hub token endpoint.
    struct ConnectionInfo: Equatable, Sendable {
        /// The CAS API base URL.
        let casURL: URL

        /// The bearer token for CAS API authentication.
        let accessToken: String

        /// When the access token expires.
        let expiresAt: Date
    }

    /// JSON response from the Hub token endpoint.
    private struct TokenResponse: Equatable, Codable, Sendable {
        let accessToken: String
        let exp: Int
        let casUrl: String
    }
}

// MARK: - Private Helpers

extension XetDownloader {
    private static func makeHTTPClientConfiguration(
        connectionsPerHost: Int,
        prewarmedConnections: Int,
        forceHTTP1: Bool,
        connectTimeout: TimeInterval,
        readTimeout: TimeInterval,
        waitsForConnectivity: Bool
    ) -> HTTPClient.Configuration {
        var configuration = HTTPClient.Configuration()
        configuration.httpVersion = forceHTTP1 ? .http1Only : .automatic
        configuration.timeout = .init(
            connect: .seconds(Int64(connectTimeout)),
            read: .seconds(Int64(readTimeout))
        )
        configuration.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit = max(
            1,
            connectionsPerHost
        )
        configuration.connectionPool.idleTimeout = .seconds(120)
        configuration.connectionPool.preWarmedHTTP1ConnectionCount = max(
            0,
            min(prewarmedConnections, connectionsPerHost)
        )
        configuration.networkFrameworkWaitForConnectivity = waitsForConnectivity
        configuration.enableMultipath = true
        return configuration
    }
}

// MARK: - HTTPClient Lifecycle

extension XetDownloader {
    /// Shuts down the internal HTTP client pool.
    ///
    /// Call this when you are done with the downloader to release resources.
    public func shutdown() async throws {
        try await httpClientPool.shutdown()
    }
}

// MARK: - Lifecycle Helpers

extension XetDownloader {
    /// Creates a downloader for the duration of the closure, then shuts it down.
    public static func withDownloader<T>(
        refreshURL: URL,
        hubToken: String? = nil,
        configuration: Configuration = .highThroughput,
        _ body: (XetDownloader) async throws -> T
    ) async throws -> T {
        let downloader = XetDownloader(
            refreshURL: refreshURL,
            hubToken: hubToken,
            configuration: configuration
        )
        do {
            let result = try await body(downloader)
            try await downloader.shutdown()
            return result
        } catch {
            try? await downloader.shutdown()
            throw error
        }
    }
}

/// Key for tracking which fetch ranges have been downloaded.
private struct FetchRangeKey: Hashable {
    let hash: String
    let start: Int
    let end: Int
    let urlRangeStart: UInt64
    let urlRangeEnd: UInt64
}

private struct FetchedXorb {
    let data: Data
    let chunkByteIndices: [Int]
    let chunkRange: Range<Int>
}

/// A destination for writing downloaded chunk data.
protocol OutputWriter: Sendable {
    func write(_ data: Data) async throws
}

protocol RandomAccessOutputWriter: OutputWriter {
    func write(_ data: Data, at offset: Int64) async throws
    func writeRaw(_ buffer: UnsafeRawBufferPointer, at offset: Int64) throws
    func close() async throws
}

/// An in-memory output writer that accumulates data.
actor DataOutputWriter: OutputWriter {
    private(set) var data = Data()

    func write(_ data: Data) async throws {
        self.data.append(data)
    }
}

/// An output writer that writes to a file.
actor FileOutputWriter: OutputWriter {
    private let handle: FileHandle

    init(destinationURL: URL) throws {
        self.handle = try FileHandle(forWritingTo: destinationURL)
    }

    func write(_ data: Data) async throws {
        try handle.write(contentsOf: data)
    }

    func close() throws {
        try handle.close()
    }
}

/// A random access output writer backed by POSIX pwrite.
final class PwriteFileWriter: RandomAccessOutputWriter {
    private let fd: Int32

    init(destinationURL: URL) throws {
        let fm = FileManager.default
        if fm.fileExists(atPath: destinationURL.path) {
            try fm.removeItem(at: destinationURL)
        }
        fm.createFile(atPath: destinationURL.path, contents: nil)
        let flags = O_CREAT | O_RDWR | O_TRUNC
        let mode: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
        let fd = open(destinationURL.path, flags, mode)
        if fd < 0 {
            throw POSIXError(POSIXError.Code(rawValue: errno) ?? .EIO)
        }
        self.fd = fd
    }

    func write(_ data: Data) async throws {
        try await write(data, at: 0)
    }

    func write(_ data: Data, at offset: Int64) async throws {
        try data.withUnsafeBytes { rawBuffer in
            try writeRaw(rawBuffer, at: offset)
        }
    }

    func writeRaw(_ buffer: UnsafeRawBufferPointer, at offset: Int64) throws {
        if buffer.count == 0 {
            return
        }
        guard let baseAddress = buffer.baseAddress else {
            return
        }
        var bytesRemaining = buffer.count
        var localOffset = 0
        while bytesRemaining > 0 {
            let writeSize = bytesRemaining
            let written = pwrite(
                fd,
                baseAddress.advanced(by: localOffset),
                writeSize,
                off_t(offset + Int64(localOffset))
            )
            if written < 0 {
                throw POSIXError(POSIXError.Code(rawValue: errno) ?? .EIO)
            }
            bytesRemaining -= written
            localOffset += written
        }
    }

    func close() async throws {
        if Darwin.close(fd) != 0 {
            throw POSIXError(POSIXError.Code(rawValue: errno) ?? .EIO)
        }
    }
}
