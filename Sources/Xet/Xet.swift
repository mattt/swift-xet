import AsyncHTTPClient
import Darwin
import Foundation
import NIOCore
import NIOHTTP1
import os

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
                do {
                    let result = try await fetchXorbChunks(
                        termHash: term.hash,
                        fetchInfo: context.fetchInfo,
                        request: context.request,
                        expectedUnpackedLength: expectedUnpackedLength
                    )
                    await fetchSemaphore.release()
                    return result
                } catch {
                    await fetchSemaphore.release()
                    throw error
                }
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
        let contentLength = response.headers.first(name: "Content-Length").flatMap(Int.init)
        logDebug(
            "XET DEBUG: response status=\(statusCode) version=\(response.version) contentLength=\(contentLength.map(String.init) ?? "nil")"
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
            expectedUnpackedLength: expectedUnpackedLength,
            expectedCompressedLength: contentLength
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
        expectedUnpackedLength: Int?,
        expectedCompressedLength: Int?
    ) async throws -> (data: Data, chunkByteIndices: [Int]) {
        var buffer = ByteBuffer()
        if let capacity = expectedCompressedLength {
            buffer.reserveCapacity(capacity)
        }
        var chunkByteIndices: [Int] = [0]

        let useDirectWrite = expectedUnpackedLength != nil && expectedUnpackedLength! > 0

        if useDirectWrite {
            let totalSize = expectedUnpackedLength!
            guard let outputBase = malloc(totalSize) else {
                throw XorbError.decompressionFailed
            }
            let outputBuffer = UnsafeMutableRawBufferPointer(start: outputBase, count: totalSize)

            var scratchBuffer: UnsafeMutableRawBufferPointer?
            var writeOffset = 0

            func ensureScratch(size: Int) {
                if scratchBuffer == nil || scratchBuffer!.count < size {
                    scratchBuffer?.deallocate()
                    scratchBuffer = UnsafeMutableRawBufferPointer.allocate(
                        byteCount: size,
                        alignment: 8
                    )
                }
            }

            func drainBuffer(isEOF: Bool) throws {
                while true {
                    let peekedScheme = buffer.withUnsafeReadableBytes { raw -> UInt8? in
                        guard raw.count >= 8 else { return nil }
                        return raw[4]
                    }
                    if let scheme = peekedScheme, scheme == 2 {
                        let uncompressedLen = buffer.withUnsafeReadableBytes { raw -> Int in
                            guard raw.count >= 8 else { return 64 * 1024 }
                            return Int(raw[5]) | (Int(raw[6]) << 8) | (Int(raw[7]) << 16)
                        }
                        ensureScratch(size: uncompressedLen)
                    }
                    if let bytesWritten = try Xorb.decodeNextChunk(
                        from: &buffer,
                        into: outputBuffer,
                        at: writeOffset,
                        scratch: scratchBuffer
                    ) {
                        writeOffset += bytesWritten
                        chunkByteIndices.append(writeOffset)
                        continue
                    }
                    if isEOF {
                        if buffer.readableBytes == 0 {
                            return
                        }
                        throw XorbError.truncatedStream
                    }
                    break
                }
            }

            do {
                for try await var incoming in stream {
                    if incoming.readableBytes > 0 {
                        buffer.writeBuffer(&incoming)
                    }
                    await bufferSemaphore.release()
                    try drainBuffer(isEOF: false)
                }

                try drainBuffer(isEOF: true)
            } catch {
                free(outputBase)
                scratchBuffer?.deallocate()
                throw error
            }

            scratchBuffer?.deallocate()

            let data = Data(bytesNoCopy: outputBase, count: writeOffset, deallocator: .free)
            return (data: data, chunkByteIndices: chunkByteIndices)
        }

        var data = Data()

        func drainBuffer(isEOF: Bool) throws {
            while true {
                if let uncompressed = try Xorb.decodeNextChunk(from: &buffer) {
                    data.append(uncompressed)
                    chunkByteIndices.append(data.count)
                    continue
                }
                if isEOF {
                    if buffer.readableBytes == 0 {
                        return
                    }
                    throw XorbError.truncatedStream
                }
                break
            }
        }

        for try await var incoming in stream {
            if incoming.readableBytes > 0 {
                buffer.writeBuffer(&incoming)
            }
            await bufferSemaphore.release()
            try drainBuffer(isEOF: false)
        }

        try drainBuffer(isEOF: true)
        return (data: data, chunkByteIndices: chunkByteIndices)
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

/// A sequential output writer that batches writes using vectorized I/O (writev).
///
/// This writer accumulates buffers and flushes them in batches using the `writev`
/// syscall, reducing syscall overhead compared to individual write calls.
/// Suitable for sequential writes where data is appended in order.
final class WritevFileWriter: RandomAccessOutputWriter, @unchecked Sendable {
    private let fd: Int32
    private let flushThreshold: Int
    private var pendingBuffers: [Data] = []
    private var pendingSize: Int = 0
    private var currentOffset: Int64 = 0
    private let lock = NSLock()

    init(destinationURL: URL, flushThreshold: Int = 1024 * 1024) throws {
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
        self.flushThreshold = flushThreshold
    }

    func write(_ data: Data) async throws {
        try lock.withLock {
            if data.isEmpty { return }
            pendingBuffers.append(data)
            pendingSize += data.count
            if pendingSize >= flushThreshold {
                try flushLocked()
            }
        }
    }

    func write(_ data: Data, at offset: Int64) async throws {
        try lock.withLock {
            if !pendingBuffers.isEmpty {
                try flushLocked()
            }
            currentOffset = offset
            if data.isEmpty { return }
            pendingBuffers.append(data)
            pendingSize += data.count
            if pendingSize >= flushThreshold {
                try flushLocked()
            }
        }
    }

    func writeRaw(_ buffer: UnsafeRawBufferPointer, at offset: Int64) throws {
        try lock.withLock {
            if !pendingBuffers.isEmpty {
                try flushLocked()
            }
            currentOffset = offset
            if buffer.count == 0 { return }
            guard let base = buffer.baseAddress else { return }
            var remaining = buffer.count
            var localOffset = 0
            while remaining > 0 {
                let written = pwrite(fd, base.advanced(by: localOffset), remaining, off_t(currentOffset))
                if written < 0 {
                    let err = errno
                    if err == EINTR { continue }
                    throw POSIXError(POSIXError.Code(rawValue: err) ?? .EIO)
                }
                remaining -= written
                localOffset += written
                currentOffset += Int64(written)
            }
        }
    }

    func flush() throws {
        try lock.withLock {
            try flushLocked()
        }
    }

    private func flushLocked() throws {
        guard !pendingBuffers.isEmpty else { return }

        let bufferCount = pendingBuffers.count
        var iovecs = [iovec](repeating: iovec(), count: bufferCount)

        _ = pendingBuffers.withContiguousStorageIfAvailable { bufferPtrs in
            for i in 0..<bufferCount {
                bufferPtrs[i].withUnsafeBytes { rawBuffer in
                    guard let baseAddress = rawBuffer.baseAddress else { return }
                    iovecs[i] = iovec(
                        iov_base: UnsafeMutableRawPointer(mutating: baseAddress),
                        iov_len: rawBuffer.count
                    )
                }
            }
        }

        if iovecs.contains(where: { $0.iov_base == nil }) {
            for i in 0..<bufferCount {
                pendingBuffers[i].withUnsafeBytes { rawBuffer in
                    guard let baseAddress = rawBuffer.baseAddress else { return }
                    iovecs[i] = iovec(
                        iov_base: UnsafeMutableRawPointer(mutating: baseAddress),
                        iov_len: rawBuffer.count
                    )
                }
            }
        }

        try writevAll(fd: fd, iovecs: &iovecs)

        let bytesWritten = pendingSize
        currentOffset += Int64(bytesWritten)
        pendingBuffers.removeAll(keepingCapacity: true)
        pendingSize = 0
    }

    private func writevAll(fd: Int32, iovecs: inout [iovec]) throws {
        var remaining = iovecs.count
        var startIndex = 0

        while remaining > 0 {
            let count = min(remaining, Int(IOV_MAX))
            let written = iovecs.withUnsafeMutableBufferPointer { ptr in
                Darwin.writev(fd, ptr.baseAddress! + startIndex, Int32(count))
            }

            if written < 0 {
                let err = errno
                if err == EINTR {
                    continue
                }
                throw POSIXError(POSIXError.Code(rawValue: err) ?? .EIO)
            }

            var bytesWritten = written
            while bytesWritten > 0 && startIndex < iovecs.count {
                let currentLen = iovecs[startIndex].iov_len
                if bytesWritten >= currentLen {
                    bytesWritten -= currentLen
                    startIndex += 1
                    remaining -= 1
                } else {
                    iovecs[startIndex].iov_base = iovecs[startIndex].iov_base?.advanced(by: Int(bytesWritten))
                    iovecs[startIndex].iov_len -= Int(bytesWritten)
                    bytesWritten = 0
                }
            }
        }
    }

    func close() async throws {
        try flush()
        if Darwin.close(fd) != 0 {
            throw POSIXError(POSIXError.Code(rawValue: errno) ?? .EIO)
        }
    }
}
