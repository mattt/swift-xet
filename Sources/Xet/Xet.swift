import Foundation

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
    private let urlSession: URLSession
    private let fetchSessionPool: SessionPool
    private let maxConcurrentFetches: Int

    /// Whether to allow insecure (non-HTTPS) connections.
    ///
    /// By default, the downloader requires HTTPS for all CAS and fetch URLs.
    /// Set this to `true` only for local development or testing with
    /// non-production servers.
    ///
    /// - Warning: Enabling insecure connections in production is a security risk.
    ///   Tokens and file contents may be transmitted in plaintext.
    public var allowsInsecureConnections: Bool = false

    /// Creates a downloader configured for a specific repository.
    ///
    /// - Parameters:
    ///   - refreshURL: The Hugging Face Hub URL for obtaining CAS tokens.
    ///     Format: `https://huggingface.co/api/{type}s/{repo}/xet-read-token/{ref}`
    ///   - hubToken: Optional Hugging Face Hub authentication token.
    ///     Required for private repositories.
    ///   - urlSession: The URL session for network requests.
    ///     Defaults to `.shared`.
    ///   - maxConcurrentFetches: Maximum concurrent xorb fetches.
    ///     Defaults to 100 (matches xet-core high performance mode).
    ///   - httpMaximumConnectionsPerHost: Maximum connections per host
    ///     for download sessions. Defaults to 24.
    ///   - sessionPoolSize: Number of download sessions to shard requests
    ///     across. Defaults to 4.
    ///   - autoScaleFetchConcurrency: When enabled, scales the fetch
    ///     concurrency to at least `sessionPoolSize * httpMaximumConnectionsPerHost`.
    ///     Defaults to true.
    ///   - enableTaskMetricsLogging: Enables URLSession task metrics logging
    ///     for fetch requests. Defaults to false.
    ///   - waitsForConnectivity: Whether to wait for connectivity rather than
    ///     failing immediately. Defaults to true.
    ///   - timeoutIntervalForRequest: Timeout per request in seconds.
    ///     Defaults to 120.
    public init(
        refreshURL: URL,
        hubToken: String? = nil,
        urlSession: URLSession = .shared,
        maxConcurrentFetches: Int = 100,
        httpMaximumConnectionsPerHost: Int = 24,
        sessionPoolSize: Int = 4,
        autoScaleFetchConcurrency: Bool = true,
        enableTaskMetricsLogging: Bool = false,
        waitsForConnectivity: Bool = true,
        timeoutIntervalForRequest: TimeInterval = 120
    ) {
        self.refreshURL = refreshURL
        self.hubToken = hubToken
        self.urlSession = urlSession
        self.tokenProvider = TokenProvider(urlSession: urlSession)
        self.casClient = CASClient(urlSession: urlSession)
        let effectiveMaxConcurrentFetches = max(1, maxConcurrentFetches)
        let configuration = XetDownloader.makeDownloadConfiguration(
            httpMaximumConnectionsPerHost: httpMaximumConnectionsPerHost,
            waitsForConnectivity: waitsForConnectivity,
            timeoutIntervalForRequest: timeoutIntervalForRequest
        )
        let metricsLogger = enableTaskMetricsLogging ? TaskMetricsLogger() : nil
        self.fetchSessionPool = SessionPool(
            configuration: configuration,
            size: sessionPoolSize,
            delegate: metricsLogger
        )
        if autoScaleFetchConcurrency {
            let poolSize = max(1, sessionPoolSize)
            let target = poolSize * max(1, httpMaximumConnectionsPerHost)
            self.maxConcurrentFetches = max(effectiveMaxConcurrentFetches, target)
        } else {
            self.maxConcurrentFetches = effectiveMaxConcurrentFetches
        }
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
        let writer = try FileOutputWriter(destinationURL: destinationURL)
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
        print(
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
        print(
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
        print(
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
            print("XET DEBUG: fetchURL=\(fetchURL.absoluteString)")

            // Validate fetch URL uses HTTPS unless insecure connections are allowed
            if !allowsInsecureConnections && fetchURL.scheme != "https" {
                throw XetDownloaderError.insecureURL(fetchURL)
            }

            var request = URLRequest(url: fetchURL)
            request.httpMethod = "GET"
            request.setValue(fetchInfo.urlRangeHeaderValue, forHTTPHeaderField: "Range")
            print("XET DEBUG: request Range=\(fetchInfo.urlRangeHeaderValue)")

            let key = FetchRangeKey(
                hash: term.hash,
                start: fetchInfo.range.lowerBound,
                end: fetchInfo.range.upperBound,
                urlRangeStart: fetchInfo.urlRange.lowerBound,
                urlRangeEnd: fetchInfo.urlRange.upperBound
            )

            termContexts.append(
                TermContext(
                    term: term,
                    fetchInfo: fetchInfo,
                    key: key,
                    request: request
                )
            )
        }

        var chunkCache: [String: [Int: Data]] = [:]

        var fetchedFetchRanges: Set<FetchRangeKey> = []

        var totalWritten: Int64 = 0
        let fetchSemaphore = AsyncSemaphore(value: maxConcurrentFetches)
        var inflightFetches: [FetchRangeKey: Task<[Int: Data], Error>] = [:]

        func ensureFetchTask(for context: TermContext) {
            let term = context.term
            let key = context.key
            let shouldCacheAllForXorb = (xorbUsageCount[term.hash] ?? 0) > 1

            if inflightFetches[key] != nil {
                return
            }
            if fetchedFetchRanges.contains(key), shouldCacheAllForXorb {
                return
            }
            if shouldCacheAllForXorb, let cached = chunkCache[term.hash] {
                var allPresent = true
                for idx in context.fetchInfo.range {
                    if cached[idx] == nil {
                        allPresent = false
                        break
                    }
                }
                if allPresent {
                    return
                }
            }

            inflightFetches[key] = Task {
                await fetchSemaphore.acquire()
                defer { Task { await fetchSemaphore.release() } }
                return try await fetchXorbChunks(
                    termHash: term.hash,
                    fetchInfo: context.fetchInfo,
                    request: context.request
                )
            }
        }

        for (termIndex, context) in termContexts.enumerated() {
            let term = context.term
            let fetchInfo = context.fetchInfo
            let key = context.key
            if let remainingBytesToWrite, remainingBytesToWrite == 0 {
                print("XET DEBUG: remainingBytesToWrite=0, stopping")
                break
            }

            if let cached = chunkCache[term.hash] {
                var allPresent = true
                for idx in term.range {
                    if cached[idx] == nil {
                        allPresent = false
                        break
                    }
                }
                if allPresent {
                    print(
                        "XET DEBUG: using cached chunks xorbHash=\(term.hash.prefix(12)) range=\(term.range.lowerBound)..<\(term.range.upperBound)"
                    )
                    for idx in term.range {
                        guard var chunk = cached[idx] else { continue }
                        if bytesToSkipInFirstTerm > 0 {
                            let skip = min(UInt64(chunk.count), bytesToSkipInFirstTerm)
                            chunk = chunk.dropFirst(Int(skip))
                            bytesToSkipInFirstTerm -= skip
                            if chunk.isEmpty { continue }
                        }
                        if let remaining = remainingBytesToWrite {
                            if remaining == 0 { break }
                            if UInt64(chunk.count) > remaining {
                                chunk = chunk.prefix(Int(remaining))
                            }
                            remainingBytesToWrite = remaining - UInt64(chunk.count)
                        }
                        try await writer.write(chunk)
                        totalWritten += Int64(chunk.count)
                        print(
                            "XET DEBUG: wrote cached chunk xorbHash=\(term.hash.prefix(12)) idx=\(idx) bytes=\(chunk.count) totalWritten=\(totalWritten)"
                        )
                    }
                    continue
                }
            }

            print(
                "XET DEBUG: term xorbHash=\(term.hash.prefix(12)) termRange=\(term.range.lowerBound)..<\(term.range.upperBound) fetchRange=\(fetchInfo.range.lowerBound)..<\(fetchInfo.range.upperBound) urlRange=\(fetchInfo.urlRange.lowerBound)..<\(fetchInfo.urlRange.upperBound)"
            )

            let shouldCacheAllForXorb = (xorbUsageCount[term.hash] ?? 0) > 1
            if fetchedFetchRanges.contains(key) {
                if shouldCacheAllForXorb {
                    print("XET DEBUG: fetch range already handled, skipping xorbHash=\(term.hash.prefix(12))")
                    continue
                }
            }

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
                var map = chunkCache[term.hash] ?? [:]
                for (index, chunk) in fetchedChunks {
                    map[index] = chunk
                }
                chunkCache[term.hash] = map
            }

            for idx in term.range {
                guard var outChunk = fetchedChunks[idx] else { continue }
                if bytesToSkipInFirstTerm > 0 {
                    let skip = min(UInt64(outChunk.count), bytesToSkipInFirstTerm)
                    outChunk = outChunk.dropFirst(Int(skip))
                    bytesToSkipInFirstTerm -= skip
                    if outChunk.isEmpty {
                        print("XET DEBUG: skipped entire chunk idx=\(idx)")
                        continue
                    }
                }

                if let remaining = remainingBytesToWrite {
                    if remaining == 0 {
                        print("XET DEBUG: remainingBytesToWrite=0 inside chunk loop, breaking")
                        break
                    }
                    if UInt64(outChunk.count) > remaining {
                        outChunk = outChunk.prefix(Int(remaining))
                    }
                    remainingBytesToWrite = remaining - UInt64(outChunk.count)
                }

                if !outChunk.isEmpty {
                    try await writer.write(outChunk)
                    totalWritten += Int64(outChunk.count)
                    print(
                        "XET DEBUG: wrote chunk xorbHash=\(term.hash.prefix(12)) idx=\(idx) bytes=\(outChunk.count) totalWritten=\(totalWritten)"
                    )
                }
            }
            print("XET DEBUG: finished term write for xorbHash=\(term.hash.prefix(12))")

            if shouldCacheAllForXorb {
                fetchedFetchRanges.insert(key)
            }
        }

        print("XET DEBUG: download complete totalWritten=\(totalWritten)")
        return totalWritten
    }

    private func fetchXorbChunks(
        termHash: String,
        fetchInfo: CASClient.ReconstructionResponse.FetchInfo,
        request: URLRequest
    ) async throws -> [Int: Data] {
        print(
            "XET DEBUG: fetch start xorbHash=\(termHash.prefix(12)) range=\(fetchInfo.range.lowerBound)..<\(fetchInfo.range.upperBound)"
        )
        let session = await fetchSessionPool.nextSession()
        let (stream, response) = try await session.bytes(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw XetDownloaderError.fetchFailed(statusCode: nil, url: request.url ?? URL(fileURLWithPath: "/"))
        }
        guard (200 ..< 300).contains(http.statusCode) || http.statusCode == 206 else {
            throw XetDownloaderError.fetchFailed(
                statusCode: http.statusCode,
                url: request.url ?? URL(fileURLWithPath: "/")
            )
        }
        print(
            "XET DEBUG: response status=\(http.statusCode) contentLength=\(http.value(forHTTPHeaderField: "Content-Length") ?? "nil")"
        )

        var chunkIndex = fetchInfo.range.lowerBound
        var result: [Int: Data] = [:]
        for try await uncompressed in Xorb.decode(bytes: stream) {
            // print("XET DEBUG: decoded chunkIndex=\(chunkIndex) bytes=\(uncompressed.count)")
            if fetchInfo.range.contains(chunkIndex) {
                result[chunkIndex] = uncompressed
                // print("XET DEBUG: cached chunk xorbHash=\(termHash.prefix(12)) idx=\(chunkIndex)")
            }
            chunkIndex += 1
        }
        print("XET DEBUG: finished fetch loop for xorbHash=\(termHash.prefix(12)) chunks=\(result.count)")
        return result
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

    init(value: Int) {
        self.available = max(0, value)
    }

    func acquire() async {
        while available == 0 {
            await Task.yield()
        }
        available -= 1
    }

    func release() {
        available += 1
    }
}

private actor SessionPool {
    private let sessions: [URLSession]
    private let delegate: URLSessionTaskDelegate?
    private var nextIndex = 0

    init(
        configuration: URLSessionConfiguration,
        size: Int,
        delegate: URLSessionTaskDelegate?
    ) {
        let poolSize = max(1, size)
        var created: [URLSession] = []
        created.reserveCapacity(poolSize)
        for _ in 0 ..< poolSize {
            created.append(
                URLSession(
                    configuration: configuration,
                    delegate: delegate,
                    delegateQueue: nil
                )
            )
        }
        self.sessions = created
        self.delegate = delegate
    }

    func nextSession() -> URLSession {
        let session = sessions[nextIndex]
        nextIndex = (nextIndex + 1) % sessions.count
        return session
    }
}

private final class TaskMetricsLogger: NSObject, URLSessionTaskDelegate {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didFinishCollecting metrics: URLSessionTaskMetrics
    ) {
        guard let transaction = metrics.transactionMetrics.last else {
            return
        }
        let url = task.originalRequest?.url?.absoluteString ?? "unknown"
        var protocolName = "unknown"
        if #available(iOS 13.0, macOS 10.15, tvOS 13.0, watchOS 6.0, *) {
            protocolName = transaction.networkProtocolName ?? "unknown"
        }
        let reused = transaction.isReusedConnection
        let remote = transaction.remoteAddress ?? "unknown"
        print(
            "XET DEBUG: metrics url=\(url) protocol=\(protocolName) reused=\(reused) remote=\(remote)"
        )
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
        init(urlSession: URLSession = .shared, safetyWindow: TimeInterval = 60) {
            self.urlSession = urlSession
            self.safetyWindow = safetyWindow
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
                print(
                    "XET DEBUG: token cache hit refreshURL=\(refreshURL.absoluteString) expiresAt=\(cached.expiresAt)"
                )
                return cached
            }

            if let existing = inflight[key] {
                print("XET DEBUG: token request already inflight refreshURL=\(refreshURL.absoluteString)")
                return try await existing.value
            }

            let task = Task { [urlSession] () throws -> ConnectionInfo in
                print("XET DEBUG: token request start refreshURL=\(refreshURL.absoluteString)")
                var request = URLRequest(url: refreshURL)
                request.httpMethod = "GET"
                request.cachePolicy = .reloadIgnoringLocalCacheData
                if let hubToken {
                    request.setValue("Bearer \(hubToken)", forHTTPHeaderField: "Authorization")
                    print("XET DEBUG: token request has auth header tokenPrefix=\(hubToken.prefix(8))")
                }

                let (data, response) = try await urlSession.data(for: request)
                guard let http = response as? HTTPURLResponse else {
                    print("XET DEBUG: token response not HTTPURLResponse")
                    throw XetDownloaderError.invalidTokenResponse
                }
                print("XET DEBUG: token response status=\(http.statusCode) bytes=\(data.count)")
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
                    print("XET DEBUG: token decode failed error=\(error.localizedDescription)")
                    throw XetDownloaderError.invalidTokenResponse
                }
                guard let casURL = URL(string: decoded.casUrl) else {
                    throw XetDownloaderError.invalidCASURL(decoded.casUrl)
                }

                let expiresAt = Date(timeIntervalSince1970: TimeInterval(decoded.exp))
                print(
                    "XET DEBUG: token decoded casURL=\(casURL.absoluteString) expiresAt=\(expiresAt) accessTokenPrefix=\(decoded.accessToken.prefix(12))"
                )
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
    private static func makeDownloadConfiguration(
        httpMaximumConnectionsPerHost: Int,
        waitsForConnectivity: Bool,
        timeoutIntervalForRequest: TimeInterval
    ) -> URLSessionConfiguration {
        let configuration = URLSessionConfiguration.default
        configuration.httpMaximumConnectionsPerHost = max(1, httpMaximumConnectionsPerHost)
        configuration.httpShouldUsePipelining = true
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.waitsForConnectivity = waitsForConnectivity
        configuration.timeoutIntervalForRequest = timeoutIntervalForRequest
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            if #available(iOS 11.0, tvOS 11.0, watchOS 4.0, *) {
                configuration.multipathServiceType = .handover
            }
        #endif
        return configuration
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

/// A destination for writing downloaded chunk data.
protocol OutputWriter: Sendable {
    func write(_ data: Data) async throws
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
