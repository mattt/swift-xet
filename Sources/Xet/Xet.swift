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
    public init(
        refreshURL: URL,
        hubToken: String? = nil,
        urlSession: URLSession = .shared
    ) {
        self.refreshURL = refreshURL
        self.hubToken = hubToken
        self.urlSession = urlSession
        self.tokenProvider = TokenProvider(urlSession: urlSession)
        self.casClient = CASClient(urlSession: urlSession)
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

        let maxBytesToWrite: UInt64? = byteRange.map { UInt64($0.count) }
        var remainingBytesToWrite = maxBytesToWrite

        var bytesToSkipInFirstTerm = reconstruction.offsetIntoFirstRange

        var xorbUsageCount: [String: Int] = [:]
        for term in reconstruction.terms {
            xorbUsageCount[term.hash, default: 0] += 1
        }

        var chunkCache: [String: [Int: Data]] = [:]

        var fetchedFetchRanges: Set<FetchRangeKey> = []

        var totalWritten: Int64 = 0

        for term in reconstruction.terms {
            if let remainingBytesToWrite, remainingBytesToWrite == 0 {
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
                    }
                    continue
                }
            }

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

            let key = FetchRangeKey(
                hash: term.hash,
                start: fetchInfo.range.lowerBound,
                end: fetchInfo.range.upperBound,
                urlRangeStart: fetchInfo.urlRange.lowerBound,
                urlRangeEnd: fetchInfo.urlRange.upperBound
            )
            let shouldCacheAllForXorb = (xorbUsageCount[term.hash] ?? 0) > 1
            if fetchedFetchRanges.contains(key) {
                if shouldCacheAllForXorb {
                    continue
                }
            }

            guard let fetchURL = URL(string: fetchInfo.url) else {
                throw XetDownloaderError.invalidFetchURL(fetchInfo.url)
            }

            // Validate fetch URL uses HTTPS unless insecure connections are allowed
            if !allowsInsecureConnections && fetchURL.scheme != "https" {
                throw XetDownloaderError.insecureURL(fetchURL)
            }

            var request = URLRequest(url: fetchURL)
            request.httpMethod = "GET"
            request.setValue(fetchInfo.urlRangeHeaderValue, forHTTPHeaderField: "Range")

            let (stream, response) = try await urlSession.bytes(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw XetDownloaderError.fetchFailed(statusCode: nil, url: fetchURL)
            }
            guard (200 ..< 300).contains(http.statusCode) || http.statusCode == 206 else {
                throw XetDownloaderError.fetchFailed(statusCode: http.statusCode, url: fetchURL)
            }

            var chunkIndex = fetchInfo.range.lowerBound

            for try await uncompressed in Xorb.decode(bytes: stream) {
                let inFetchRange = fetchInfo.range.contains(chunkIndex)

                if shouldCacheAllForXorb, inFetchRange {
                    var map = chunkCache[term.hash] ?? [:]
                    map[chunkIndex] = uncompressed
                    chunkCache[term.hash] = map
                }

                if inFetchRange, term.range.contains(chunkIndex) {
                    var outChunk = uncompressed
                    if bytesToSkipInFirstTerm > 0 {
                        let skip = min(UInt64(outChunk.count), bytesToSkipInFirstTerm)
                        outChunk = outChunk.dropFirst(Int(skip))
                        bytesToSkipInFirstTerm -= skip
                        if outChunk.isEmpty {
                            chunkIndex += 1
                            continue
                        }
                    }

                    if let remaining = remainingBytesToWrite {
                        if remaining == 0 {
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
                    }
                }

                chunkIndex += 1
            }

            if shouldCacheAllForXorb {
                fetchedFetchRanges.insert(key)
            }
        }

        return totalWritten
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
                return cached
            }

            if let existing = inflight[key] {
                return try await existing.value
            }

            let task = Task { [urlSession] () throws -> ConnectionInfo in
                var request = URLRequest(url: refreshURL)
                request.httpMethod = "GET"
                request.cachePolicy = .reloadIgnoringLocalCacheData
                if let hubToken {
                    request.setValue("Bearer \(hubToken)", forHTTPHeaderField: "Authorization")
                }

                let (data, response) = try await urlSession.data(for: request)
                guard let http = response as? HTTPURLResponse else {
                    throw XetDownloaderError.invalidTokenResponse
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
                    throw XetDownloaderError.invalidTokenResponse
                }
                guard let casURL = URL(string: decoded.casUrl) else {
                    throw XetDownloaderError.invalidCASURL(decoded.casUrl)
                }

                let expiresAt = Date(timeIntervalSince1970: TimeInterval(decoded.exp))
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
