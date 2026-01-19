import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// Client for the Xet Content Addressable Storage (CAS) reconstruction API.
///
/// The CAS server stores file data as deduplicated, compressed chunks
/// organized into xorbs. This client fetches reconstruction metadata
/// that describes how to reassemble a file from its constituent chunks.
struct CASClient: Sendable {
    private let urlSession: URLSession

    /// Creates a CAS client with the specified URL session.
    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }

    /// Fetches reconstruction metadata for a file.
    ///
    /// The reconstruction response contains:
    /// - An ordered list of terms, each referencing a chunk range within a xorb
    /// - Fetch info with presigned URLs for downloading xorb data
    /// - An offset for partial range requests
    ///
    /// - Parameters:
    ///   - fileID: The 64-character hex file identifier (Merkle hash).
    ///   - casURL: The CAS API base URL from token response.
    ///   - accessToken: The CAS access token.
    ///   - byteRange: Optional byte range for partial file reconstruction.
    ///
    /// - Returns: The reconstruction response with terms and fetch info.
    ///
    /// - Throws: ``XetDownloaderError`` if the request fails.
    func reconstruction(
        of fileID: String,
        casURL: URL,
        accessToken: String,
        byteRange: Range<UInt64>?
    ) async throws -> ReconstructionResponse {
        let url = casURL.appendingPathComponent("v1").appendingPathComponent("reconstructions")
            .appendingPathComponent(fileID)

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        if let byteRange, !byteRange.isEmpty {
            request.setValue(byteRange.httpRangeHeaderValue, forHTTPHeaderField: "Range")
        }

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw XetDownloaderError.invalidReconstructionResponse
        }
        guard (200 ..< 300).contains(http.statusCode) else {
            throw XetDownloaderError.reconstructionRequestFailed(
                statusCode: http.statusCode,
                body: data
            )
        }

        do {
            return try JSONDecoder().decode(ReconstructionResponse.self, from: data)
        } catch {
            throw XetDownloaderError.reconstructionDecodingFailed(error)
        }
    }

    /// Response from the CAS reconstruction API.
    ///
    /// Describes how to reassemble a file from chunks stored across one or more xorbs.
    /// The file is reconstructed by processing terms in order,
    /// fetching the referenced chunk ranges,
    /// and concatenating the decompressed results.
    struct ReconstructionResponse: Codable, Sendable {
        /// Byte offset to skip in the first term's output.
        ///
        /// For full file downloads this is always 0.
        /// For range requests, indicates where the requested range starts
        /// within the first chunk's decompressed data.
        let offsetIntoFirstRange: UInt64

        /// Ordered list of terms describing chunks to fetch.
        ///
        /// Each term references a contiguous range of chunks within a xorb.
        /// Terms must be processed in order to reconstruct the file correctly.
        let terms: [Term]

        /// Fetch info keyed by xorb hash.
        ///
        /// Maps xorb hashes to arrays of fetch info,
        /// each providing a presigned URL and byte range for downloading chunk data.
        let fetchInfo: [String: [FetchInfo]]

        /// Creates a reconstruction response.
        init(offsetIntoFirstRange: UInt64, terms: [Term], fetchInfo: [String: [FetchInfo]]) {
            self.offsetIntoFirstRange = offsetIntoFirstRange
            self.terms = terms
            self.fetchInfo = fetchInfo
        }

        private enum CodingKeys: String, CodingKey {
            case offsetIntoFirstRange = "offset_into_first_range"
            case terms
            case fetchInfo = "fetch_info"
        }

        private enum RangeCodingKeys: String, CodingKey {
            case start
            case end
        }

        /// A reconstruction term referencing chunks within a xorb.
        ///
        /// Each term specifies which chunks to extract from a particular xorb.
        /// The `hash` identifies the xorb,
        /// and the `range` specifies the half-open interval of chunk indices.
        struct Term: Codable, Sendable {
            /// The xorb's 64-character hex hash.
            let hash: String

            /// Expected total bytes after decompressing all chunks in this term.
            let unpackedLength: UInt32

            /// Half-open range of chunk indices: `[start, end)`.
            let range: Range<Int>

            /// Creates a term.
            init(hash: String, unpackedLength: UInt32, range: Range<Int>) {
                self.hash = hash
                self.unpackedLength = unpackedLength
                self.range = range
            }

            private enum CodingKeys: String, CodingKey {
                case hash
                case unpackedLength = "unpacked_length"
                case range
            }

            public init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: CodingKeys.self)
                hash = try container.decode(String.self, forKey: .hash)
                unpackedLength = try container.decode(UInt32.self, forKey: .unpackedLength)

                let rangeContainer = try container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .range
                )
                let start = try rangeContainer.decode(Int.self, forKey: .start)
                let end = try rangeContainer.decode(Int.self, forKey: .end)
                range = start ..< end
            }

            public func encode(to encoder: Encoder) throws {
                var container = encoder.container(keyedBy: CodingKeys.self)
                try container.encode(hash, forKey: .hash)
                try container.encode(unpackedLength, forKey: .unpackedLength)

                var rangeContainer = container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .range
                )
                try rangeContainer.encode(range.lowerBound, forKey: .start)
                try rangeContainer.encode(range.upperBound, forKey: .end)
            }
        }

        /// Information for fetching chunk data from a xorb.
        ///
        /// Provides a presigned URL and byte range for downloading
        /// a contiguous sequence of compressed chunks.
        struct FetchInfo: Codable, Sendable {
            /// Presigned URL for downloading xorb data.
            let url: String

            /// Half-open range of chunk indices covered by this fetch.
            let range: Range<Int>

            /// Closed byte range to request via HTTP `Range` header.
            let urlRange: ClosedRange<UInt64>

            /// Creates fetch info.
            init(url: String, range: Range<Int>, urlRange: ClosedRange<UInt64>) {
                self.url = url
                self.range = range
                self.urlRange = urlRange
            }

            /// The HTTP `Range` header value for this fetch.
            var urlRangeHeaderValue: String {
                "bytes=\(urlRange.lowerBound)-\(urlRange.upperBound)"
            }

            private enum CodingKeys: String, CodingKey {
                case url
                case range
                case urlRange = "url_range"
            }

            public init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: CodingKeys.self)
                url = try container.decode(String.self, forKey: .url)

                let rangeContainer = try container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .range
                )
                let rangeStart = try rangeContainer.decode(Int.self, forKey: .start)
                let rangeEnd = try rangeContainer.decode(Int.self, forKey: .end)
                range = rangeStart ..< rangeEnd

                let urlRangeContainer = try container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .urlRange
                )
                let urlStart = try urlRangeContainer.decode(UInt64.self, forKey: .start)
                let urlEnd = try urlRangeContainer.decode(UInt64.self, forKey: .end)
                urlRange = urlStart ... urlEnd
            }

            public func encode(to encoder: Encoder) throws {
                var container = encoder.container(keyedBy: CodingKeys.self)
                try container.encode(url, forKey: .url)

                var rangeContainer = container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .range
                )
                try rangeContainer.encode(range.lowerBound, forKey: .start)
                try rangeContainer.encode(range.upperBound, forKey: .end)

                var urlRangeContainer = container.nestedContainer(
                    keyedBy: RangeCodingKeys.self,
                    forKey: .urlRange
                )
                try urlRangeContainer.encode(urlRange.lowerBound, forKey: .start)
                try urlRangeContainer.encode(urlRange.upperBound, forKey: .end)
            }
        }
    }
}

// MARK: -

extension Range<UInt64> {
    /// Formats the range as an HTTP `Range` header value.
    ///
    /// Uses the standard `bytes=start-end` format where end is inclusive.
    fileprivate var httpRangeHeaderValue: String {
        precondition(!isEmpty)
        return "bytes=\(lowerBound)-\(upperBound - 1)"
    }
}
