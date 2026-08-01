import Foundation
import Testing

@testable import Xet

typealias ReconstructionResponse = CASClient.ReconstructionResponse

@Suite("CAS Tests")
struct CASTests {

    // MARK: - ReconstructionResponse Codable

    @Test func reconstructionResponseDecodesFromJSON() throws {
        let json = """
            {
                "offset_into_first_range": 100,
                "terms": [
                    {
                        "hash": "abc123",
                        "unpacked_length": 1024,
                        "range": {"start": 0, "end": 5}
                    },
                    {
                        "hash": "def456",
                        "unpacked_length": 2048,
                        "range": {"start": 0, "end": 3}
                    }
                ],
                "fetch_info": {
                    "abc123": [
                        {
                            "url": "https://example.com/xorb1",
                            "range": {"start": 0, "end": 10},
                            "url_range": {"start": 0, "end": 999}
                        }
                    ],
                    "def456": [
                        {
                            "url": "https://example.com/xorb2",
                            "range": {"start": 0, "end": 5},
                            "url_range": {"start": 1000, "end": 1999}
                        }
                    ]
                }
            }
            """

        let response = try JSONDecoder().decode(
            ReconstructionResponse.self,
            from: Data(json.utf8)
        )

        #expect(response.offsetIntoFirstRange == 100)
        #expect(response.terms.count == 2)

        let term0 = response.terms[0]
        #expect(term0.hash == "abc123")
        #expect(term0.unpackedLength == 1024)
        #expect(term0.range == 0 ..< 5)

        let term1 = response.terms[1]
        #expect(term1.hash == "def456")
        #expect(term1.unpackedLength == 2048)
        #expect(term1.range == 0 ..< 3)

        #expect(response.fetchInfo.count == 2)

        let fetchInfo0 = response.fetchInfo["abc123"]!.first!
        #expect(fetchInfo0.url == "https://example.com/xorb1")
        #expect(fetchInfo0.range == 0 ..< 10)
        #expect(fetchInfo0.urlRange == 0 ... 999)

        let fetchInfo1 = response.fetchInfo["def456"]!.first!
        #expect(fetchInfo1.url == "https://example.com/xorb2")
        #expect(fetchInfo1.range == 0 ..< 5)
        #expect(fetchInfo1.urlRange == 1000 ... 1999)
    }

    @Test func reconstructionResponseEncodesToJSON() throws {
        let response = ReconstructionResponse(
            offsetIntoFirstRange: 50,
            terms: [
                .init(hash: "hash1", unpackedLength: 512, range: 0 ..< 3)
            ],
            fetchInfo: [
                "hash1": [
                    .init(url: "https://cdn.example/blob", range: 0 ..< 3, urlRange: 100 ... 500)
                ]
            ]
        )

        let data = try JSONEncoder().encode(response)
        let decoded = try JSONDecoder().decode(ReconstructionResponse.self, from: data)

        #expect(decoded.offsetIntoFirstRange == response.offsetIntoFirstRange)
        #expect(decoded.terms.count == response.terms.count)
        #expect(decoded.terms[0].hash == response.terms[0].hash)
        #expect(decoded.terms[0].range == response.terms[0].range)
        #expect(decoded.fetchInfo["hash1"]!.first!.urlRange == 100 ... 500)
    }

    @Test func termDecodesSnakeCaseFields() throws {
        let json = """
            {
                "hash": "somehash",
                "unpacked_length": 4096,
                "range": {"start": 5, "end": 10}
            }
            """

        let term = try JSONDecoder().decode(
            ReconstructionResponse.Term.self,
            from: Data(json.utf8)
        )

        #expect(term.hash == "somehash")
        #expect(term.unpackedLength == 4096)
        #expect(term.range == 5 ..< 10)
    }

    @Test func fetchInfoDecodesNestedRanges() throws {
        let json = """
            {
                "url": "https://storage.example/file",
                "range": {"start": 0, "end": 100},
                "url_range": {"start": 1000, "end": 2000}
            }
            """

        let fetchInfo = try JSONDecoder().decode(
            ReconstructionResponse.FetchInfo.self,
            from: Data(json.utf8)
        )

        #expect(fetchInfo.url == "https://storage.example/file")
        #expect(fetchInfo.range == 0 ..< 100)
        #expect(fetchInfo.urlRange == 1000 ... 2000)
    }

    @Test func fetchInfoURLRangeHeaderValue() throws {
        let fetchInfo = ReconstructionResponse.FetchInfo(
            url: "https://example.com",
            range: 0 ..< 5,
            urlRange: 100 ... 500
        )

        #expect(fetchInfo.urlRangeHeaderValue == "bytes=100-500")
    }

    // MARK: - Edge Cases

    @Test func emptyTermsArray() throws {
        let json = """
            {
                "offset_into_first_range": 0,
                "terms": [],
                "fetch_info": {}
            }
            """

        let response = try JSONDecoder().decode(
            ReconstructionResponse.self,
            from: Data(json.utf8)
        )

        #expect(response.offsetIntoFirstRange == 0)
        #expect(response.terms.isEmpty)
        #expect(response.fetchInfo.isEmpty)
    }

    @Test func multipleFetchInfosForSameHash() throws {
        let json = """
            {
                "offset_into_first_range": 0,
                "terms": [],
                "fetch_info": {
                    "hash1": [
                        {"url": "url1", "range": {"start": 0, "end": 5}, "url_range": {"start": 0, "end": 100}},
                        {"url": "url2", "range": {"start": 5, "end": 10}, "url_range": {"start": 100, "end": 200}}
                    ]
                }
            }
            """

        let response = try JSONDecoder().decode(
            ReconstructionResponse.self,
            from: Data(json.utf8)
        )

        #expect(response.fetchInfo["hash1"]?.count == 2)
        #expect(response.fetchInfo["hash1"]?[0].url == "url1")
        #expect(response.fetchInfo["hash1"]?[1].url == "url2")
    }

    @Test func largeOffsetValue() throws {
        let json = """
            {
                "offset_into_first_range": 18446744073709551615,
                "terms": [],
                "fetch_info": {}
            }
            """

        let response = try JSONDecoder().decode(
            ReconstructionResponse.self,
            from: Data(json.utf8)
        )

        #expect(response.offsetIntoFirstRange == UInt64.max)
    }

    @Test func rangeWithZeroLength() throws {
        let term = ReconstructionResponse.Term(
            hash: "empty",
            unpackedLength: 0,
            range: 5 ..< 5  // empty range
        )

        #expect(term.range.isEmpty)
        #expect(term.range.lowerBound == 5)
        #expect(term.range.upperBound == 5)
    }

    // MARK: - expectedTotalBytes (progress reporting)

    @Test func expectedTotalBytesSumsUnpackedLengthsForFullFileDownload() throws {
        let terms = [
            ReconstructionResponse.Term(hash: "a", unpackedLength: 1000, range: 0 ..< 2),
            ReconstructionResponse.Term(hash: "b", unpackedLength: 2500, range: 0 ..< 3),
        ]

        let reconstruction = ReconstructionResponse.init(offsetIntoFirstRange: 0, terms: terms, fetchInfo: [:])
        
        let total = reconstruction.expectedTotalBytes(byteRange: nil)

        #expect(total == 3500)
    }

    @Test func expectedTotalBytesSubtractsOffsetIntoFirstRange() throws {
        let terms = [
            ReconstructionResponse.Term(hash: "a", unpackedLength: 1000, range: 0 ..< 2)
        ]
        
        let reconstruction = ReconstructionResponse.init(offsetIntoFirstRange: 200, terms: terms, fetchInfo: [:])

        let total = reconstruction.expectedTotalBytes(byteRange: nil)

        #expect(total == 800)
    }

    @Test func expectedTotalBytesClampsToRequestedByteRange() throws {
        let terms = [
            ReconstructionResponse.Term(hash: "a", unpackedLength: 10_000, range: 0 ..< 20)
        ]
        
        let reconstruction = ReconstructionResponse.init(offsetIntoFirstRange: 0, terms: terms, fetchInfo: [:])

        let total = reconstruction.expectedTotalBytes(byteRange: 0..<512)

        #expect(total == 512)
    }

    @Test func expectedTotalBytesNeverGoesNegative() throws {
        let terms = [
            ReconstructionResponse.Term(hash: "a", unpackedLength: 100, range: 0 ..< 1)
        ]

        // Offset larger than total unpacked bytes.
        let reconstruction = ReconstructionResponse.init(offsetIntoFirstRange: 1000, terms: terms, fetchInfo: [:])

        let total = reconstruction.expectedTotalBytes(byteRange: nil)

        #expect(total == 0)
    }

    @Test func expectedTotalBytesForEmptyTerms() throws {
        let reconstruction = ReconstructionResponse.init(offsetIntoFirstRange: 0, terms: [], fetchInfo: [:])

        let total = reconstruction.expectedTotalBytes(byteRange: nil)

        #expect(total == 0)
    }
}
