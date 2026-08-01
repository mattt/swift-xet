import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

import Testing
import NIOConcurrencyHelpers

@testable import Xet

private final class NoRedirectDelegate: NSObject, URLSessionTaskDelegate {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

private func resolveXetFileID(
    resolveURL: URL,
    hubToken: String?
) async throws -> String? {
    let config = URLSessionConfiguration.ephemeral
    config.httpAdditionalHeaders = ["Accept-Encoding": "identity"]
    let delegate = NoRedirectDelegate()
    let session = URLSession(configuration: config, delegate: delegate, delegateQueue: nil)

    var request = URLRequest(url: resolveURL)
    request.httpMethod = "GET"
    if let hubToken {
        request.setValue("Bearer \(hubToken)", forHTTPHeaderField: "Authorization")
    }

    let (_, response) = try await session.data(for: request)
    _ = delegate
    guard let http = response as? HTTPURLResponse else { return nil }

    let headers = http.allHeaderFields
    for (k, v) in headers {
        if let key = k as? String, key.lowercased() == "x-xet-hash" {
            return v as? String
        }
    }
    return nil
}

@Suite(
    "Integration Tests",
    .enabled(if: ProcessInfo.processInfo.environment["HF_TOKEN"] != nil)
)
struct IntegrationTests {
    @Test func rangeDownload() async throws {
        let hubToken = ProcessInfo.processInfo.environment["HF_TOKEN"]
        guard let hubToken else { return }

        let repoID = "xet-team/xet-spec-reference-files"
        let revision = "main"
        let filePath = "Electric_Vehicle_Population_Data_20250917.csv"

        let resolveURL = URL(
            string: "https://huggingface.co/datasets/\(repoID)/resolve/\(revision)/\(filePath)"
        )!
        let refreshURL = URL(
            string: "https://huggingface.co/api/datasets/\(repoID)/xet-read-token/\(revision)"
        )!

        let fileID = try await resolveXetFileID(resolveURL: resolveURL, hubToken: hubToken)
        #expect(fileID != nil)
        guard let fileID else { return }

        let range: Range<UInt64> = 0 ..< (512 * 1024)
        let bytes1 = try await Xet.withDownloader(refreshURL: refreshURL) { downloader in
            try await downloader.data(for: fileID, byteRange: range)
        }

        #expect(!bytes1.isEmpty)
        #expect(bytes1.count <= Int(range.count))
        #expect(
            String(data: bytes1.prefix(80), encoding: .utf8)?.hasPrefix(
                "VIN (1-10),County,City,State,Postal Code"
            ) == true
        )

        let bytes2 = try await Xet.withDownloader(refreshURL: refreshURL) { downloader in
            try await downloader.data(for: fileID, byteRange: range)
        }
        #expect(bytes1 == bytes2)
    }

    @Test func downloadToFile() async throws {
        let hubToken = ProcessInfo.processInfo.environment["HF_TOKEN"]
        guard let hubToken else { return }

        let repoID = "xet-team/xet-spec-reference-files"
        let revision = "main"
        let filePath = "Electric_Vehicle_Population_Data_20250917.csv"

        let resolveURL = URL(
            string: "https://huggingface.co/datasets/\(repoID)/resolve/\(revision)/\(filePath)"
        )!
        let refreshURL = URL(
            string: "https://huggingface.co/api/datasets/\(repoID)/xet-read-token/\(revision)"
        )!

        let fileID = try await resolveXetFileID(resolveURL: resolveURL, hubToken: hubToken)
        #expect(fileID != nil)
        guard let fileID else { return }

        let tempDir = FileManager.default.temporaryDirectory
        let destinationURL = tempDir.appendingPathComponent(UUID().uuidString + ".csv")
        defer { try? FileManager.default.removeItem(at: destinationURL) }

        let range: Range<UInt64> = 0 ..< (256 * 1024)
        let bytesWritten = try await Xet.withDownloader(refreshURL: refreshURL) { downloader in
            try await downloader.download(
                fileID,
                byteRange: range,
                to: destinationURL
            )
        }

        #expect(bytesWritten > 0)
        #expect(bytesWritten <= Int64(range.count))

        let fileData = try Data(contentsOf: destinationURL)
        #expect(fileData.count == Int(bytesWritten))
        #expect(
            String(data: fileData.prefix(80), encoding: .utf8)?.hasPrefix(
                "VIN (1-10),County,City,State,Postal Code"
            ) == true
        )
    }

    @Test func downloadReportsIncrementalProgress() async throws {
        let hubToken = ProcessInfo.processInfo.environment["HF_TOKEN"]
        guard let hubToken else { return }

        let repoID = "xet-team/xet-spec-reference-files"
        let revision = "main"
        let filePath = "Electric_Vehicle_Population_Data_20250917.csv"

        let resolveURL = URL(
            string: "https://huggingface.co/datasets/\(repoID)/resolve/\(revision)/\(filePath)"
        )!
        let refreshURL = URL(
            string: "https://huggingface.co/api/datasets/\(repoID)/xet-read-token/\(revision)"
        )!

        let fileID = try await resolveXetFileID(resolveURL: resolveURL, hubToken: hubToken)
        #expect(fileID != nil)
        guard let fileID else { return }

        let tempDir = FileManager.default.temporaryDirectory
        let destinationURL = tempDir.appendingPathComponent(UUID().uuidString + ".csv")
        defer { try? FileManager.default.removeItem(at: destinationURL) }
        
        // Use a large enough byte range to trigger multiple xorb chunks. This makes sure that the downloader is not returning per file progress, and is instead returning per trunk progress.
        let range: Range<UInt64> = 0 ..< (8 * 1024 * 1024)

        // A thread-safe array of progress values. Using a NIO box to prevent writes from multiple threads at once since the Xet downloader downloads chunks on multiple threads.
        let progressValues = NIOLockedValueBox<[(bytesWritten: Int64, totalBytes: Int64)]>([])
        
        let bytesWritten = try await Xet.withDownloader(refreshURL: refreshURL) { downloader in
            try await downloader.download(
                fileID,
                byteRange: range,
                to: destinationURL
            ) { bytesWritten, totalBytes in
                progressValues.withLockedValue { prog in
                    prog.append((bytesWritten, totalBytes))
                }
            }
        }

        let updates = progressValues.withLockedValue({ return $0 })

        #expect(bytesWritten > 0)
        #expect(updates.count > 5, "expected many incremental progress calls, got \(updates.count)")

        let totals = Set(updates.map(\.totalBytes))
        #expect(totals.count == 1, "totalBytes should be constant across all calls")

        for (previous, current) in zip(updates, updates.dropFirst()) {
            #expect(
                current.bytesWritten >= previous.bytesWritten,
                "bytesWritten must not decrease through download"
            )
        }
        
        #expect(updates.last?.bytesWritten == bytesWritten, "The final update should equal the bytesWrittena as reported by the downloader.")
    }
}
