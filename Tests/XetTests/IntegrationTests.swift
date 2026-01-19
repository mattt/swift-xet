import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

import Testing

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
}
