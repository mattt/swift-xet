# Xet

A Swift implementation of the
[Xet protocol](https://github.com/huggingface/xet-core)
for downloading files from Hugging Face's content-addressable storage (CAS).

Xet is a storage layer that provides efficient file transfer
through content-defined chunking, deduplication, and compression.
This package implements the download path,
enabling Swift applications to fetch files from Hugging Face Hub repositories
that use Xet storage.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="Assets/xet-speed-dark.gif">
  <source media="(prefers-color-scheme: light)" srcset="Assets/xet-speed.gif">
  <img alt="XET vs LFS">
</picture>

## Requirements

- Swift 6.0+ / Xcode 16+
- macOS 13+ / iOS 15+ / tvOS 15+ / watchOS 8+ / visionOS

## Installation

### Swift Package Manager

Add the following to your `Package.swift` file:

```swift
dependencies: [
    .package(url: "https://github.com/mattt/swift-xet.git", from: "0.1.0")
]
```

Then add the dependency to your target:

```swift
.target(
    name: "YourTarget",
    dependencies: [
        .product(name: "Xet", package: "swift-xet")
    ]
)
```

## Usage

### Downloading a File

To download a file, you need:

- A **file ID**: the 64-character hex hash from the `X-Xet-Hash` response header
- A **refresh URL**: the Hub endpoint for obtaining CAS access tokens

```swift
import Xet

try await Xet.withDownloader(
    refreshURL: refreshURL,
    hubToken: "hf_..."  // optional, required for private repos
) { downloader in
    // Download to memory
    let data = try await downloader.data(for: fileID)

    // Download to disk
    try await downloader.download(fileID, to: destinationURL)
}
```

### Partial Downloads

Both methods support partial downloads via the `byteRange` parameter:

```swift
// Download first 1MiB only
let data = try await downloader.data(
    for: fileID,
    byteRange: 0..<(1024 * 1024)
)
```

The file ID comes from the `X-Xet-Hash` header
when resolving a file URL without following redirects:

```swift
// Construct URLs for a Hugging Face repository
let repoType = "datasets"  // or "models", "spaces"
let repoID = "username/repo-name"
let revision = "main"
let filePath = "path/to/file.bin"

let resolveURL = URL(string:
    "https://huggingface.co/\(repoType)/\(repoID)/resolve/\(revision)/\(filePath)"
)!

let refreshURL = URL(string:
    "https://huggingface.co/api/\(repoType)/\(repoID)/xet-read-token/\(revision)"
)!

// Get the file ID by making a request that doesn't follow redirects
// and reading the X-Xet-Hash header from the response
```

### Tuning HTTP Performance

This package uses `AsyncHTTPClient` under the hood for CAS and xorb downloads.
The downloader manages a small pool of HTTP clients and shuts them down
automatically when you use `Xet.withDownloader`.
Tuning can help when you need to balance throughput, memory, and connection
limits for your network environment.

You can configure the client pool and timeouts through
`XetDownloader.Configuration`:

```swift
var configuration = XetDownloader.Configuration.default
configuration.connectionsPerHost = 8
configuration.poolSize = 2
configuration.readTimeout = 300

try await Xet.withDownloader(
    refreshURL: refreshURL,
    hubToken: "hf_...",
    configuration: configuration
) { downloader in
    try await downloader.download(fileID, to: destinationURL)
}
```


## How It Works

The Xet protocol reconstructs files from deduplicated, compressed chunks:

1. **Token Refresh**: Obtain a short-lived CAS access token from the Hub
2. **Reconstruction Query**: Fetch metadata describing which chunks comprise the file
3. **Chunk Download**: Fetch compressed chunk data from xorb storage
4. **Decompression**: Decompress chunks using LZ4 or BG4+LZ4
5. **Reassembly**: Concatenate chunks in order to reconstruct the file

### Xorb Format

Files are stored as _xorbs_ (Xet Orbs)—sequences of compressed chunks.
Each chunk has an 8-byte header specifying:

- Version (1 byte)
- Compressed size (3 bytes, little-endian)
- Compression scheme (1 byte): none, LZ4, or BG4+LZ4
- Uncompressed size (3 bytes, little-endian)

BG4 (Byte Grouping 4) is a preprocessing step that improves compression
for floating-point and structured data by grouping bytes by position.
