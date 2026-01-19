// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Xet",
    platforms: [
        .macOS(.v13),
        .iOS(.v15),
        .tvOS(.v15),
        .watchOS(.v8),
        .visionOS(.v1),
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "Xet",
            targets: ["Xet"]
        )
    ],
    dependencies: [
        // 1.28.0: HTTPClient.Configuration.connectionPool.preWarmedHTTP1ConnectionCount
        .package(url: "https://github.com/swift-server/async-http-client", from: "1.28.0"),
        // 2.81.0: minimum required by async-http-client 1.28.0+
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "Xet",
            dependencies: [
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "XetTests",
            dependencies: ["Xet"]
        ),
    ]
)
