// swift-tools-version: 6.4

import PackageDescription

let package = Package(
    name: "OptionalSubscripts",
    platforms: [.macOS(.v11), .iOS(.v14)],
    products: [
        .library(name: "OptionalSubscripts", targets: ["OptionalSubscripts"]),
    ],
    dependencies: [
        .package(url: "https://github.com/screensailor/Hope.git", branch: "trunk"),
        .package(url: "https://github.com/pointfreeco/swift-clocks", exact: "1.1.1"),
    ],
    targets: [
        .target(name: "OptionalSubscripts", dependencies: ["OptionalSubscriptsLegacy"]),
        // Any? streams and unconstrained legacy stores predate Sendable checking.
        // Keep their source compatibility without weakening the typed public core.
        .target(name: "OptionalSubscriptsLegacy", swiftSettings: [.swiftLanguageMode(.v5)]),
        .testTarget(name: "OptionalSubscriptsTests", dependencies: [
            "OptionalSubscripts", .product(name: "Clocks", package: "swift-clocks"),
        ]),
        .testTarget(name: "OptionalSubscriptsLegacyTests", dependencies: ["OptionalSubscriptsLegacy", "Hope"],
                    swiftSettings: [.swiftLanguageMode(.v5)]),
    ],
    swiftLanguageModes: [.v6]
)
