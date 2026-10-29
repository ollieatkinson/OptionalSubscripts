// swift-tools-version: 6.4

import PackageDescription

let package = Package(
    name: "OptionalSubscripts",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [
        .library(name: "OptionalSubscripts", targets: ["OptionalSubscripts"]),
    ],
    dependencies: [
        .package(url: "https://github.com/pointfreeco/swift-clocks", exact: "1.1.1"),
        .package(url: "https://github.com/apple/swift-collections", from: "1.7.5"),
    ],
    targets: [
        .target(name: "OptionalSubscripts", dependencies: [
            .product(name: "OrderedCollections", package: "swift-collections"),
        ]),
        .testTarget(name: "OptionalSubscriptsTests", dependencies: [
            "OptionalSubscripts", .product(name: "Clocks", package: "swift-clocks"),
        ]),
    ],
    swiftLanguageModes: [.v6]
)
