// swift-tools-version:5.5

import PackageDescription

var dependencies: [Package.Dependency] = [
    .package(url: "https://github.com/screensailor/Hope.git", .branch("trunk")),
]
var testDependencies: [Target.Dependency] = ["OptionalSubscripts", "Hope"]
// Only modern behaviour tests need a virtual clock; production uses Swift Clock.
#if compiler(>=6.0)
dependencies.append(.package(url: "https://github.com/pointfreeco/swift-clocks", .exact("1.1.1")))
testDependencies.append(.product(name: "Clocks", package: "swift-clocks"))
#endif

let package = Package(
    name: "OptionalSubscripts",
    platforms: [.macOS(.v11), .iOS(.v14)],
    products: [
        .library(name: "OptionalSubscripts", targets: ["OptionalSubscripts"]),
    ],
    dependencies: dependencies,
    targets: [
        .target(name: "OptionalSubscripts"),
        .testTarget(name: "OptionalSubscriptsTests", dependencies: testDependencies),
    ]
)
