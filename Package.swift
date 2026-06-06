// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "AuralKit",
    platforms: [
        .iOS(.v15),
        .macOS(.v15),
    ],
    products: [
        .library(
            name: "AuralKit",
            targets: ["AuralKit"]
        )
    ],
    targets: [
        .target(
            name: "AuralKit"
        ),
        .testTarget(
            name: "AuralKitTests",
            dependencies: ["AuralKit"]
        ),
    ]
)
