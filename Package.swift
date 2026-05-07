// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "AuralKit",
    platforms: [
        .iOS("15.0"),
        .macOS("13.0"),
    ],
    products: [
        .library(
            name: "AuralKit",
            targets: ["AuralKit"]
        ),
        .executable(
            name: "Aural",
            targets: ["Aural"]
        )
    ],
    targets: [
        .target(
            name: "AuralKit"
        ),
        .executableTarget(
            name: "Aural",
            dependencies: ["AuralKit"],
            path: "Aural",
            exclude: [
                "Info.plist",
                "NoiseShader.metal"
            ],
            resources: [
                .process("Assets.xcassets")
            ]
        ),
        .testTarget(
            name: "AuralKitTests",
            dependencies: ["AuralKit"]
        ),
    ]
)
