// swift-tools-version: 6.0
import PackageDescription

// Stress harness for GoogleCloudPubSub, kept out of the library package so it doesn't affect
// consumers. It uses `@testable import`, so build it with testing enabled:
//
//   swift build -c release -Xswiftc -enable-testing
//
// Run `.build/release/PubSubStress --help` for options.
let package = Package(
  name: "google-cloud-pubsub-stress",
  platforms: [
    .macOS(.v15)
  ],
  dependencies: [
    .package(name: "google-cloud-pubsub-swift", path: ".."),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
    .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.3.0"),
    .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.0.0"),
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.54.0"),
  ],
  targets: [
    .executableTarget(
      name: "PubSubStress",
      dependencies: [
        .product(name: "GoogleCloudPubSub", package: "google-cloud-pubsub-swift"),
        .product(name: "Logging", package: "swift-log"),
        .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
        .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
        .product(name: "NIOCore", package: "swift-nio"),
      ]
    )
  ]
)
