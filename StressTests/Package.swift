// swift-tools-version: 6.0
import PackageDescription

// Stress harness for GoogleCloudPubSub, kept out of the library package so it doesn't affect
// consumers. It only uses the library's public API, so measure with a production build:
//
//   swift build -c release
//
// Run `.build/release/PubSubStress --help` for options and see README.md.
let package = Package(
  name: "google-cloud-pubsub-stress",
  platforms: [
    .macOS(.v15)
  ],
  dependencies: [
    .package(name: "google-cloud-pubsub-swift", path: ".."),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
    .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.3.0"),
    .package(url: "https://github.com/rosecoder/google-cloud-auth-swift.git", from: "1.3.3"),
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.54.0"),
  ],
  targets: [
    .executableTarget(
      name: "PubSubStress",
      dependencies: [
        .product(name: "GoogleCloudPubSub", package: "google-cloud-pubsub-swift"),
        .product(name: "Logging", package: "swift-log"),
        .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
        .product(name: "GoogleCloudAuth", package: "google-cloud-auth-swift"),
        .product(name: "NIOPosix", package: "swift-nio"),
      ]
    )
  ]
)
