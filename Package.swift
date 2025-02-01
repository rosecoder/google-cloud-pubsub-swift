// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "google-cloud-pubsub",
  platforms: [
    .macOS(.v15)
  ],
  products: [
    .library(name: "GoogleCloudPubSub", targets: ["GoogleCloudPubSub"]),
    .library(name: "GoogleCloudPubSubTesting", targets: ["GoogleCloudPubSubTesting"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
    .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.1.0"),
    .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "1.0.0"),
    .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "1.0.0"),
    .package(url: "https://github.com/rosecoder/google-cloud-auth-swift.git", from: "1.2.0"),
    .package(url: "https://github.com/rosecoder/retryable-task.git", from: "1.1.2"),
    .package(
      url: "https://github.com/rosecoder/google-cloud-service-context.git", from: "0.0.2"),
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.54.0"),
    .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.3.0"),
  ],
  targets: [
    .target(
      name: "GoogleCloudPubSub",
      dependencies: [
        .product(name: "Logging", package: "swift-log"),
        .product(name: "Tracing", package: "swift-distributed-tracing"),
        .product(
          name: "GoogleCloudServiceContext", package: "google-cloud-service-context"),
        .product(name: "GoogleCloudAuth", package: "google-cloud-auth-swift"),
        .product(name: "GoogleCloudAuthGRPC", package: "google-cloud-auth-swift"),
        .product(name: "RetryableTask", package: "retryable-task"),
        .product(name: "NIOHTTP1", package: "swift-nio"),
        .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
        .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
      ]
    ),
    .testTarget(
      name: "GoogleCloudPubSubTests",
      dependencies: [
        "GoogleCloudPubSub",
        .product(
          name: "ServiceLifecycleTestKit", package: "swift-service-lifecycle"),
      ]
    ),

    .target(
      name: "GoogleCloudPubSubTesting",
      dependencies: [
        "GoogleCloudPubSub"
      ]),
    .testTarget(
      name: "GoogleCloudPubSubTestingTests",
      dependencies: [
        "GoogleCloudPubSubTesting"
      ]),
  ]
)
