import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import GRPCProtobuf
import Logging
import SwiftProtobuf
import Synchronization
import Testing

@testable import GoogleCloudPubSub

/// Unary RPCs against a local server that, like Google front ends, allows 100 concurrent streams
/// per connection.
@Suite(.timeLimit(.minutes(2)))
struct UnaryConcurrencyIntegrationTests {

  @Test func shouldCompleteSustainedPublishesBeyondStreamLimitPromptly() async throws {
    let publishers = 150
    let body = Data(repeating: 1, count: 50_000)
    try await withPublisherServer(publishDelay: .milliseconds(20)) { service, server in
      let publisher = Publisher(
        projectID: "test", createTopicsIfNeeded: false, pubSubService: service)
      let topic = Topic<DataMessage>(name: "unary-concurrency")
      let latencies = Mutex<[Duration]>([])
      let end = ContinuousClock.now.advanced(by: .seconds(6))

      try await withThrowingDiscardingTaskGroup { group in
        for _ in 0..<publishers {
          group.addTask {
            while ContinuousClock.now < end {
              let start = ContinuousClock.now
              try await publisher.publish(to: topic, messages: [OutgoingDataMessage(body: body)])
              let latency = start.duration(to: .now)
              latencies.withLock { $0.append(latency) }
            }
          }
        }
      }

      // Before swift-nio-http2 1.46.0, some RPCs waited until the load stopped.
      let slowest = try #require(latencies.withLock { $0.max() })
      #expect(slowest < .seconds(3))
      #expect(server.peakRPCsPerConnection.allSatisfy { $0 <= 90 })
      #expect(server.peakRPCsPerConnection.count >= 2)
    }
  }

  @Test func shouldNotDelayAcknowledgementsBehindPublishes() async throws {
    try await withPublisherServer(publishDelay: .seconds(1), maxUnaryConnections: 1) {
      service, server in
      let publisher = Publisher(
        projectID: "test", createTopicsIfNeeded: false, pubSubService: service)
      let topic = Topic<DataMessage>(name: "unary-concurrency")
      let publishes = Task {
        try await withThrowingDiscardingTaskGroup { group in
          for _ in 0..<300 {
            group.addTask {
              try await publisher.publish(to: topic, messages: [OutgoingDataMessage(body: Data())])
            }
          }
        }
      }
      defer { publishes.cancel() }
      try await waitUntil { server.peakRPCsPerConnection.first ?? 0 >= 80 }

      let transport = GRPCAckTransport(
        pubSubService: service, subscriptionID: "projects/test/subscriptions/test")
      let start = ContinuousClock.now
      try await transport.acknowledge([AckID("a")])
      #expect(start.duration(to: .now) < .milliseconds(500))
      try await publishes.value
    }
  }

  /// Runs `body` with a service connected to a local server whose publishes take `publishDelay`
  /// and whose acknowledgements succeed right away.
  private func withPublisherServer(
    publishDelay: Duration,
    maxUnaryConnections: Int = 4,
    _ body: (PubSubService, FakePubSubServer) async throws -> Void
  ) async throws {
    let fake = FakePubSubServer(publishDelay: publishDelay)
    var config = HTTP2ServerTransport.Posix.Config.defaults
    config.http2.maxConcurrentStreams = 100
    let server = GRPCServer(
      transport: HTTP2ServerTransport.Posix(
        address: .ipv4(host: "127.0.0.1", port: 0), transportSecurity: .plaintext, config: config),
      services: [fake])
    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { try await Self.throwingUnlessCancelled(server.serve) }
      defer { group.cancelAll() }
      let port = try #require(try await server.listeningAddress?.ipv4?.port)
      let service = try PubSubService(
        maxUnaryConnections: maxUnaryConnections,
        emulatorHost: "127.0.0.1:\(port)", callTimeout: .seconds(30))
      // Stops the clients when done rather than letting them linger after the last RPC.
      try await service.withClientInUse(.service) { _ in
        try await body(service, fake)
      }
    }
  }

  private static func throwingUnlessCancelled(_ body: () async throws -> Void) async throws {
    do {
      try await body()
    } catch {
      if !Task.isCancelled {
        throw error
      }
    }
  }
}

private final class FakePubSubServer: RegistrableRPCService {

  private let publishDelay: Duration
  private let rpcsPerConnection = Mutex<[String: (current: Int, peak: Int)]>([:])

  init(publishDelay: Duration) {
    self.publishDelay = publishDelay
  }

  /// Most publishes in flight at once on each connection.
  var peakRPCsPerConnection: [Int] {
    rpcsPerConnection.withLock { $0.values.map(\.peak) }
  }

  func registerMethods<Transport: ServerTransport>(with router: inout RPCRouter<Transport>) {
    router.registerHandler(
      forMethod: Google_Pubsub_V1_Publisher.Method.Publish.descriptor,
      deserializer: ProtobufDeserializer<Google_Pubsub_V1_PublishRequest>(),
      serializer: ProtobufSerializer<Google_Pubsub_V1_PublishResponse>(),
      handler: { request, context in
        try await self.publish(request, connection: context.remotePeer)
      })
    router.registerHandler(
      forMethod: Google_Pubsub_V1_Subscriber.Method.Acknowledge.descriptor,
      deserializer: ProtobufDeserializer<Google_Pubsub_V1_AcknowledgeRequest>(),
      serializer: ProtobufSerializer<Google_Protobuf_Empty>(),
      handler: { request, _ in
        for try await _ in request.messages {}
        return StreamingServerResponse(single: ServerResponse(message: Google_Protobuf_Empty()))
      })
  }

  private func publish(
    _ request: StreamingServerRequest<Google_Pubsub_V1_PublishRequest>, connection: String
  ) async throws -> StreamingServerResponse<Google_Pubsub_V1_PublishResponse> {
    var messageCount = 0
    for try await message in request.messages {
      messageCount += message.messages.count
    }
    rpcsPerConnection.withLock {
      let current = ($0[connection]?.current ?? 0) + 1
      $0[connection] = (current, max(current, $0[connection]?.peak ?? 0))
    }
    defer { rpcsPerConnection.withLock { $0[connection]!.current -= 1 } }
    try await Task.sleep(for: publishDelay)
    let response = Google_Pubsub_V1_PublishResponse.with {
      $0.messageIds = (0..<messageCount).map(String.init)
    }
    return StreamingServerResponse(single: ServerResponse(message: response))
  }
}
