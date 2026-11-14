import GRPCCore
import GRPCNIOTransportHTTP2Posix
import GRPCProtobuf
import Logging
import SwiftProtobuf
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.timeLimit(.minutes(1)))
struct GRPCAckTransportTests {

  @Test func shouldReportTimedOutRPCsAsTimeouts() async throws {
    try await withSubscriberServer(ignoringFirstCalls: .max) { transport, _ in
      let start = ContinuousClock.now
      let error = await #expect(throws: RPCError.self) {
        try await transport.acknowledge([AckID("a")])
      }
      #expect(start.duration(to: .now) < .seconds(5))
      #expect(error?.code == .unknown)
      #expect(error?.cause is CancellationError)
      #expect(error.map { AckFailure($0, isSenderCancelled: false) } == .timedOut)

      await #expect(throws: RPCError.self) {
        try await transport.modifyAckDeadline([AckID("a")], to: .nack)
      }
    }
  }

  @Test func shouldRetryTimedOutAck() async throws {
    try await withSubscriberServer(ignoringFirstCalls: 1) { transport, service in
      var configuration = AckBatcher<ContinuousClock>.Configuration()
      configuration.maxDelay = .milliseconds(10)
      configuration.retryBackoff = ExponentialBackoff(
        initial: .milliseconds(10), maximum: .milliseconds(10))
      let batcher = AckBatcher(
        transport: transport, configuration: configuration, logger: Logger(label: "test"))

      try await withThrowingDiscardingTaskGroup { group in
        group.addTask { await batcher.run() }
        defer { batcher.finish() }
        try await batcher.ack(AckID("a"))
      }
      #expect(service.calls.withLock { $0 } == 2)
    }
  }

  /// Runs `body` with a transport to a local server whose first `ignoredCalls` acks and deadline
  /// modifications never get a response, and whose later ones succeed. RPCs time out after
  /// 200 milliseconds.
  private func withSubscriberServer(
    ignoringFirstCalls ignoredCalls: Int,
    _ body: (GRPCAckTransport, SubscriberService) async throws -> Void
  ) async throws {
    let service = SubscriberService(ignoredCalls: ignoredCalls)
    let server = GRPCServer(
      transport: .http2NIOPosix(
        address: .ipv4(host: "127.0.0.1", port: 0), transportSecurity: .plaintext),
      services: [service])
    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { try await Self.throwingUnlessCancelled(server.serve) }
      // Ignored calls never finish, so a graceful shutdown would wait for them.
      defer { group.cancelAll() }
      let port = try #require(try await server.listeningAddress?.ipv4?.port)
      let pubSubService = try PubSubService(
        emulatorHost: "127.0.0.1:\(port)", callTimeout: .milliseconds(200))
      // Stops the clients when done rather than letting them linger after the last RPC.
      try await pubSubService.withClientInUse(.service) { _ in
        try await body(
          GRPCAckTransport(
            pubSubService: pubSubService, subscriptionID: "projects/test/subscriptions/test"),
          service)
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

private final class SubscriberService: RegistrableRPCService {

  let calls = Mutex(0)
  private let ignoredCalls: Int

  init(ignoredCalls: Int) {
    self.ignoredCalls = ignoredCalls
  }

  func registerMethods<Transport: ServerTransport>(with router: inout RPCRouter<Transport>) {
    router.registerHandler(
      forMethod: Google_Pubsub_V1_Subscriber.Method.Acknowledge.descriptor,
      deserializer: ProtobufDeserializer<Google_Pubsub_V1_AcknowledgeRequest>(),
      serializer: ProtobufSerializer<Google_Protobuf_Empty>(),
      handler: { _, _ in try await self.respond() })
    router.registerHandler(
      forMethod: Google_Pubsub_V1_Subscriber.Method.ModifyAckDeadline.descriptor,
      deserializer: ProtobufDeserializer<Google_Pubsub_V1_ModifyAckDeadlineRequest>(),
      serializer: ProtobufSerializer<Google_Protobuf_Empty>(),
      handler: { _, _ in try await self.respond() })
  }

  private func respond() async throws -> StreamingServerResponse<Google_Protobuf_Empty> {
    let call = calls.withLock { calls in
      calls += 1
      return calls
    }
    if call <= ignoredCalls {
      try await Task.sleep(for: .seconds(3_600))
    }
    return StreamingServerResponse(single: ServerResponse(message: Google_Protobuf_Empty()))
  }
}
