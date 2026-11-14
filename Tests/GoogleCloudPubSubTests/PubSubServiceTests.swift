import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import NIOCore
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PubSubServiceTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldBeginShutdownOnlyAfterLastClientUseEnds() async throws {
    let service = try PubSubService()
    let firstGate = Gate()
    let secondGate = Gate()
    let first = Task { try await service.withClientInUse { _ in try await firstGate.wait() } }
    defer { first.cancel() }
    let second = Task {
      try await service.withClientInUse { clients in
        try await secondGate.wait()
        return clients
      }
    }
    defer { second.cancel() }
    try await waitUntil { service.clientUsage.users == 2 }

    firstGate.open()
    try await first.cancellableValue
    #expect(!service.clientUsage.hasBegunShutdown)
    try await #require(service.clientUsage.current).pool.withStreamSlot { _ in }

    secondGate.open()
    let stopped = try await second.cancellableValue
    #expect(service.clientUsage.hasBegunShutdown)
    await #expect(throws: RuntimeError.self) {
      try await stopped.pool.withStreamSlot { _ in }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStartNewGenerationWhenUsedAfterShutdown() async throws {
    let service = try PubSubService()
    let stopped = try await service.withClientInUse { $0 }
    #expect(service.clientUsage.hasBegunShutdown)

    let restarted = try await service.withClientInUse { clients in
      #expect(service.clientUsage.users == 1)
      #expect(!service.clientUsage.hasBegunShutdown)
      try await clients.pool.withStreamSlot { _ in }
      return clients
    }

    #expect(restarted.primary !== stopped.primary)
    #expect(service.clientUsage.startedGenerations == 2)
    #expect(service.clientUsage.users == 0)
    #expect(service.clientUsage.hasBegunShutdown)
    await #expect(throws: RuntimeError.self) {
      try await restarted.pool.withStreamSlot { _ in }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStartNewGenerationWithoutStreamsOfPreviousOne() async throws {
    let service = try PubSubService(maxStreamsPerConnection: 1)
    let gate = Gate()
    let streams = (0..<2).map { _ in
      Task {
        try await service.withClientInUse { clients in
          try await clients.pool.withStreamSlot { _ in try await gate.wait() }
        }
      }
    }
    defer { for stream in streams { stream.cancel() } }
    try await waitUntil { service.activeStreamCounts == [1, 1] }
    gate.open()
    for stream in streams {
      try await stream.cancellableValue
    }
    #expect(service.activeStreamCounts == [])

    let counts = try await service.withClientInUse { $0.pool.activeStreamCounts }
    #expect(counts == [0])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStartGenerationForEachSequentialUse() async throws {
    let service = try PubSubService()
    // Holds the clients so that their identifiers aren't reused.
    var primaries: [GRPCClient<HTTP2ClientTransport.Posix>] = []
    for _ in 0..<5 {
      let primary = try await service.withClientInUse { clients in
        try await clients.pool.withStreamSlot { $0 }
      }
      primaries.append(primary)
    }
    #expect(Set(primaries.map(ObjectIdentifier.init)).count == 5)
    #expect(service.clientUsage.startedGenerations == 5)
    #expect(service.clientUsage.users == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldEndClientUseWhenBodyThrows() async throws {
    let service = try PubSubService()
    let gate = Gate()
    let holder = Task { try await service.withClientInUse { _ in try await gate.wait() } }
    defer { holder.cancel() }
    try await waitUntil { service.clientUsage.users == 1 }

    await #expect(throws: CancellationError.self) {
      try await service.withClientInUse { _ in
        #expect(service.clientUsage.users == 2)
        throw CancellationError()
      }
    }
    #expect(service.clientUsage.users == 1)
    #expect(!service.clientUsage.hasBegunShutdown)

    gate.open()
    try await holder.cancellableValue
    #expect(service.clientUsage.hasBegunShutdown)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRethrowBodyErrorOfLastClientUse() async throws {
    let service = try PubSubService()
    struct BodyError: Error {}

    await #expect(throws: BodyError.self) {
      try await service.withClientInUse { _ in throw BodyError() }
    }
    #expect(service.clientUsage.hasBegunShutdown)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldOpenAnotherConnectionForStreamsBeyondMaxStreamsPerConnection() async throws {
    let service = try PubSubService(maxStreamsPerConnection: 2)
    let gate = Gate()
    let clients = Mutex<[ObjectIdentifier]>([])
    let streams = (0..<3).map { _ in
      Task {
        try await service.withClientInUse { pooled in
          try await pooled.pool.withStreamSlot { client in
            clients.withLock { $0.append(ObjectIdentifier(client)) }
            try await gate.wait()
          }
          return pooled
        }
      }
    }
    defer { for stream in streams { stream.cancel() } }
    try await waitUntil { service.activeStreamCounts == [2, 1] }

    let primary = ObjectIdentifier(try #require(service.clientUsage.current).primary)
    #expect(clients.withLock { $0 }.count { $0 == primary } == 2)
    #expect(Set(clients.withLock { $0 }).count == 2)

    gate.open()
    var stopped: [PubSubService.Clients] = []
    for stream in streams {
      stopped.append(try await stream.cancellableValue)
    }
    await #expect(throws: RuntimeError.self) {
      try await stopped[0].pool.withStreamSlot { _ in }
    }
  }

  // MARK: - Stopping

  /// A server that accepts the TCP connection but never completes the HTTP/2 handshake keeps the
  /// connection attempt in progress, as an overloaded server does.
  @Test(.timeLimit(.minutes(1)))
  func shouldNotWaitForConnectionAttemptWhenStopping() async throws {
    let server = try UnresponsiveServer()
    defer { server.close() }
    let connections = Mutex(0)
    let service = try PubSubService(
      channelDebuggingCallbacks: .init(
        onCreateTCPConnection: { channel in
          connections.withLock { $0 += 1 }
          return channel.eventLoop.makeSucceededVoidFuture()
        },
        onCreateHTTP2Stream: nil),
      emulatorHost: "127.0.0.1:\(server.port)")
    let gate = Gate()
    let user = Task { try await service.withClientInUse { _ in try await gate.wait() } }
    defer { user.cancel() }
    try await waitUntil { connections.withLock { $0 } > 0 }

    let start = ContinuousClock.now
    gate.open()
    try await user.cancellableValue
    #expect(start.duration(to: .now) < .seconds(1))
  }

  /// Such as a publish from outside a service while the service shuts down.
  @Test(.timeLimit(.minutes(1)))
  func shouldKeepClientsRunningForRPCOutlivingOtherUsers() async throws {
    let service = try PubSubService()
    let userGate = Gate()
    let rpcGate = Gate()
    let user = Task { try await service.withClientInUse { _ in try await userGate.wait() } }
    defer { user.cancel() }
    let rpc = Task {
      try await service.withClientInUse { clients in
        try await rpcGate.wait()
        try await clients.pool.withStreamSlot { _ in }
      }
    }
    defer { rpc.cancel() }
    try await waitUntil { service.clientUsage.users == 2 }

    userGate.open()
    try await user.cancellableValue
    #expect(!service.clientUsage.hasBegunShutdown)

    rpcGate.open()
    try await rpc.cancellableValue
    #expect(service.clientUsage.hasBegunShutdown)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStopClientsRightAwayWhenLastUserIsCancelledDuringConnectionAttempt() async throws {
    let server = try UnresponsiveServer()
    defer { server.close() }
    let service = try PubSubService(emulatorHost: "127.0.0.1:\(server.port)")
    let hasStartedRPC = Mutex(false)
    let user = Task {
      try await service.withClientInUse { clients in
        hasStartedRPC.withLock { $0 = true }
        _ = try await clients.publisher.getTopic(.with { $0.topic = "projects/p/topics/t" })
      }
    }
    defer { user.cancel() }
    try await waitUntil { hasStartedRPC.withLock { $0 } }

    let start = ContinuousClock.now
    user.cancel()
    _ = await user.result
    #expect(start.duration(to: .now) < .seconds(1))
    #expect(service.clientUsage.hasBegunShutdown)
  }
}

/// Listens on a loopback port without ever accepting, so the kernel completes TCP handshakes but
/// nothing is ever sent back.
private final class UnresponsiveServer: Sendable {

  let port: Int
  private let descriptor: Int32

  init() throws {
    let descriptor = socket(AF_INET, SOCK_STREAM, 0)
    var address = sockaddr_in()
    address.sin_family = sa_family_t(AF_INET)
    address.sin_addr.s_addr = inet_addr("127.0.0.1")
    var length = socklen_t(MemoryLayout<sockaddr_in>.size)
    let isListening = withUnsafeMutablePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        bind(descriptor, $0, length) == 0 && listen(descriptor, 16) == 0
          && getsockname(descriptor, $0, &length) == 0
      }
    }
    guard isListening else {
      Darwin.close(descriptor)
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    self.descriptor = descriptor
    port = Int(UInt16(bigEndian: address.sin_port))
  }

  func close() {
    Darwin.close(descriptor)
  }
}

@Suite struct ClientUsageTests {

  private struct StartError: Error {}

  @Test func shouldStartGenerationForFirstUserOnly() {
    var usage = PubSubService.ClientUsage<Int>()
    #expect(usage.retain { 1 } == 1)
    #expect(usage.retain { Issue.record("Started a second generation."); return 2 } == 1)
    #expect(usage.users == 2)
    #expect(usage.startedGenerations == 1)
    #expect(!usage.hasBegunShutdown)
  }

  @Test func shouldReturnGenerationToShutDownOnlyFromLastUser() {
    var usage = PubSubService.ClientUsage<Int>()
    _ = usage.retain { 1 }
    _ = usage.retain { 2 }

    #expect(usage.release() == nil)
    #expect(!usage.hasBegunShutdown)
    #expect(usage.release() == 1)
    #expect(usage.users == 0)
    #expect(usage.current == nil)
    #expect(usage.hasBegunShutdown)
  }

  @Test func shouldStartNewGenerationAfterLastUserLeft() {
    var usage = PubSubService.ClientUsage<Int>()
    _ = usage.retain { 1 }
    _ = usage.release()

    // The previous generation may still be shutting down.
    #expect(usage.retain { 2 } == 2)
    #expect(usage.retain { 3 } == 2)
    #expect(usage.startedGenerations == 2)
    #expect(!usage.hasBegunShutdown)
    #expect(usage.release() == nil)
    #expect(usage.release() == 2)
  }

  @Test func shouldStayUnchangedWhenStartingGenerationFails() {
    var usage = PubSubService.ClientUsage<Int>()
    #expect(throws: StartError.self) {
      try usage.retain { throw StartError() }
    }
    #expect(usage.users == 0)
    #expect(usage.startedGenerations == 0)
    #expect(!usage.hasBegunShutdown)
    #expect(usage.retain { 1 } == 1)
  }
}
