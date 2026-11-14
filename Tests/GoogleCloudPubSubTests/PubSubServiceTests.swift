import Foundation
import GRPCCore
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
    let first = Task { try await service.withClientInUse { await firstGate.wait() } }
    let second = Task { try await service.withClientInUse { await secondGate.wait() } }
    try await waitUntil { service.clientUsage.users == 2 }

    firstGate.open()
    try await first.value
    #expect(!service.clientUsage.hasBegunShutdown)
    try await service.pool.withStreamSlot { _ in }

    secondGate.open()
    try await second.value
    #expect(service.clientUsage.hasBegunShutdown)
    await #expect(throws: RuntimeError.self) {
      try await service.pool.withStreamSlot { _ in }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldThrowWithoutRunningBodyWhenUsedAfterShutdown() async throws {
    let service = try PubSubService()
    try await service.withClientInUse {}

    let error = await #expect(throws: RuntimeError.self) {
      try await service.withClientInUse {
        Issue.record("Body ran after the clients shut down.")
      }
    }
    #expect(error?.code == .clientIsStopped)
    #expect(service.clientUsage.users == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldEndClientUseWhenBodyThrows() async throws {
    let service = try PubSubService()
    let gate = Gate()
    let holder = Task { try await service.withClientInUse { await gate.wait() } }
    try await waitUntil { service.clientUsage.users == 1 }

    await #expect(throws: CancellationError.self) {
      try await service.withClientInUse {
        #expect(service.clientUsage.users == 2)
        throw CancellationError()
      }
    }
    #expect(service.clientUsage.users == 1)
    #expect(!service.clientUsage.hasBegunShutdown)

    gate.open()
    try await holder.value
    #expect(service.clientUsage.hasBegunShutdown)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRethrowBodyErrorOfLastClientUse() async throws {
    let service = try PubSubService()
    struct BodyError: Error {}

    await #expect(throws: BodyError.self) {
      try await service.withClientInUse { throw BodyError() }
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
        try await service.withClientInUse {
          try await service.pool.withStreamSlot { client in
            clients.withLock { $0.append(ObjectIdentifier(client)) }
            await gate.wait()
          }
        }
      }
    }
    try await waitUntil { service.pool.activeStreamCounts == [2, 1] }

    let primary = ObjectIdentifier(service.grpcClient)
    #expect(clients.withLock { $0 }.count { $0 == primary } == 2)
    #expect(Set(clients.withLock { $0 }).count == 2)

    gate.open()
    for stream in streams {
      try await stream.value
    }
    await #expect(throws: RuntimeError.self) {
      try await service.pool.withStreamSlot { _ in }
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
    let user = Task { try await service.withClientInUse { await gate.wait() } }
    try await waitUntil { connections.withLock { $0 } > 0 }

    let start = ContinuousClock.now
    gate.open()
    try await user.value
    #expect(start.duration(to: .now) < .seconds(1))
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldLetRPCsInFlightFinishBeforeStoppingClients() async throws {
    let service = try PubSubService()
    let userGate = Gate()
    let rpcGate = Gate()
    let user = Task { try await service.withClientInUse { await userGate.wait() } }
    let rpc = Task { await service.withRPC { await rpcGate.wait() } }
    try await waitUntil { service.clientUsage.users == 1 && service.rpcsInFlight == 1 }
    let hasReturned = Mutex(false)
    let returned = Task {
      try await user.value
      hasReturned.withLock { $0 = true }
    }

    userGate.open()
    try await waitUntil { service.clientUsage.hasBegunShutdown }
    try await Task.sleep(for: .milliseconds(100))
    #expect(!hasReturned.withLock { $0 })

    rpcGate.open()
    await rpc.value
    try await returned.value
    #expect(service.rpcsInFlight == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStopClientsRightAwayWhenLastUserIsCancelled() async throws {
    let service = try PubSubService()
    let rpcGate = Gate()
    let rpc = Task { await service.withRPC { await rpcGate.wait() } }
    try await waitUntil { service.rpcsInFlight == 1 }
    let user = Task {
      try await service.withClientInUse { try? await Task.sleep(for: .seconds(3_600)) }
    }
    try await waitUntil { service.clientUsage.users == 1 }

    let start = ContinuousClock.now
    user.cancel()
    try await user.value
    #expect(start.duration(to: .now) < .seconds(1))
    rpcGate.open()
    await rpc.value
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
