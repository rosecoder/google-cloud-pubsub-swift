import GRPCCore
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.timeLimit(.minutes(1)))
struct GRPCClientPoolTests {

  @Test func shouldAssignEightyStreamsPerClientAndCreateClientLazily() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(makeClient: factory.make)
    #expect(factory.clients.count == 1)
    #expect(pool.primary === factory.clients[0])

    let held = (0..<80).map { _ in holdSlot(in: pool) }
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeStreamCounts == [80] }
    #expect(factory.clients.count == 1)

    let id = try await pool.withStreamSlot { $0.id }
    #expect(id == 1)
    #expect(factory.clients.count == 2)
    #expect(pool.activeStreamCounts == [80, 0])
  }

  @Test func shouldReuseReleasedSlotsAtLowestIndex() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(maxStreamsPerClient: 2, makeClient: factory.make)

    var held: [Task<Void, any Error>] = []
    defer { for task in held { task.cancel() } }
    for expected in [[1], [2], [2, 1]] {
      held.append(holdSlot(in: pool))
      try await waitUntil { pool.activeStreamCounts == expected }
    }

    held[0].cancel()
    try await waitUntil { pool.activeStreamCounts == [1, 1] }

    let id = try await pool.withStreamSlot { $0.id }
    #expect(id == 0)
    #expect(factory.clients.count == 2)
  }

  @Test func shouldReleaseSlotWhenBodyThrows() async throws {
    let pool = try GRPCClientPool(makeClient: FakeClientFactory().make)
    struct BodyError: Error {}

    await #expect(throws: BodyError.self) {
      try await pool.withStreamSlot { _ in throw BodyError() }
    }
    #expect(pool.activeStreamCounts == [0])
  }

  @Test func shouldRunClientsCreatedBeforeAndAfterRunStarted() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(maxStreamsPerClient: 1, makeClient: factory.make)

    var held = [holdSlot(in: pool), holdSlot(in: pool)]
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeStreamCounts == [1, 1] }

    let runTask = Task { try await pool.run() }
    defer { runTask.cancel() }
    try await waitUntil { factory.clients.allSatisfy(\.didStartConnections) }

    held.append(holdSlot(in: pool))
    try await waitUntil { factory.clients.count == 3 }
    try await waitUntil { factory.clients[2].didStartConnections }

    pool.beginGracefulShutdown()
    try await runTask.cancellableValue
  }

  @Test func shouldStopAllClientsAndReturnFromRunOnShutdown() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(maxStreamsPerClient: 1, makeClient: factory.make)
    let runTask = Task { try await pool.run() }
    defer { runTask.cancel() }

    let held = (0..<3).map { _ in holdSlot(in: pool) }
    defer { for task in held { task.cancel() } }
    try await waitUntil { factory.clients.count == 3 }
    try await waitUntil { factory.clients.allSatisfy(\.didStartConnections) }

    pool.beginGracefulShutdown()
    try await runTask.cancellableValue
    #expect(factory.clients.allSatisfy { $0.didStopConnections })
  }

  @Test func shouldReturnFromRunWhenShutDownBeforeRunStarted() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(makeClient: factory.make)

    pool.beginGracefulShutdown()
    try await pool.run()
    #expect(!factory.clients[0].didStartConnections)
  }

  @Test func shouldThrowWhenAcquiringSlotAfterShutdown() async throws {
    let pool = try GRPCClientPool(makeClient: FakeClientFactory().make)
    pool.beginGracefulShutdown()

    await #expect(throws: RuntimeError.self) {
      try await pool.withStreamSlot { _ in }
    }
    #expect(pool.activeStreamCounts == [0])
  }

  @Test func shouldThrowWhenRunTwice() async throws {
    let pool = try GRPCClientPool(makeClient: FakeClientFactory().make)
    let runTask = Task { try await pool.run() }
    defer { runTask.cancel() }
    try await waitUntil { pool.primary.didStartConnections }

    await #expect(throws: RuntimeError.self) {
      try await pool.run()
    }

    pool.beginGracefulShutdown()
    try await runTask.cancellableValue
  }

  @Test func shouldNeverExceedStreamsPerClientUnderConcurrency() async throws {
    let maxStreamsPerClient = 5
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxStreamsPerClient: maxStreamsPerClient, makeClient: factory.make)
    let inFlight = Mutex<[Int: (current: Int, peak: Int)]>([:])

    try await withThrowingDiscardingTaskGroup { group in
      for _ in 0..<1_000 {
        group.addTask {
          try await pool.withStreamSlot { client in
            inFlight.withLock {
              let current = ($0[client.id]?.current ?? 0) + 1
              $0[client.id] = (current, max(current, $0[client.id]?.peak ?? 0))
            }
            for _ in 0..<3 { await Task.yield() }
            inFlight.withLock { $0[client.id]!.current -= 1 }
          }
        }
      }
    }

    let peaks = inFlight.withLock { $0.values.map(\.peak) }
    #expect(peaks.allSatisfy { $0 <= maxStreamsPerClient })
    #expect(pool.activeStreamCounts.allSatisfy { $0 == 0 })
    #expect(pool.activeStreamCounts.count == factory.clients.count)
  }
}

private func holdSlot(in pool: GRPCClientPool<FakePooledClient>) -> Task<Void, any Error> {
  Task {
    try await pool.withStreamSlot { _ in try await Task.sleep(for: .seconds(3600)) }
  }
}

private final class FakeClientFactory: Sendable {

  private let made = Mutex<[FakePooledClient]>([])

  var clients: [FakePooledClient] {
    made.withLock { $0 }
  }

  @Sendable func make() -> FakePooledClient {
    made.withLock {
      let client = FakePooledClient(id: $0.count)
      $0.append(client)
      return client
    }
  }
}

/// Mirrors `GRPCClient`'s lifecycle: shutting down a client that hasn't started stops it
/// immediately and a later `runConnections()` throws.
final class FakePooledClient: PooledClient {

  enum State {
    case notStarted
    case running
    case stopping
    case stopped
  }

  let id: Int
  private let state = Mutex(State.notStarted)
  private let didStart = Mutex(false)
  private let shutdown = AsyncStream.makeStream(of: Void.self)

  init(id: Int) {
    self.id = id
  }

  var didStartConnections: Bool {
    didStart.withLock { $0 }
  }

  var didStopConnections: Bool {
    state.withLock {
      if case .stopped = $0 { true } else { false }
    }
  }

  func runConnections() async throws {
    try state.withLock {
      guard case .notStarted = $0 else {
        throw RuntimeError(code: .clientIsStopped, message: "Fake client \(id) is stopped.")
      }
      $0 = .running
    }
    didStart.withLock { $0 = true }
    for await _ in shutdown.stream {}
    state.withLock { $0 = .stopped }
  }

  func beginGracefulShutdown() {
    state.withLock {
      switch $0 {
      case .notStarted: $0 = .stopped
      case .running: $0 = .stopping
      case .stopping, .stopped: break
      }
    }
    shutdown.continuation.finish()
  }
}
