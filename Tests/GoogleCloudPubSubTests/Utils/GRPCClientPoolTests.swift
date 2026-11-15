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
    #expect(pool.unary === factory.clients[0])
    #expect(pool.activeStreamCounts == [])

    let held = (0..<80).map { _ in holdSlot(in: pool) }
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeStreamCounts == [80] }
    #expect(factory.clients.count == 2)

    let id = try await pool.withStreamSlot { $0.id }
    #expect(id == 2)
    #expect(factory.clients.count == 3)
    #expect(pool.activeStreamCounts == [80, 0])
  }

  @Test func shouldNeverAssignStreamsToUnaryClient() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(maxStreamsPerClient: 3, makeClient: factory.make)
    let ids = Mutex<[Int]>([])

    let held = (0..<7).map { _ in
      Task {
        try await pool.withStreamSlot { client in
          ids.withLock { $0.append(client.id) }
          try await Task.sleep(for: .seconds(3600))
        }
      }
    }
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeStreamCounts == [3, 3, 1] }

    #expect(!ids.withLock { $0 }.contains(pool.unary.id))
    #expect(ids.withLock { $0 }.sorted() == [1, 1, 1, 2, 2, 2, 3])
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
    #expect(id == 1)
    #expect(factory.clients.count == 3)
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
    try await waitUntil { factory.clients.count == 4 }
    try await waitUntil { factory.clients[3].didStartConnections }

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
    try await waitUntil { factory.clients.count == 4 }
    try await waitUntil { factory.clients.allSatisfy(\.didStartConnections) }

    pool.beginGracefulShutdown()
    try await runTask.cancellableValue
    #expect(pool.unary.didStopConnections)
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
    #expect(pool.activeStreamCounts == [])
  }

  @Test func shouldThrowWhenRunTwice() async throws {
    let pool = try GRPCClientPool(makeClient: FakeClientFactory().make)
    let runTask = Task { try await pool.run() }
    defer { runTask.cancel() }
    try await waitUntil { pool.unary.didStartConnections }

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
    #expect(pool.activeStreamCounts.count == factory.clients.count - 1)
  }

  // MARK: - Unary

  @Test func shouldCreateUnaryClientsOnlyWhenOthersAreFull() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxUnaryClients: 3, maxUnaryRPCsPerClient: 3, reservedHighPriorityRPCsPerClient: 1,
      makeClient: factory.make)
    #expect(pool.activeUnaryRPCCounts == [0])

    var held: [Task<Void, any Error>] = []
    defer { for task in held { task.cancel() } }
    for expected in [[1], [2], [2, 1], [2, 2], [2, 2, 1]] {
      held.append(holdUnarySlot(in: pool, .normal))
      try await waitUntil { pool.activeUnaryRPCCounts == expected }
    }
    #expect(factory.clients.count == 3)
    #expect(pool.activeStreamCounts == [])
  }

  @Test func shouldSpreadUnaryRPCsOverLeastBusyClient() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxUnaryClients: 2, maxUnaryRPCsPerClient: 3, reservedHighPriorityRPCsPerClient: 1,
      makeClient: factory.make)
    var held: [Task<Void, any Error>] = []
    defer { for task in held { task.cancel() } }
    for expected in [[1], [2], [2, 1]] {
      held.append(holdUnarySlot(in: pool, .normal))
      try await waitUntil { pool.activeUnaryRPCCounts == expected }
    }

    held[0].cancel()
    held[1].cancel()
    try await waitUntil { pool.activeUnaryRPCCounts == [0, 1] }
    held.append(holdUnarySlot(in: pool, .normal))
    try await waitUntil { pool.activeUnaryRPCCounts == [1, 1] }
  }

  @Test func shouldNeverShareClientsBetweenStreamsAndUnaryRPCs() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxStreamsPerClient: 1, maxUnaryClients: 3, maxUnaryRPCsPerClient: 2,
      reservedHighPriorityRPCsPerClient: 1, makeClient: factory.make)
    let streamIDs = Mutex<Set<Int>>([])
    let unaryIDs = Mutex<Set<Int>>([])

    let held =
      (0..<3).map { _ in
        Task {
          try await pool.withStreamSlot { client in
            streamIDs.withLock { _ = $0.insert(client.id) }
            try await Task.sleep(for: .seconds(3600))
          }
        }
      }
      + (0..<3).map { _ in
        Task {
          try await pool.withUnaryClient(.normal) { client in
            unaryIDs.withLock { _ = $0.insert(client.id) }
            try await Task.sleep(for: .seconds(3600))
          }
        }
      }
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeStreamCounts == [1, 1, 1] }
    try await waitUntil { pool.activeUnaryRPCCounts == [1, 1, 1] }

    #expect(streamIDs.withLock { $0 }.isDisjoint(with: unaryIDs.withLock { $0 }))
    #expect(unaryIDs.withLock { $0 }.contains(pool.unary.id))
  }

  @Test func shouldWaitForSlotWhenAllUnaryClientsAreFullAndServeInOrder() async throws {
    let pool = try GRPCClientPool(
      maxUnaryClients: 1, maxUnaryRPCsPerClient: 2, reservedHighPriorityRPCsPerClient: 1,
      makeClient: FakeClientFactory().make)
    let holder = holdUnarySlot(in: pool, .normal)
    defer { holder.cancel() }
    try await waitUntil { pool.activeUnaryRPCCounts == [1] }

    let order = Mutex<[Int]>([])
    let gates = [Gate(), Gate()]
    var waiting: [Task<Void, any Error>] = []
    for index in gates.indices {
      waiting.append(
        Task {
          try await pool.withUnaryClient(.normal) { _ in
            order.withLock { $0.append(index) }
            try await gates[index].wait()
          }
        })
      try await waitUntil { pool.waitingUnaryRPCCount == index + 1 }
    }

    holder.cancel()
    try await waitUntil { order.withLock { $0 } == [0] }
    #expect(pool.waitingUnaryRPCCount == 1)
    gates[0].open()
    try await waitUntil { order.withLock { $0 } == [0, 1] }
    gates[1].open()
    for task in waiting {
      try await task.value
    }
    #expect(pool.activeUnaryRPCCounts == [0])
  }

  @Test func shouldReserveSlotsForHighPriorityRPCs() async throws {
    let pool = try GRPCClientPool(
      maxUnaryClients: 2, maxUnaryRPCsPerClient: 3, reservedHighPriorityRPCsPerClient: 1,
      makeClient: FakeClientFactory().make)
    let held = (0..<5).map { _ in holdUnarySlot(in: pool, .normal) }
    defer { for task in held { task.cancel() } }
    try await waitUntil { pool.activeUnaryRPCCounts == [2, 2] && pool.waitingUnaryRPCCount == 1 }

    let acks = (0..<2).map { _ in holdUnarySlot(in: pool, .high) }
    defer { for task in acks { task.cancel() } }
    try await waitUntil { pool.activeUnaryRPCCounts == [3, 3] }
    #expect(pool.waitingUnaryRPCCount == 1)
  }

  @Test func shouldGiveFreedSlotsToHighPriorityRPCsFirst() async throws {
    let pool = try GRPCClientPool(
      maxUnaryClients: 1, maxUnaryRPCsPerClient: 2, reservedHighPriorityRPCsPerClient: 1,
      makeClient: FakeClientFactory().make)
    let normal = holdUnarySlot(in: pool, .normal)
    let high = holdUnarySlot(in: pool, .high)
    defer {
      normal.cancel()
      high.cancel()
    }
    try await waitUntil { pool.activeUnaryRPCCounts == [2] }

    let started = Mutex<[UnaryPriority]>([])
    let waitingNormal = Task {
      try await pool.withUnaryClient(.normal) { _ in started.withLock { $0.append(.normal) } }
    }
    try await waitUntil { pool.waitingUnaryRPCCount == 1 }
    let waitingHigh = Task {
      try await pool.withUnaryClient(.high) { _ in
        started.withLock { $0.append(.high) }
        try await Task.sleep(for: .seconds(3600))
      }
    }
    defer { waitingHigh.cancel() }
    try await waitUntil { pool.waitingUnaryRPCCount == 2 }

    high.cancel()
    try await waitUntil { started.withLock { $0 } == [.high] }
    #expect(pool.waitingUnaryRPCCount == 1)

    normal.cancel()
    waitingHigh.cancel()
    try await waitingNormal.value
    #expect(started.withLock { $0 } == [.high, .normal])
  }

  @Test func shouldStopWaitingForUnarySlotWhenCancelled() async throws {
    let pool = try GRPCClientPool(
      maxUnaryClients: 1, maxUnaryRPCsPerClient: 2, reservedHighPriorityRPCsPerClient: 1,
      makeClient: FakeClientFactory().make)
    let holder = holdUnarySlot(in: pool, .normal)
    defer { holder.cancel() }
    try await waitUntil { pool.activeUnaryRPCCounts == [1] }

    let waiting = holdUnarySlot(in: pool, .normal)
    try await waitUntil { pool.waitingUnaryRPCCount == 1 }
    waiting.cancel()
    await #expect(throws: CancellationError.self) { try await waiting.value }
    #expect(pool.waitingUnaryRPCCount == 0)
    #expect(pool.activeUnaryRPCCounts == [1])
  }

  @Test func shouldFailUnaryRPCsWaitingOrStartingAfterShutdown() async throws {
    let pool = try GRPCClientPool(
      maxUnaryClients: 1, maxUnaryRPCsPerClient: 2, reservedHighPriorityRPCsPerClient: 1,
      makeClient: FakeClientFactory().make)
    let holder = holdUnarySlot(in: pool, .normal)
    defer { holder.cancel() }
    try await waitUntil { pool.activeUnaryRPCCounts == [1] }
    let waiting = holdUnarySlot(in: pool, .normal)
    try await waitUntil { pool.waitingUnaryRPCCount == 1 }

    pool.beginGracefulShutdown()
    await #expect(throws: RuntimeError.self) { try await waiting.value }
    await #expect(throws: RuntimeError.self) {
      try await pool.withUnaryClient(.high) { _ in }
    }
  }

  @Test func shouldRunAndStopUnaryClientsCreatedOnDemand() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxUnaryClients: 2, maxUnaryRPCsPerClient: 2, reservedHighPriorityRPCsPerClient: 1,
      makeClient: factory.make)
    let runTask = Task { try await pool.run() }
    defer { runTask.cancel() }

    let held = (0..<2).map { _ in holdUnarySlot(in: pool, .normal) }
    defer { for task in held { task.cancel() } }
    try await waitUntil { factory.clients.count == 2 }
    try await waitUntil { factory.clients.allSatisfy(\.didStartConnections) }

    pool.beginGracefulShutdown()
    try await runTask.cancellableValue
    #expect(factory.clients.allSatisfy { $0.didStopConnections })
  }

  @Test func shouldNeverExceedUnaryRPCsPerClientUnderConcurrency() async throws {
    let factory = FakeClientFactory()
    let pool = try GRPCClientPool(
      maxUnaryClients: 3, maxUnaryRPCsPerClient: 5, reservedHighPriorityRPCsPerClient: 2,
      makeClient: factory.make)
    let inFlight = Mutex<[Int: (current: Int, peak: Int)]>([:])

    try await withThrowingDiscardingTaskGroup { group in
      for index in 0..<1_000 {
        group.addTask {
          try await pool.withUnaryClient(index.isMultiple(of: 4) ? .high : .normal) { client in
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
    #expect(peaks.allSatisfy { $0 <= 5 })
    #expect(factory.clients.count <= 3)
    #expect(pool.activeUnaryRPCCounts.allSatisfy { $0 == 0 })
    #expect(pool.waitingUnaryRPCCount == 0)
  }
}

private func holdSlot(in pool: GRPCClientPool<FakePooledClient>) -> Task<Void, any Error> {
  Task {
    try await pool.withStreamSlot { _ in try await Task.sleep(for: .seconds(3600)) }
  }
}

private func holdUnarySlot(
  in pool: GRPCClientPool<FakePooledClient>, _ priority: UnaryPriority
) -> Task<Void, any Error> {
  Task {
    try await pool.withUnaryClient(priority) { _ in try await Task.sleep(for: .seconds(3600)) }
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
