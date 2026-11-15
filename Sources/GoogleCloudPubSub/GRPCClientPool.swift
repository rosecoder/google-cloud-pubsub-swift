import DequeModule
import GRPCCore
import Synchronization

protocol PooledClient: Sendable {
  func runConnections() async throws
  func beginGracefulShutdown()
}

extension GRPCClient: PooledClient {}

/// How urgently a unary RPC needs a client while the unary clients are busy.
enum UnaryPriority: Sendable {

  /// Acknowledgements and ack deadline modifications. May use stream slots that ``normal`` RPCs
  /// can't and are given freed slots first, so that publishing can't delay them.
  case high

  /// Publishing and administrative RPCs.
  case normal
}

/// Keeps unary RPCs and long-lived streams on separate clients (and therefore HTTP/2 connections),
/// and spreads each over multiple clients.
///
/// Servers cap concurrent streams per connection (Google front ends commonly at 100) and opening a
/// stream beyond that limit queues silently instead of failing, with the queued time counting
/// towards the RPC's timeout. Neither the transport nor the load balancers open extra connections
/// based on stream count, so the pool does it by creating additional clients when every streaming
/// client holds `maxStreamsPerClient` streams. Unary RPCs, such as acknowledgements, would
/// otherwise queue behind the streams sharing their connection.
///
/// Unary RPCs likewise get up to `maxUnaryClients` clients, each carrying at most
/// `maxUnaryRPCsPerClient` of them. Beyond that, RPCs wait in the pool, where waiting doesn't count
/// towards their timeout.
final class GRPCClientPool<Client: PooledClient>: Sendable {

  /// First client used for unary RPCs. Never carries streams of ``withStreamSlot(_:)``.
  let unary: Client

  private let maxStreamsPerClient: Int
  private let maxUnaryClients: Int
  private let maxUnaryRPCsPerClient: Int
  private let maxNormalUnaryRPCsPerClient: Int
  private let makeClient: @Sendable () throws -> Client
  private let state: Mutex<State>
  private let newClients: AsyncStream<Client>
  private let newClientsContinuation: AsyncStream<Client>.Continuation

  private struct State {
    var slots: [(client: Client, activeStreams: Int)] = []
    var unarySlots: [(client: Client, activeRPCs: Int)]
    var unaryWaiters: [UnaryPriority: Deque<UnaryWaiter>] = [:]
    var nextWaiterID = 0
    var isRunning = false
    var isClosed = false
  }

  private struct UnaryWaiter {
    let id: Int
    let continuation: CheckedContinuation<(index: Int, client: Client), any Error>
  }

  /// - Parameter maxStreamsPerClient: Should stay below the server's concurrent stream limit with
  ///   headroom for streams that are still closing while their replacements open.
  /// - Parameter maxUnaryClients: Clients for unary RPCs, created as the RPCs in flight need them.
  /// - Parameter maxUnaryRPCsPerClient: Like `maxStreamsPerClient`, for unary RPCs.
  /// - Parameter reservedHighPriorityRPCsPerClient: Unary RPCs per client only
  ///   ``UnaryPriority/high`` RPCs may use.
  /// - Parameter makeClient: Called while holding the pool's lock; must not call back into the pool.
  init(
    maxStreamsPerClient: Int = 80,
    maxUnaryClients: Int = 4,
    maxUnaryRPCsPerClient: Int = 90,
    reservedHighPriorityRPCsPerClient: Int = 10,
    makeClient: @escaping @Sendable () throws -> Client
  ) throws {
    precondition(maxStreamsPerClient > 0, "maxStreamsPerClient must be positive")
    precondition(maxUnaryClients > 0, "maxUnaryClients must be positive")
    precondition(
      (0..<maxUnaryRPCsPerClient).contains(reservedHighPriorityRPCsPerClient),
      "reservedHighPriorityRPCsPerClient must leave room for other unary RPCs")

    let unary = try makeClient()
    self.unary = unary
    self.maxStreamsPerClient = maxStreamsPerClient
    self.maxUnaryClients = maxUnaryClients
    self.maxUnaryRPCsPerClient = maxUnaryRPCsPerClient
    self.maxNormalUnaryRPCsPerClient = maxUnaryRPCsPerClient - reservedHighPriorityRPCsPerClient
    self.makeClient = makeClient
    self.state = Mutex(State(unarySlots: [(unary, 0)]))
    (newClients, newClientsContinuation) = AsyncStream.makeStream(of: Client.self)
    newClientsContinuation.yield(unary)
  }

  /// Runs the connections of every client in the pool, including clients created after this
  /// method was called. Returns once ``beginGracefulShutdown()`` has been called and all clients
  /// have stopped.
  func run() async throws {
    try state.withLock { state in
      if state.isRunning {
        throw RuntimeError(
          code: .clientIsAlreadyRunning,
          message: "The client pool is already running and can only be started once."
        )
      }
      state.isRunning = true
    }

    try await withThrowingDiscardingTaskGroup { group in
      for await client in newClients {
        group.addTask {
          do {
            try await client.runConnections()
          } catch let error as RuntimeError where error.code == .clientIsStopped {
            // A client shut down before its connections started moves straight to stopped.
          }
        }
      }
    }
  }

  /// Executes `body` with a streaming client that has a free stream slot, holding the slot until
  /// `body` returns or throws. The first stream creates the first streaming client.
  func withStreamSlot<T: Sendable>(
    _ body: @Sendable (Client) async throws -> T
  ) async throws -> T {
    let (index, client) = try acquireSlot()
    defer { releaseSlot(at: index) }
    return try await body(client)
  }

  /// Executes `body` with the least busy unary client that may carry another RPC of `priority`,
  /// creating one if there is none, or waits for one if the pool has `maxUnaryClients`. `body`
  /// should run a single RPC at a time.
  func withUnaryClient<T: Sendable>(
    _ priority: UnaryPriority,
    _ body: (Client) async throws -> T
  ) async throws -> T {
    let (index, client) = try await acquireUnarySlot(priority)
    defer { releaseUnarySlot(at: index) }
    return try await body(client)
  }

  /// Shuts down all clients and lets ``run()`` return once they have stopped. New stream and unary
  /// slots can no longer be acquired, and RPCs waiting for one fail.
  func beginGracefulShutdown() {
    let (clients, waiters) = state.withLock { state -> ([Client], [UnaryWaiter]) in
      if state.isClosed {
        return ([], [])
      }
      state.isClosed = true
      let waiters = state.unaryWaiters.values.flatMap { $0 }
      state.unaryWaiters = [:]
      return (state.unarySlots.map(\.client) + state.slots.map(\.client), waiters)
    }
    for client in clients {
      client.beginGracefulShutdown()
    }
    newClientsContinuation.finish()
    for waiter in waiters {
      waiter.continuation.resume(throwing: Self.closedError)
    }
  }

  /// Number of active streams per streaming client, in creation order.
  var activeStreamCounts: [Int] {
    state.withLock { $0.slots.map(\.activeStreams) }
  }

  /// Number of unary RPCs in flight per unary client, in creation order.
  var activeUnaryRPCCounts: [Int] {
    state.withLock { $0.unarySlots.map(\.activeRPCs) }
  }

  /// Number of unary RPCs waiting for a client.
  var waitingUnaryRPCCount: Int {
    state.withLock { $0.unaryWaiters.values.map(\.count).reduce(0, +) }
  }

  private static var closedError: RuntimeError {
    RuntimeError(
      code: .clientIsStopped,
      message: "The client pool has been shut down. Can't start any more RPCs."
    )
  }

  private func acquireSlot() throws -> (index: Int, client: Client) {
    try state.withLock { state in
      if state.isClosed {
        throw RuntimeError(
          code: .clientIsStopped,
          message: "The client pool has been shut down. Can't open any more streams."
        )
      }

      // Lowest index first packs streams onto as few connections as possible.
      if let index = state.slots.firstIndex(where: { $0.activeStreams < maxStreamsPerClient }) {
        state.slots[index].activeStreams += 1
        return (index, state.slots[index].client)
      }

      // Created and yielded under the lock so a concurrent shutdown can't miss the new client.
      let client = try makeClient()
      state.slots.append((client, 1))
      newClientsContinuation.yield(client)
      return (state.slots.count - 1, client)
    }
  }

  private func releaseSlot(at index: Int) {
    state.withLock { $0.slots[index].activeStreams -= 1 }
  }

  private func acquireUnarySlot(_ priority: UnaryPriority) async throws -> (
    index: Int, client: Client
  ) {
    if let slot = try state.withLock({ try takeUnarySlot(priority, in: &$0) }) {
      return slot
    }

    let id = state.withLock { state in
      defer { state.nextWaiterID += 1 }
      return state.nextWaiterID
    }
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<(index: Int, client: Client), any Error>? = state.withLock { state in
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          do {
            if let slot = try takeUnarySlot(priority, in: &state) {
              return .success(slot)
            }
          } catch {
            return .failure(error)
          }
          state.unaryWaiters[priority, default: []]
            .append(UnaryWaiter(id: id, continuation: continuation))
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      let waiter = state.withLock { state -> UnaryWaiter? in
        guard let index = state.unaryWaiters[priority]?.firstIndex(where: { $0.id == id }) else {
          return nil
        }
        return state.unaryWaiters[priority]?.remove(at: index)
      }
      waiter?.continuation.resume(throwing: CancellationError())
    }
  }

  /// - Returns: A slot of `priority`, or `nil` if the RPC has to wait, also when others of the
  ///   same priority already wait.
  private func takeUnarySlot(
    _ priority: UnaryPriority, in state: inout State
  ) throws -> (index: Int, client: Client)? {
    if state.isClosed {
      throw Self.closedError
    }
    let precedingPriorities: [UnaryPriority] = priority == .high ? [.high] : [.high, .normal]
    if precedingPriorities.contains(where: { state.unaryWaiters[$0]?.isEmpty == false }) {
      return nil
    }
    if let index = leastBusyUnarySlot(for: priority, in: state) {
      state.unarySlots[index].activeRPCs += 1
      return (index, state.unarySlots[index].client)
    }
    guard state.unarySlots.count < maxUnaryClients else {
      return nil
    }
    // Created and yielded under the lock so a concurrent shutdown can't miss the new client.
    let client = try makeClient()
    state.unarySlots.append((client, 1))
    newClientsContinuation.yield(client)
    return (state.unarySlots.count - 1, client)
  }

  private func leastBusyUnarySlot(for priority: UnaryPriority, in state: State) -> Int? {
    let limit =
      switch priority {
      case .high: maxUnaryRPCsPerClient
      case .normal: maxNormalUnaryRPCsPerClient
      }
    return state.unarySlots.indices
      .filter { state.unarySlots[$0].activeRPCs < limit }
      .min { state.unarySlots[$0].activeRPCs < state.unarySlots[$1].activeRPCs }
  }

  /// Hands the freed slot to the first waiting RPC that may use it, high priority first.
  private func releaseUnarySlot(at index: Int) {
    let handOff = state.withLock {
      state -> (waiter: UnaryWaiter, slot: (index: Int, client: Client))? in
      state.unarySlots[index].activeRPCs -= 1
      for priority in [UnaryPriority.high, .normal] {
        guard state.unaryWaiters[priority]?.isEmpty == false,
          let slot = leastBusyUnarySlot(for: priority, in: state),
          let waiter = state.unaryWaiters[priority]?.removeFirst()
        else {
          continue
        }
        state.unarySlots[slot].activeRPCs += 1
        return (waiter, (slot, state.unarySlots[slot].client))
      }
      return nil
    }
    if let handOff {
      handOff.waiter.continuation.resume(returning: handOff.slot)
    }
  }
}
