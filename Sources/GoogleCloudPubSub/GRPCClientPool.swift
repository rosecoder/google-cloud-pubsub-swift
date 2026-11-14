import GRPCCore
import Synchronization

protocol PooledClient: Sendable {
  func runConnections() async throws
  func beginGracefulShutdown()
}

extension GRPCClient: PooledClient {}

/// Keeps unary RPCs and long-lived streams on separate clients (and therefore HTTP/2 connections),
/// and spreads the streams over multiple clients.
///
/// Servers cap concurrent streams per connection (Google front ends commonly at 100) and opening a
/// stream beyond that limit queues silently instead of failing. Neither the transport nor the load
/// balancers open extra connections based on stream count, so the pool does it by creating
/// additional clients when every streaming client holds `maxStreamsPerClient` streams. Unary RPCs,
/// such as acknowledgements, would otherwise queue behind the streams sharing their connection.
final class GRPCClientPool<Client: PooledClient>: Sendable {

  /// Client used for unary RPCs. Never carries streams of ``withStreamSlot(_:)``.
  let unary: Client

  private let maxStreamsPerClient: Int
  private let makeClient: @Sendable () throws -> Client
  private let state: Mutex<State>
  private let newClients: AsyncStream<Client>
  private let newClientsContinuation: AsyncStream<Client>.Continuation

  private struct State {
    var slots: [(client: Client, activeStreams: Int)] = []
    var isRunning = false
    var isClosed = false
  }

  /// - Parameter maxStreamsPerClient: Should stay below the server's concurrent stream limit with
  ///   headroom for streams that are still closing while their replacements open.
  /// - Parameter makeClient: Called while holding the pool's lock; must not call back into the pool.
  init(
    maxStreamsPerClient: Int = 80,
    makeClient: @escaping @Sendable () throws -> Client
  ) throws {
    precondition(maxStreamsPerClient > 0, "maxStreamsPerClient must be positive")

    let unary = try makeClient()
    self.unary = unary
    self.maxStreamsPerClient = maxStreamsPerClient
    self.makeClient = makeClient
    self.state = Mutex(State())
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

  /// Shuts down all clients and lets ``run()`` return once they have stopped. New stream slots can
  /// no longer be acquired.
  func beginGracefulShutdown() {
    let clients = state.withLock { state -> [Client] in
      if state.isClosed {
        return []
      }
      state.isClosed = true
      return [unary] + state.slots.map(\.client)
    }
    for client in clients {
      client.beginGracefulShutdown()
    }
    newClientsContinuation.finish()
  }

  /// Number of active streams per streaming client, in creation order.
  var activeStreamCounts: [Int] {
    state.withLock { $0.slots.map(\.activeStreams) }
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
}
