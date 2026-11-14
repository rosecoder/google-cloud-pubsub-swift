import GRPCCore
import Logging
import Synchronization

enum AckBatcherError: Error {

  /// The request was submitted after ``AckBatcher/run()`` returned.
  case shutDown
}

/// Coalesces acknowledgements and ack deadline modifications of one subscription into batched RPCs.
///
/// A batch is sent when ``Configuration/maxDelay`` has passed since its first request was queued,
/// or right away when it reaches ``Configuration/maxBatchSize`` IDs or would exceed
/// ``Configuration/maxRequestBytes``. Each batch sends one RPC for acks and one per distinct
/// deadline modification. Transient errors are retried with exponential backoff, and every caller
/// receives the outcome of the RPC that carried its ID.
///
/// Requests are only sent while ``run()`` is running. ``finish()`` makes ``run()`` send everything
/// still queued, including requests submitted while it drains, and return once all RPCs are done.
///
/// Cancelling a waiting caller only stops it from waiting: its request is still sent, since a
/// dropped ack would cause a redelivery.
final class AckBatcher<C: Clock>: Sendable where C.Duration == Duration {

  struct Configuration: Sendable {

    var maxDelay: Duration = .milliseconds(100)
    var maxBatchSize: Int = 2_500
    var maxRequestBytes: Int = 510 * 1024
    var retryBackoff = ExponentialBackoff(initial: .milliseconds(100), maximum: .seconds(5))

    /// Time after the first attempt of an RPC after which it is no longer retried.
    var maxRetryDuration: Duration = .seconds(60)
  }

  let configuration: Configuration
  private let transport: any AckTransport
  private let clock: C
  private let logger: Logger

  // A Mutex rather than an actor: submitting is a short critical section on the caller's own
  // executor, so each request costs one lock and one suspension for its result instead of an
  // extra hop to and from an actor executor.
  private let state = Mutex(State())

  init(
    transport: any AckTransport,
    configuration: Configuration = Configuration(),
    clock: C,
    logger: Logger
  ) {
    precondition(configuration.maxBatchSize > 0, "maxBatchSize must be greater than zero")
    precondition(configuration.maxRequestBytes > 0, "maxRequestBytes must be greater than zero")
    self.transport = transport
    self.configuration = configuration
    self.clock = clock
    self.logger = logger
  }

  func ack(_ id: AckID) async throws {
    try await submit(id, .acknowledge)
  }

  func nack(_ id: AckID) async throws {
    try await submit(id, .modifyAckDeadline(.nack))
  }

  func modifyDeadline(_ id: AckID, to deadline: AckDeadline) async throws {
    try await submit(id, .modifyAckDeadline(.extend(deadline)))
  }

  /// Queues nacks without waiting for their outcome. Failures are only logged, and IDs submitted
  /// after ``run()`` returned are dropped.
  func enqueueNacks(_ ids: some Sequence<AckID>) {
    for id in ids {
      do {
        _ = try enqueue(id, .modifyAckDeadline(.nack), awaitsResult: false)
      } catch {
        logger.debug("Dropped nack of \(id): \(error)")
      }
    }
  }

  /// Number of submitted IDs that have not yet been handed to an RPC.
  var queuedCount: Int {
    state.withLock { state in
      state.open.count + state.sealed.reduce(0) { $0 + $1.count }
    }
  }

  /// Number of callers that have not yet received their result.
  var waiterCount: Int {
    state.withLock { $0.waiters.count }
  }

  // MARK: - Submitting

  private func submit(_ id: AckID, _ operation: Operation) async throws {
    let token = try enqueue(id, operation, awaitsResult: true)
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = state.withLock { state in
          switch state.waiters[token] {
          case .queued:
            state.waiters[token] = .waiting(continuation)
            return nil
          case .completed(let result):
            state.waiters[token] = nil
            return result
          case .cancelled, .waiting, nil:
            state.waiters[token] = nil
            return .failure(CancellationError())
          }
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      let continuation = state.withLock { state -> CheckedContinuation<Void, any Error>? in
        switch state.waiters[token] {
        case .waiting(let continuation):
          state.waiters[token] = nil
          return continuation
        case .queued:
          state.waiters[token] = .cancelled
          return nil
        case .cancelled, .completed, nil:
          return nil
        }
      }
      continuation?.resume(throwing: CancellationError())
    }
  }

  private func enqueue(
    _ id: AckID, _ operation: Operation, awaitsResult: Bool
  ) throws(AckBatcherError) -> Token {
    let size = id.encodedSize
    let (token, wakeup) = try state.withLock {
      state throws(AckBatcherError) -> (Token, CheckedContinuation<Void, Never>?) in
      guard state.lifecycle != .finished else {
        throw .shutDown
      }

      var shouldWake = false
      if !state.open.isEmpty && state.open.bytes + size > configuration.maxRequestBytes {
        state.sealOpen()
        shouldWake = true
      }
      if state.open.isEmpty {
        state.openedAt = clock.now
        shouldWake = true
      }

      let token = state.nextToken
      state.nextToken &+= 1
      if awaitsResult {
        state.waiters[token] = .queued
      }
      state.open.append(Entry(id: id, token: token), for: operation, size: size)

      if state.open.count >= configuration.maxBatchSize {
        state.sealOpen()
        shouldWake = true
      }
      return (token, shouldWake ? state.wake() : nil)
    }
    wakeup?.resume()
    return token
  }

  // MARK: - Running

  /// Sends queued requests until ``finish()`` is called or the task is cancelled, then sends
  /// everything still queued and returns when all RPCs have completed.
  ///
  /// Cancellation is treated like ``finish()`` and is not forwarded to the RPCs, so cancelling
  /// the owner does not drop acks that are already queued.
  func run() async {
    let alreadyStarted = state.withLock { state in
      defer { state.hasStarted = true }
      return state.hasStarted
    }
    precondition(!alreadyStarted, "run() must only be called once")

    // Awaited right away, so its lifetime is still bound to run(); it only exists to keep
    // cancellation from reaching the RPCs.
    let loop = Task {
      await withDiscardingTaskGroup { group in
        while let batches = await nextBatches() {
          for batch in batches {
            for (operation, entries) in batch.requests {
              group.addTask {
                await self.send(entries, operation)
              }
            }
          }
        }
      }
    }
    await withTaskCancellationHandler {
      await loop.value
    } onCancel: {
      finish()
    }
  }

  /// Makes ``run()`` send all queued requests and return.
  func finish() {
    state.withLock { state in
      if state.lifecycle == .running {
        state.lifecycle = .finishing
      }
      return state.wake()
    }?.resume()
  }

  private func nextBatches() async -> [Batch]? {
    while true {
      let action = state.withLock { state -> Action in
        // Everything a pending wakeup signals is observed below.
        state.hasPendingWakeup = false
        if let openedAt = state.openedAt,
          state.lifecycle != .running || openedAt.advanced(by: configuration.maxDelay) <= clock.now
        {
          state.sealOpen()
        }
        if !state.sealed.isEmpty {
          defer { state.sealed = [] }
          state.inFlightCount += state.sealed.reduce(0) { $0 + $1.requests.count }
          return .flush(state.sealed)
        }
        if state.lifecycle != .running {
          guard state.inFlightCount == 0 else {
            return .wait
          }
          state.lifecycle = .finished
          return .stop
        }
        if let openedAt = state.openedAt {
          return .sleep(until: openedAt.advanced(by: configuration.maxDelay))
        }
        return .wait
      }

      switch action {
      case .flush(let batches):
        return batches
      case .stop:
        return nil
      case .wait:
        await waitForWakeup()
      case .sleep(let deadline):
        await sleep(until: deadline)
      }
    }
  }

  private func sleep(until deadline: C.Instant) async {
    await withDiscardingTaskGroup { group in
      group.addTask {
        guard (try? await self.clock.sleep(until: deadline, tolerance: nil)) != nil else {
          return
        }
        self.state.withLock { $0.wake() }?.resume()
      }
      await waitForWakeup()
      group.cancelAll()
    }
  }

  // Not cancellation aware: cancelling run() calls finish(), which wakes the loop.
  private func waitForWakeup() async {
    await withCheckedContinuation { continuation in
      let resumeNow = state.withLock { state in
        if state.hasPendingWakeup {
          state.hasPendingWakeup = false
          return true
        }
        state.wakeup = continuation
        return false
      }
      if resumeNow {
        continuation.resume()
      }
    }
  }

  // MARK: - Sending

  private func send(_ entries: [Entry], _ operation: Operation) async {
    let ids = entries.map(\.id)
    let result: Result<Void, any Error>
    do {
      try await withRetries(operation, count: ids.count) {
        switch operation {
        case .acknowledge:
          try await transport.acknowledge(ids)
        case .modifyAckDeadline(let modification):
          try await transport.modifyAckDeadline(ids, to: modification)
        }
      }
      result = .success(())
    } catch {
      logger.debug("Failed to send \(operation) of \(ids.count) messages: \(error)")
      result = .failure(error)
    }
    complete(entries, with: result)
  }

  private func withRetries(
    _ operation: Operation, count: Int, _ body: () async throws -> Void
  ) async throws {
    var backoff = configuration.retryBackoff
    let giveUpAt = clock.now.advanced(by: configuration.maxRetryDuration)
    while true {
      do {
        return try await body()
      } catch {
        guard Self.isTransient(error) else {
          throw error
        }
        let delay = backoff.next()
        guard clock.now.advanced(by: delay) <= giveUpAt else {
          throw error
        }
        logger.debug("Retrying \(operation) of \(count) messages in \(delay): \(error)")
        guard (try? await clock.sleep(for: delay)) != nil else {
          throw error
        }
      }
    }
  }

  static func isTransient(_ error: any Error) -> Bool {
    switch (error as? RPCError)?.code {
    case .unavailable, .deadlineExceeded, .resourceExhausted, .aborted, .internalError:
      true
    default:
      false
    }
  }

  private func complete(_ entries: [Entry], with result: Result<Void, any Error>) {
    let (continuations, wakeup) = state.withLock { state in
      state.inFlightCount -= 1
      let wakeup = state.lifecycle == .finishing && state.inFlightCount == 0 ? state.wake() : nil

      var continuations: [CheckedContinuation<Void, any Error>] = []
      continuations.reserveCapacity(entries.count)
      for entry in entries {
        switch state.waiters[entry.token] {
        case .waiting(let continuation):
          state.waiters[entry.token] = nil
          continuations.append(continuation)
        case .queued:
          state.waiters[entry.token] = .completed(result)
        case .cancelled:
          state.waiters[entry.token] = nil
        case .completed, nil:
          break
        }
      }
      return (continuations, wakeup)
    }
    for continuation in continuations {
      continuation.resume(with: result)
    }
    wakeup?.resume()
  }
}

extension AckBatcher where C == ContinuousClock {

  convenience init(
    transport: any AckTransport,
    configuration: Configuration = Configuration(),
    logger: Logger
  ) {
    self.init(
      transport: transport, configuration: configuration, clock: .continuous, logger: logger)
  }
}

// MARK: - State

extension AckBatcher {

  fileprivate typealias Token = UInt64

  fileprivate enum Operation: Hashable, Sendable, CustomStringConvertible {
    case acknowledge
    case modifyAckDeadline(AckDeadlineModification)

    var description: String {
      switch self {
      case .acknowledge: "ack"
      case .modifyAckDeadline(let modification): modification.description
      }
    }
  }

  fileprivate struct Entry: Sendable {
    let id: AckID
    let token: Token
  }

  fileprivate struct Batch: Sendable {
    private(set) var requests: [Operation: [Entry]] = [:]
    private(set) var count = 0
    private(set) var bytes = 0

    var isEmpty: Bool {
      count == 0
    }

    mutating func append(_ entry: Entry, for operation: Operation, size: Int) {
      requests[operation, default: []].append(entry)
      count += 1
      bytes += size
    }
  }

  fileprivate enum Waiter {
    case queued
    case waiting(CheckedContinuation<Void, any Error>)
    case cancelled
    case completed(Result<Void, any Error>)
  }

  fileprivate enum Lifecycle {
    case running
    case finishing
    case finished
  }

  fileprivate enum Action {
    case flush([Batch])
    case sleep(until: C.Instant)
    case wait
    case stop
  }

  fileprivate struct State {
    var open = Batch()
    var openedAt: C.Instant?
    var sealed: [Batch] = []
    var waiters: [Token: Waiter] = [:]
    var inFlightCount = 0
    var nextToken: Token = 0
    var wakeup: CheckedContinuation<Void, Never>?
    var hasPendingWakeup = false
    var lifecycle = Lifecycle.running
    var hasStarted = false

    mutating func sealOpen() {
      guard !open.isEmpty else {
        return
      }
      sealed.append(open)
      open = Batch()
      openedAt = nil
    }

    mutating func wake() -> CheckedContinuation<Void, Never>? {
      if let wakeup {
        self.wakeup = nil
        return wakeup
      }
      hasPendingWakeup = true
      return nil
    }
  }
}
