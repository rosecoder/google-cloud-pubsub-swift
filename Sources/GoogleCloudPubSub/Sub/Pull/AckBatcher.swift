import GRPCCore
import Logging
import Synchronization

enum AckBatcherError: Error {

  /// The request was submitted after ``AckBatcher/run()`` returned.
  case shutDown

  /// The request had not been sent by the deadline passed to ``AckBatcher/finish(flushBy:)``.
  case flushDeadlineExceeded
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
/// ``finish(flushBy:)`` also bounds that: retries stop at the deadline, and at the deadline RPCs
/// still running are cancelled and requests not yet sent fail.
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

  /// Extends the deadline of all `ids` and waits until every RPC carrying them has completed or
  /// the task is cancelled. Failures are only logged.
  ///
  /// Waiting costs one continuation for the whole set rather than one per ID.
  func modifyDeadlines(_ ids: some Collection<AckID>, to deadline: AckDeadline) async {
    guard !ids.isEmpty else {
      return
    }
    let group: Token
    do {
      group = try enqueueGroup(ids, .modifyAckDeadline(.extend(deadline)))
    } catch {
      logger.debug("Dropped deadline extension of \(ids.count) messages: \(error)")
      return
    }

    await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        let resumeNow = state.withLock { state in
          guard var waiter = state.groups[group], waiter.remaining > 0, !waiter.isCancelled else {
            state.groups[group] = nil
            return true
          }
          waiter.continuation = continuation
          state.groups[group] = waiter
          return false
        }
        if resumeNow {
          continuation.resume()
        }
      }
    } onCancel: {
      state.withLock { state -> CheckedContinuation<Void, Never>? in
        guard var waiter = state.groups[group] else {
          return nil
        }
        if let continuation = waiter.continuation {
          state.groups[group] = nil
          return continuation
        }
        waiter.isCancelled = true
        state.groups[group] = waiter
        return nil
      }?.resume()
    }
  }

  /// Queues nacks without waiting for their outcome. Failures are only logged, and IDs submitted
  /// after ``run()`` returned are dropped.
  func enqueueNacks(_ ids: some Sequence<AckID>) {
    enqueueWithoutWaiting(ids, .modifyAckDeadline(.nack))
  }

  /// Queues deadline extensions without waiting for their outcome. Failures are only logged, and
  /// IDs submitted after ``run()`` returned are dropped.
  func enqueueModifyDeadlines(_ ids: some Sequence<AckID>, to deadline: AckDeadline) {
    enqueueWithoutWaiting(ids, .modifyAckDeadline(.extend(deadline)))
  }

  /// Number of submitted IDs that have not yet been handed to an RPC.
  var queuedCount: Int {
    state.withLock { state in
      state.open.count + state.sealed.reduce(0) { $0 + $1.count }
    }
  }

  /// Number of callers that have not yet received their result.
  var waiterCount: Int {
    state.withLock { state in
      state.waiters.count { $0.value.isCaller } + state.groups.count
    }
  }

  // MARK: - Submitting

  private func submit(_ id: AckID, _ operation: Operation) async throws {
    let token = try enqueue(id, operation)
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
          case .cancelled, .waiting, .member, nil:
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
        case .cancelled, .completed, .member, nil:
          return nil
        }
      }
      continuation?.resume(throwing: CancellationError())
    }
  }

  private func enqueue(_ id: AckID, _ operation: Operation) throws(AckBatcherError) -> Token {
    let (token, wakeup) = try state.withLock {
      state throws(AckBatcherError) -> (Token, CheckedContinuation<Void, Never>?) in
      guard state.lifecycle != .finished else {
        throw .shutDown
      }
      let (token, shouldWake) = append(id, operation, waiter: .queued, to: &state)
      return (token, shouldWake ? state.wake() : nil)
    }
    wakeup?.resume()
    return token
  }

  private func enqueueGroup(
    _ ids: some Collection<AckID>, _ operation: Operation
  ) throws(AckBatcherError) -> Token {
    let (group, wakeup) = try state.withLock {
      state throws(AckBatcherError) -> (Token, CheckedContinuation<Void, Never>?) in
      guard state.lifecycle != .finished else {
        throw .shutDown
      }
      let group = state.nextToken
      state.nextToken &+= 1
      state.groups[group] = GroupWaiter(remaining: ids.count)
      var shouldWake = false
      for id in ids {
        shouldWake = append(id, operation, waiter: .member(group), to: &state).shouldWake
          || shouldWake
      }
      return (group, shouldWake ? state.wake() : nil)
    }
    wakeup?.resume()
    return group
  }

  private func enqueueWithoutWaiting(_ ids: some Sequence<AckID>, _ operation: Operation) {
    let wakeup = state.withLock { state -> CheckedContinuation<Void, Never>? in
      guard state.lifecycle != .finished else {
        logger.debug("Dropped \(operation) after shutdown")
        return nil
      }
      var shouldWake = false
      for id in ids {
        shouldWake = append(id, operation, waiter: nil, to: &state).shouldWake || shouldWake
      }
      return shouldWake ? state.wake() : nil
    }
    wakeup?.resume()
  }

  private func append(
    _ id: AckID, _ operation: Operation, waiter: Waiter?, to state: inout State
  ) -> (token: Token, shouldWake: Bool) {
    let size = id.encodedSize
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
    if let waiter {
      state.waiters[token] = waiter
    }
    state.open.append(Entry(id: id, token: token), for: operation, size: size)

    if state.open.count >= configuration.maxBatchSize {
      state.sealOpen()
      shouldWake = true
    }
    return (token, shouldWake)
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
        while true {
          switch await nextBatches() {
          case .flush(let batches):
            for batch in batches {
              let (requests, superseded) = batch.deduplicated()
              complete(superseded, with: .success(()), finishesRequest: false)
              state.withLock { $0.inFlightCount += requests.count }
              for (operation, entries) in requests {
                group.addTask {
                  await self.send(entries, operation)
                }
              }
            }
          case .stop:
            return
          case .abandon(let unsent):
            logger.debug("Dropped \(unsent.count) requests not sent by the flush deadline")
            complete(
              unsent, with: .failure(AckBatcherError.flushDeadlineExceeded),
              finishesRequest: false)
            group.cancelAll()
            return
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
    finish(flushBy: nil)
  }

  /// Makes ``run()`` send all queued requests and return, at the latest at `deadline`.
  ///
  /// Requests are not retried past the deadline. At the deadline, RPCs still running are
  /// cancelled and requests not yet sent fail with ``AckBatcherError/flushDeadlineExceeded``.
  /// Calling it again only ever moves the deadline earlier.
  func finish(flushBy deadline: C.Instant?) {
    state.withLock { state in
      if state.lifecycle == .running {
        state.lifecycle = .finishing
      }
      if let deadline {
        state.flushDeadline = min(state.flushDeadline ?? deadline, deadline)
      }
      return state.wake()
    }?.resume()
  }

  private func nextBatches() async -> Next {
    while true {
      let action = state.withLock { state -> Action in
        // Everything a pending wakeup signals is observed below.
        state.hasPendingWakeup = false
        if state.lifecycle != .running, let deadline = state.flushDeadline, deadline <= clock.now {
          state.sealOpen()
          defer { state.sealed = [] }
          state.lifecycle = .finished
          return .next(.abandon(state.sealed.flatMap(\.entries)))
        }
        if let openedAt = state.openedAt,
          state.lifecycle != .running || openedAt.advanced(by: configuration.maxDelay) <= clock.now
        {
          state.sealOpen()
        }
        if !state.sealed.isEmpty {
          defer { state.sealed = [] }
          return .next(.flush(state.sealed))
        }
        if state.lifecycle != .running {
          guard state.inFlightCount == 0 else {
            return state.flushDeadline.map { .sleep(until: $0) } ?? .wait
          }
          state.lifecycle = .finished
          return .next(.stop)
        }
        if let openedAt = state.openedAt {
          return .sleep(until: openedAt.advanced(by: configuration.maxDelay))
        }
        return .wait
      }

      switch action {
      case .next(let next):
        return next
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
      // Sends are only cancelled at the flush deadline.
      result = .failure(Task.isCancelled ? AckBatcherError.flushDeadlineExceeded : error)
    }
    complete(entries, with: result, finishesRequest: true)
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
        let retryAt = clock.now.advanced(by: delay)
        let flushDeadline = state.withLock { $0.flushDeadline }
        guard retryAt <= giveUpAt, flushDeadline.map({ retryAt < $0 }) ?? true else {
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

  /// - Parameter finishesRequest: Whether `entries` were sent in an RPC counted as in flight,
  ///   rather than superseded by other requests for the same IDs.
  private func complete(
    _ entries: [Entry], with result: Result<Void, any Error>, finishesRequest: Bool
  ) {
    guard !entries.isEmpty || finishesRequest else {
      return
    }
    let (continuations, groupContinuations, wakeup) = state.withLock { state in
      var wakeup: CheckedContinuation<Void, Never>?
      if finishesRequest {
        state.inFlightCount -= 1
        if state.lifecycle == .finishing && state.inFlightCount == 0 {
          wakeup = state.wake()
        }
      }

      var continuations: [CheckedContinuation<Void, any Error>] = []
      var groupContinuations: [CheckedContinuation<Void, Never>] = []
      for entry in entries {
        switch state.waiters[entry.token] {
        case .waiting(let continuation):
          state.waiters[entry.token] = nil
          continuations.append(continuation)
        case .queued:
          state.waiters[entry.token] = .completed(result)
        case .cancelled:
          state.waiters[entry.token] = nil
        case .member(let group):
          state.waiters[entry.token] = nil
          if let continuation = state.completeMember(of: group) {
            groupContinuations.append(continuation)
          }
        case .completed, nil:
          break
        }
      }
      return (continuations, groupContinuations, wakeup)
    }
    for continuation in continuations {
      continuation.resume(with: result)
    }
    for continuation in groupContinuations {
      continuation.resume()
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

    /// Whether this request makes `other` for the same ID pointless: an ack wins over everything,
    /// a nack wins over extensions and a longer extension wins over a shorter one.
    func supersedes(_ other: Operation) -> Bool {
      precedence > other.precedence
    }

    private var precedence: (Int, Int32) {
      switch self {
      case .acknowledge: (2, 0)
      case .modifyAckDeadline(.nack): (1, 0)
      case .modifyAckDeadline(.extend(let deadline)): (0, deadline.seconds)
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

    var entries: [Entry] {
      requests.values.flatMap { $0 }
    }

    mutating func append(_ entry: Entry, for operation: Operation, size: Int) {
      requests[operation, default: []].append(entry)
      count += 1
      bytes += size
    }

    /// Requests to send, with each ID only once: in the superseding request, and there only its
    /// first entry.
    func deduplicated() -> (requests: [(Operation, [Entry])], superseded: [Entry]) {
      var winners: [AckID: (operation: Operation, token: Token)] = [:]
      winners.reserveCapacity(count)
      for (operation, entries) in requests {
        for entry in entries {
          if let current = winners[entry.id], !operation.supersedes(current.operation) {
            continue
          }
          winners[entry.id] = (operation, entry.token)
        }
      }

      var deduplicated: [(Operation, [Entry])] = []
      var superseded: [Entry] = []
      for (operation, entries) in requests {
        var kept: [Entry] = []
        kept.reserveCapacity(entries.count)
        for entry in entries {
          if let winner = winners[entry.id], winner.operation == operation,
            winner.token == entry.token
          {
            kept.append(entry)
          } else {
            superseded.append(entry)
          }
        }
        if !kept.isEmpty {
          deduplicated.append((operation, kept))
        }
      }
      return (deduplicated, superseded)
    }
  }

  fileprivate enum Waiter {
    case queued
    case waiting(CheckedContinuation<Void, any Error>)
    case cancelled
    case completed(Result<Void, any Error>)
    case member(Token)

    var isCaller: Bool {
      if case .member = self {
        return false
      }
      return true
    }
  }

  /// One waiter for a set of IDs submitted together. `continuation` is set once the caller
  /// waits, which may happen after all of them have completed.
  fileprivate struct GroupWaiter {
    var remaining: Int
    var continuation: CheckedContinuation<Void, Never>?
    var isCancelled = false
  }

  fileprivate enum Lifecycle {
    case running
    case finishing
    case finished
  }

  fileprivate enum Next {
    case flush([Batch])
    case stop

    /// The flush deadline has passed with these entries still unsent.
    case abandon([Entry])
  }

  fileprivate enum Action {
    case next(Next)
    case sleep(until: C.Instant)
    case wait
  }

  fileprivate struct State {
    var open = Batch()
    var openedAt: C.Instant?
    var sealed: [Batch] = []
    var waiters: [Token: Waiter] = [:]
    var groups: [Token: GroupWaiter] = [:]
    var inFlightCount = 0
    var nextToken: Token = 0
    var wakeup: CheckedContinuation<Void, Never>?
    var hasPendingWakeup = false
    var lifecycle = Lifecycle.running
    var flushDeadline: C.Instant?
    var hasStarted = false

    mutating func sealOpen() {
      guard !open.isEmpty else {
        return
      }
      sealed.append(open)
      open = Batch()
      openedAt = nil
    }

    /// Returns the group's continuation once its last member has completed.
    mutating func completeMember(of group: Token) -> CheckedContinuation<Void, Never>? {
      guard var waiter = groups[group] else {
        return nil
      }
      waiter.remaining -= 1
      if waiter.remaining == 0 && (waiter.continuation != nil || waiter.isCancelled) {
        groups[group] = nil
        return waiter.continuation
      }
      groups[group] = waiter
      return nil
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
