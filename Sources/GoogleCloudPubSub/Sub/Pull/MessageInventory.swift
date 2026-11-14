import DequeModule
import Synchronization

/// Messages received during one run of a pull subscriber, limited by
/// ``PullSubscriberOptions/FlowControl``.
///
/// A message is *dispatched* from when it is handed to a handler until it is released after its
/// ack or nack, and only dispatched messages count towards the limits. Received messages that do
/// not fit are *held* and dispatched in the order they were received as capacity frees up. Flow
/// control never nacks a message.
///
/// Receiving pauses once the load (the larger fraction of either limit in use) reaches 1 and
/// resumes when it drops below 0.8, so receiving does not resume for one free slot at a time.
///
/// All messages, held and dispatched, are leased: ``renewLeases(now:)`` returns the IDs whose ack
/// deadline should be extended. A dispatched message stops being leased and frees its capacity
/// once it has been dispatched for longer than ``PullSubscriberOptions/Lease/maxLeaseDuration``.
final class MessageInventory: Sendable {

  struct Item: Sendable {
    let ackID: AckID
    let message: Google_Pubsub_V1_ReceivedMessage
    let size: Int
    let receivedAt: ContinuousClock.Instant

    init(_ message: Google_Pubsub_V1_ReceivedMessage, receivedAt: ContinuousClock.Instant) {
      self.ackID = AckID(message.ackID)
      self.message = message
      self.size = message.message.data.count
      self.receivedAt = receivedAt
    }
  }

  struct Received: Sendable, Equatable {

    /// IDs that are now leased, whose deadline should be extended to ``ackDeadline`` right away.
    let accepted: [AckID]

    /// IDs to nack, which are all of them if ``stopReceiving()`` has already been called.
    let rejected: [AckID]

    let ackDeadline: AckDeadline
  }

  struct LeaseRenewal: Sendable, Equatable {
    let ackDeadline: AckDeadline

    /// IDs of held and dispatched messages whose deadline should be extended.
    let ids: [AckID]

    /// IDs of messages dispatched for longer than the max lease duration, which are no longer
    /// leased or counted towards flow control limits.
    let expired: [AckID]
  }

  private let storage: Mutex<Storage>

  init(
    flowControl: PullSubscriberOptions.FlowControl,
    lease: PullSubscriberOptions.Lease = .init(),
    initialAckDeadline: AckDeadline = .minimum
  ) {
    self.storage = Mutex(
      Storage(
        state: State(
          flowControl: flowControl, lease: lease, initialAckDeadline: initialAckDeadline)))
  }

  var isEmpty: Bool {
    storage.withLock { $0.state.isEmpty }
  }

  var ackDeadline: AckDeadline {
    storage.withLock { $0.state.ackDeadline }
  }

  var isReceiving: Bool {
    storage.withLock { $0.state.isReceiving }
  }

  var exactlyOnceDelivery: Bool {
    storage.withLock { $0.state.exactlyOnceDelivery }
  }

  /// - Returns: Whether exactly-once delivery was changed, which changes ``ackDeadline``.
  func setExactlyOnceDelivery(_ isEnabled: Bool) -> Bool {
    storage.withLock { storage in
      defer { storage.state.exactlyOnceDelivery = isEnabled }
      return storage.state.exactlyOnceDelivery != isEnabled
    }
  }

  var hasLeaseWaiter: Bool {
    storage.withLock { $0.leaseWaiter != nil }
  }

  /// Dispatched messages in the order they were received. Ends once ``stopReceiving()`` has been
  /// called and every dispatched message has been returned. Ending it is not tied to the
  /// consuming task's cancellation, so no dispatched message is dropped.
  ///
  /// Must only be iterated by one task.
  var dispatched: Dispatched {
    Dispatched(inventory: self)
  }

  /// Waits until more messages may be received.
  ///
  /// Must only be called by one task at a time.
  ///
  /// - Throws: `CancellationError` if the task is cancelled or ``stopReceiving()`` is called.
  func waitUntilReceivable() async throws {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = storage.withLock { storage in
          precondition(storage.receiveWaiter == nil, "Only one task may wait to receive")
          if Task.isCancelled || !storage.state.isReceiving {
            return .failure(CancellationError())
          }
          if storage.state.isReceivable {
            return .success(())
          }
          storage.receiveWaiter = continuation
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      storage.withLock { storage in
        defer { storage.receiveWaiter = nil }
        return storage.receiveWaiter
      }?.resume(throwing: CancellationError())
    }
  }

  /// Adds received messages and dispatches as many as the limits allow.
  func add(_ messages: [Google_Pubsub_V1_ReceivedMessage], now: ContinuousClock.Instant)
    -> Received
  {
    let items = messages.map { Item($0, receivedAt: now) }
    return mutate { $0.add(items, now: now) }
  }

  /// Frees the capacity of a message once it has been acked or nacked. Unknown IDs are ignored.
  func release(_ id: AckID, now: ContinuousClock.Instant = .now) {
    mutate { $0.release(id, now: now) }
  }

  func recordAckLatency(_ latency: Duration) {
    storage.withLock { $0.state.recordAckLatency(latency) }
  }

  /// Stops leasing expired messages and returns the IDs whose deadline should be extended.
  func renewLeases(now: ContinuousClock.Instant) -> LeaseRenewal {
    mutate { $0.renewLeases(now: now) }
  }

  /// Waits until at least one message is leased.
  ///
  /// Must only be called by one task at a time.
  ///
  /// - Returns: Whether it had to wait, that is whether nothing was leased when called.
  /// - Throws: `CancellationError` if the task is cancelled.
  @discardableResult
  func waitUntilNonEmpty() async throws -> Bool {
    var waited = false
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = storage.withLock { storage in
          precondition(storage.leaseWaiter == nil, "Only one task may wait for messages to lease")
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if !storage.state.isEmpty {
            return .success(())
          }
          storage.leaseWaiter = continuation
          waited = true
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      storage.withLock { storage in
        defer { storage.leaseWaiter = nil }
        return storage.leaseWaiter
      }?.resume(throwing: CancellationError())
    }
    return waited
  }

  /// Waits until messages have been accepted with an ack deadline shorter than `deadline` since
  /// leases were last renewed.
  ///
  /// Must only be called by one task at a time.
  ///
  /// - Returns: The shortest ack deadline messages were accepted with since leases were last
  ///   renewed.
  /// - Throws: `CancellationError` if the task is cancelled.
  func waitUntilAccepted(shorterThan deadline: AckDeadline) async throws -> AckDeadline {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<AckDeadline, any Error>? = storage.withLock { storage in
          precondition(storage.acceptWaiter == nil, "Only one task may wait for accepted messages")
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if let accepted = storage.state.shortestAcceptedAckDeadline, accepted < deadline {
            return .success(accepted)
          }
          storage.acceptWaiter = (deadline, continuation)
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      storage.withLock { storage in
        defer { storage.acceptWaiter = nil }
        return storage.acceptWaiter?.continuation
      }?.resume(throwing: CancellationError())
    }
  }

  /// Stops receiving and ends ``dispatched`` once it has returned all dispatched messages.
  ///
  /// - Returns: IDs of held messages, which are removed and should be nacked.
  func stopReceiving() -> [AckID] {
    mutate { $0.stopReceiving() }
  }

  /// Marks a message as being acked or nacked by its handler, so ``closeAll()`` leaves it to it.
  ///
  /// - Returns: `false` if ``closeAll()`` already took the message, so its handler's result must
  ///   be dropped.
  func claim(_ id: AckID) -> Bool {
    storage.withLock { $0.state.claim(id) }
  }

  /// Stops receiving and removes every message not claimed by its handler.
  ///
  /// - Returns: IDs of the removed messages, which should be nacked.
  func closeAll() -> [AckID] {
    mutate { $0.closeAll() }
  }

  /// Waits until no message is leased.
  ///
  /// Must only be called by one task at a time.
  ///
  /// - Throws: `CancellationError` if the task is cancelled.
  func waitUntilEmpty() async throws {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = storage.withLock { storage in
          precondition(storage.emptyWaiter == nil, "Only one task may wait for no messages")
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if storage.state.isEmpty {
            return .success(())
          }
          storage.emptyWaiter = continuation
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      storage.withLock { storage in
        defer { storage.emptyWaiter = nil }
        return storage.emptyWaiter
      }?.resume(throwing: CancellationError())
    }
  }

  fileprivate func nextDispatched() async -> Item? {
    await withCheckedContinuation { continuation in
      let (resumeNow, item) = storage.withLock { storage -> (Bool, Item?) in
        precondition(storage.dispatchWaiter == nil, "Only one task may iterate dispatched")
        if let item = storage.state.nextReady() {
          return (true, item)
        }
        if !storage.state.isReceiving {
          return (true, nil)
        }
        storage.dispatchWaiter = continuation
        return (false, nil)
      }
      if resumeNow {
        continuation.resume(returning: item)
      }
    }
  }

  private func mutate<Result: Sendable>(_ body: (inout State) -> Result) -> Result {
    let (result, resumptions) = storage.withLock { storage in
      let result = body(&storage.state)
      return (result, storage.takeResumptions())
    }
    resumptions.resume()
    return result
  }

  struct Dispatched: AsyncSequence {

    fileprivate let inventory: MessageInventory

    func makeAsyncIterator() -> Iterator {
      Iterator(inventory: inventory)
    }

    struct Iterator: AsyncIteratorProtocol {

      fileprivate let inventory: MessageInventory

      func next() async -> Item? {
        await inventory.nextDispatched()
      }
    }
  }
}

// MARK: - State

extension MessageInventory {

  /// All flow control decisions, without any concurrency.
  struct State: Sendable {

    static let resumeLoad = 0.8

    struct Entry: Sendable {
      let size: Int
      let receivedAt: ContinuousClock.Instant
      var dispatchedAt: ContinuousClock.Instant?

      /// Whether its handler is acking or nacking it.
      var isClaimed = false

      var isDispatched: Bool {
        dispatchedAt != nil
      }
    }

    let flowControl: PullSubscriberOptions.FlowControl
    let lease: PullSubscriberOptions.Lease
    let initialAckDeadline: AckDeadline
    var exactlyOnceDelivery = false
    private(set) var histogram = AckLatencyHistogram()

    /// Shortest ack deadline messages were accepted with since leases were last renewed.
    private(set) var shortestAcceptedAckDeadline: AckDeadline?
    private(set) var entries: [AckID: Entry] = [:]

    /// IDs removed by ``closeAll()``, whose handler results must be dropped.
    private(set) var closed: Set<AckID> = []
    private(set) var held: Deque<Item> = []

    /// Dispatched messages not yet taken by ``nextReady()``.
    private(set) var ready: Deque<Item> = []
    private(set) var outstandingMessages = 0
    private(set) var outstandingBytes = 0
    private(set) var isPaused = false
    private(set) var isReceiving = true

    init(
      flowControl: PullSubscriberOptions.FlowControl,
      lease: PullSubscriberOptions.Lease = .init(),
      initialAckDeadline: AckDeadline = .minimum
    ) {
      self.flowControl = flowControl
      self.lease = lease
      self.initialAckDeadline = initialAckDeadline
    }

    var isEmpty: Bool {
      entries.isEmpty
    }

    var ackDeadline: AckDeadline {
      lease.ackDeadline(
        basedOn: histogram, initial: initialAckDeadline, exactlyOnceDelivery: exactlyOnceDelivery)
    }

    /// Whether more messages may be received, which is not until held messages are dispatched.
    var isReceivable: Bool {
      isReceiving && !isPaused && held.isEmpty
    }

    var load: Double {
      max(
        Double(outstandingMessages) / Double(flowControl.maxOutstandingMessages),
        Double(outstandingBytes) / Double(flowControl.maxOutstandingBytes))
    }

    mutating func add(_ items: [Item], now: ContinuousClock.Instant) -> Received {
      guard isReceiving else {
        return Received(accepted: [], rejected: items.map(\.ackID), ackDeadline: ackDeadline)
      }
      var accepted: [AckID] = []
      accepted.reserveCapacity(items.count)
      for item in items where entries[item.ackID] == nil {
        entries[item.ackID] = Entry(size: item.size, receivedAt: item.receivedAt)
        held.append(item)
        accepted.append(item.ackID)
      }
      dispatchHeld(now: now)
      let ackDeadline = ackDeadline
      if !accepted.isEmpty {
        shortestAcceptedAckDeadline = min(shortestAcceptedAckDeadline ?? ackDeadline, ackDeadline)
      }
      return Received(accepted: accepted, rejected: [], ackDeadline: ackDeadline)
    }

    mutating func release(_ id: AckID, now: ContinuousClock.Instant) {
      remove(id)
      dispatchHeld(now: now)
    }

    mutating func recordAckLatency(_ latency: Duration) {
      histogram.record(latency)
    }

    mutating func renewLeases(now: ContinuousClock.Instant) -> LeaseRenewal {
      var ids: [AckID] = []
      var expired: [AckID] = []
      ids.reserveCapacity(entries.count)
      for (id, entry) in entries {
        if let dispatchedAt = entry.dispatchedAt,
          dispatchedAt.duration(to: now) >= lease.maxLeaseDuration
        {
          expired.append(id)
        } else {
          ids.append(id)
        }
      }
      for id in expired {
        remove(id)
      }
      dispatchHeld(now: now)
      shortestAcceptedAckDeadline = nil
      return LeaseRenewal(ackDeadline: ackDeadline, ids: ids, expired: expired)
    }

    private mutating func remove(_ id: AckID) {
      guard let entry = entries.removeValue(forKey: id) else {
        return
      }
      if entry.isDispatched {
        outstandingMessages -= 1
        outstandingBytes -= entry.size
      } else {
        held.removeAll { $0.ackID == id }
      }
    }

    mutating func nextReady() -> Item? {
      ready.popFirst()
    }

    mutating func stopReceiving() -> [AckID] {
      isReceiving = false
      let ids = held.map(\.ackID)
      for id in ids {
        entries[id] = nil
      }
      held.removeAll()
      return ids
    }

    mutating func claim(_ id: AckID) -> Bool {
      guard !closed.contains(id) else {
        return false
      }
      entries[id]?.isClaimed = true
      return true
    }

    mutating func closeAll() -> [AckID] {
      var ids = stopReceiving()
      ready.removeAll()
      for (id, entry) in entries where !entry.isClaimed {
        ids.append(id)
      }
      for id in ids {
        remove(id)
        closed.insert(id)
      }
      return ids
    }

    // Limits are checked before dispatching, so a single message larger than the byte limit is
    // still dispatched once nothing else is outstanding.
    private mutating func dispatchHeld(now: ContinuousClock.Instant) {
      while let item = held.first,
        outstandingMessages < flowControl.maxOutstandingMessages,
        outstandingBytes < flowControl.maxOutstandingBytes
      {
        held.removeFirst()
        entries[item.ackID]?.dispatchedAt = now
        outstandingMessages += 1
        outstandingBytes += item.size
        ready.append(item)
      }

      if load >= 1 {
        isPaused = true
      } else if load < Self.resumeLoad {
        isPaused = false
      }
    }
  }
}

// MARK: - Storage

extension MessageInventory {

  private struct Storage {
    var state: State
    var receiveWaiter: CheckedContinuation<Void, any Error>?
    var dispatchWaiter: CheckedContinuation<Item?, Never>?
    var leaseWaiter: CheckedContinuation<Void, any Error>?
    var emptyWaiter: CheckedContinuation<Void, any Error>?
    var acceptWaiter:
      (deadline: AckDeadline, continuation: CheckedContinuation<AckDeadline, any Error>)?

    mutating func takeResumptions() -> Resumptions {
      var resumptions = Resumptions()
      if let waiter = leaseWaiter, !state.isEmpty {
        resumptions.lease = waiter
        leaseWaiter = nil
      }
      if let waiter = emptyWaiter, state.isEmpty {
        resumptions.empty = waiter
        emptyWaiter = nil
      }
      if let waiter = acceptWaiter, let accepted = state.shortestAcceptedAckDeadline,
        accepted < waiter.deadline
      {
        resumptions.accept = (waiter.continuation, accepted)
        acceptWaiter = nil
      }
      if let waiter = dispatchWaiter {
        if let item = state.nextReady() {
          resumptions.dispatch = (waiter, item)
          dispatchWaiter = nil
        } else if !state.isReceiving {
          resumptions.dispatch = (waiter, nil)
          dispatchWaiter = nil
        }
      }
      if let waiter = receiveWaiter {
        if !state.isReceiving {
          resumptions.receive = (waiter, .failure(CancellationError()))
          receiveWaiter = nil
        } else if state.isReceivable {
          resumptions.receive = (waiter, .success(()))
          receiveWaiter = nil
        }
      }
      return resumptions
    }
  }

  private struct Resumptions: Sendable {
    var dispatch: (CheckedContinuation<Item?, Never>, Item?)?
    var receive: (CheckedContinuation<Void, any Error>, Result<Void, any Error>)?
    var lease: CheckedContinuation<Void, any Error>?
    var empty: CheckedContinuation<Void, any Error>?
    var accept: (CheckedContinuation<AckDeadline, any Error>, AckDeadline)?

    func resume() {
      lease?.resume()
      empty?.resume()
      if let (continuation, deadline) = accept {
        continuation.resume(returning: deadline)
      }
      if let (continuation, item) = dispatch {
        continuation.resume(returning: item)
      }
      if let (continuation, result) = receive {
        continuation.resume(with: result)
      }
    }
  }
}
