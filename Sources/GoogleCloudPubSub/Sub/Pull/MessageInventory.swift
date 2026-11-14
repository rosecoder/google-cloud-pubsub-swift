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
/// resumes when it drops below 0.8, so pulls are not issued for one free slot at a time.
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

  private let storage: Mutex<Storage>

  init(flowControl: PullSubscriberOptions.FlowControl) {
    self.storage = Mutex(Storage(state: State(flowControl: flowControl)))
  }

  var isEmpty: Bool {
    storage.withLock { $0.state.isEmpty }
  }

  /// Dispatched messages in the order they were received. Ends once ``stopReceiving()`` has been
  /// called and every dispatched message has been returned. Ending it is not tied to the
  /// consuming task's cancellation, so no dispatched message is dropped.
  ///
  /// Must only be iterated by one task.
  var dispatched: Dispatched {
    Dispatched(inventory: self)
  }

  /// Waits until messages may be received and returns how many to request at most.
  ///
  /// Must only be called by one task at a time.
  ///
  /// - Throws: `CancellationError` if the task is cancelled or ``stopReceiving()`` is called.
  func waitUntilReceivable() async throws -> Int {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Int, any Error>? = storage.withLock { storage in
          precondition(storage.receiveWaiter == nil, "Only one task may wait to receive")
          if Task.isCancelled || !storage.state.isReceiving {
            return .failure(CancellationError())
          }
          if let count = storage.state.receivableCount {
            return .success(count)
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
  ///
  /// - Returns: IDs of messages to nack, which are all of them if ``stopReceiving()`` has already
  ///   been called and none otherwise.
  func add(_ messages: [Google_Pubsub_V1_ReceivedMessage], now: ContinuousClock.Instant)
    -> [AckID]
  {
    let items = messages.map { Item($0, receivedAt: now) }
    return mutate { $0.add(items) }
  }

  /// Frees the capacity of a message once it has been acked or nacked. Unknown IDs are ignored.
  func release(_ id: AckID) {
    mutate { $0.release(id) }
  }

  /// Stops receiving and ends ``dispatched`` once it has returned all dispatched messages.
  ///
  /// - Returns: IDs of held messages, which are removed and should be nacked.
  func stopReceiving() -> [AckID] {
    mutate { $0.stopReceiving() }
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

    static let maxMessagesPerPull = 1_000
    static let resumeLoad = 0.8

    struct Entry: Sendable {
      let size: Int
      let receivedAt: ContinuousClock.Instant
      var isDispatched: Bool
    }

    let flowControl: PullSubscriberOptions.FlowControl
    private(set) var entries: [AckID: Entry] = [:]
    private(set) var held: Deque<Item> = []

    /// Dispatched messages not yet taken by ``nextReady()``.
    private(set) var ready: Deque<Item> = []
    private(set) var outstandingMessages = 0
    private(set) var outstandingBytes = 0
    private(set) var isPaused = false
    private(set) var isReceiving = true

    init(flowControl: PullSubscriberOptions.FlowControl) {
      self.flowControl = flowControl
    }

    var isEmpty: Bool {
      entries.isEmpty
    }

    /// Number of messages to request in the next pull, or `nil` if receiving should wait.
    ///
    /// Requesting no more than the free message capacity means the message limit is never
    /// exceeded. The byte limit cannot be requested, so messages over it are held.
    var receivableCount: Int? {
      guard isReceiving, !isPaused, held.isEmpty else {
        return nil
      }
      let capacity = flowControl.maxOutstandingMessages - outstandingMessages
      return capacity > 0 ? min(capacity, Self.maxMessagesPerPull) : nil
    }

    var load: Double {
      max(
        Double(outstandingMessages) / Double(flowControl.maxOutstandingMessages),
        Double(outstandingBytes) / Double(flowControl.maxOutstandingBytes))
    }

    mutating func add(_ items: [Item]) -> [AckID] {
      guard isReceiving else {
        return items.map(\.ackID)
      }
      for item in items where entries[item.ackID] == nil {
        entries[item.ackID] = Entry(
          size: item.size, receivedAt: item.receivedAt, isDispatched: false)
        held.append(item)
      }
      dispatchHeld()
      return []
    }

    mutating func release(_ id: AckID) {
      guard let entry = entries.removeValue(forKey: id) else {
        return
      }
      if entry.isDispatched {
        outstandingMessages -= 1
        outstandingBytes -= entry.size
      } else {
        held.removeAll { $0.ackID == id }
      }
      dispatchHeld()
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

    // Limits are checked before dispatching, so a single message larger than the byte limit is
    // still dispatched once nothing else is outstanding.
    private mutating func dispatchHeld() {
      while let item = held.first,
        outstandingMessages < flowControl.maxOutstandingMessages,
        outstandingBytes < flowControl.maxOutstandingBytes
      {
        held.removeFirst()
        entries[item.ackID]?.isDispatched = true
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
    var receiveWaiter: CheckedContinuation<Int, any Error>?
    var dispatchWaiter: CheckedContinuation<Item?, Never>?

    mutating func takeResumptions() -> Resumptions {
      var resumptions = Resumptions()
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
        } else if let count = state.receivableCount {
          resumptions.receive = (waiter, .success(count))
          receiveWaiter = nil
        }
      }
      return resumptions
    }
  }

  private struct Resumptions: Sendable {
    var dispatch: (CheckedContinuation<Item?, Never>, Item?)?
    var receive: (CheckedContinuation<Int, any Error>, Result<Int, any Error>)?

    func resume() {
      if let (continuation, item) = dispatch {
        continuation.resume(returning: item)
      }
      if let (continuation, result) = receive {
        continuation.resume(with: result)
      }
    }
  }
}
