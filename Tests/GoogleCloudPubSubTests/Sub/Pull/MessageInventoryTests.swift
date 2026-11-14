import Testing

@testable import GoogleCloudPubSub

@Suite struct MessageInventoryStateTests {

  typealias State = MessageInventory.State

  @Test func shouldReceiveWhileBelowLimits() {
    var state = State(flowControl: flowControl(messages: 10))
    #expect(state.isReceivable)

    _ = state.add([item("a"), item("b"), item("c")])
    #expect(state.isReceivable)
  }

  @Test func shouldHoldMessagesOverMessageLimitAndNeverExceedIt() {
    var state = State(flowControl: flowControl(messages: 2))

    _ = state.add([item("a"), item("b"), item("c")])

    #expect(state.outstandingMessages == 2)
    #expect(state.held.map(\.ackID) == [AckID("ack-c")])
    #expect(state.ready.map(\.ackID) == [AckID("ack-a"), AckID("ack-b")])
  }

  @Test func shouldHoldMessagesOverByteLimit() {
    var state = State(flowControl: flowControl(messages: 10, bytes: 10))

    _ = state.add([item("a", size: 6), item("b", size: 6), item("c", size: 1)])

    #expect(state.ready.map(\.ackID) == [AckID("ack-a"), AckID("ack-b")])
    #expect(state.held.map(\.ackID) == [AckID("ack-c")])
    #expect(state.outstandingBytes == 12)
  }

  @Test func shouldNotReceiveWhileMessagesAreHeld() {
    var state = State(flowControl: flowControl(messages: 10, bytes: 10))
    _ = state.add([item("a", size: 10), item("b", size: 1)])
    #expect(!state.held.isEmpty)
    #expect(!state.isReceivable)

    state.release(AckID("ack-a"))
    #expect(state.held.isEmpty)
    #expect(state.isReceivable)
  }

  @Test func shouldPauseAtFullLoadAndResumeBelowEightyPercent() {
    var state = State(flowControl: flowControl(messages: 5))
    _ = state.add((0..<5).map { item("\($0)") })
    #expect(state.isPaused)
    #expect(!state.isReceivable)

    state.release(AckID("ack-0"))
    #expect(state.load == 0.8)
    #expect(state.isPaused)
    #expect(!state.isReceivable)

    state.release(AckID("ack-1"))
    #expect(!state.isPaused)
    #expect(state.isReceivable)
  }

  @Test func shouldApplyHysteresisToByteLoad() {
    var state = State(flowControl: flowControl(messages: 100, bytes: 10))
    _ = state.add([item("a", size: 5), item("b", size: 5)])
    #expect(state.isPaused)

    state.release(AckID("ack-a"))
    #expect(!state.isPaused)
  }

  @Test func shouldDispatchHeldMessagesInReceivedOrder() {
    var state = State(flowControl: flowControl(messages: 1))
    _ = state.add([item("a"), item("b"), item("c")])
    var dispatched = drain(&state)

    for id in ["a", "b", "c"] {
      state.release(AckID("ack-\(id)"))
      dispatched += drain(&state)
    }

    #expect(dispatched == ["ack-a", "ack-b", "ack-c"].map(AckID.init(rawValue:)))
    #expect(state.isEmpty)
  }

  @Test func shouldDispatchMessageLargerThanByteLimitWhenNothingIsOutstanding() {
    var state = State(flowControl: flowControl(messages: 10, bytes: 10))

    _ = state.add([item("large", size: 50), item("small", size: 1)])
    #expect(drain(&state) == [AckID("ack-large")])

    state.release(AckID("ack-large"))
    #expect(drain(&state) == [AckID("ack-small")])
  }

  @Test func shouldIgnoreReleaseOfUnknownOrAlreadyReleasedID() {
    var state = State(flowControl: flowControl(messages: 10))
    _ = state.add([item("a")])

    state.release(AckID("unknown"))
    #expect(state.outstandingMessages == 1)

    state.release(AckID("ack-a"))
    state.release(AckID("ack-a"))
    #expect(state.outstandingMessages == 0)
    #expect(state.outstandingBytes == 0)
  }

  @Test func shouldIgnoreDuplicateDelivery() {
    var state = State(flowControl: flowControl(messages: 10))

    _ = state.add([item("a"), item("a")])

    #expect(state.outstandingMessages == 1)
    #expect(drain(&state) == [AckID("ack-a")])
  }

  @Test func shouldReturnHeldMessagesWhenStoppingAndNackLaterAdds() {
    var state = State(flowControl: flowControl(messages: 1))
    _ = state.add([item("a"), item("b")])

    #expect(state.stopReceiving() == [AckID("ack-b")])
    #expect(!state.isReceivable)
    #expect(state.add([item("c")]).rejected == [AckID("ack-c")])
    #expect(state.entries.keys.sorted { $0.rawValue < $1.rawValue } == [AckID("ack-a")])
  }

  @Test func shouldNeverNackWhileReceiving() {
    var state = State(flowControl: flowControl(messages: 1, bytes: 1))

    let received = state.add((0..<10).map { item("\($0)", size: 100) })

    #expect(received.rejected.isEmpty)
    #expect(state.held.count == 9)
  }

  // MARK: - Shutdown

  @Test func shouldCloseAllUnclaimedMessagesAndDropTheirLaterResults() {
    var state = State(flowControl: flowControl(messages: 2))
    _ = state.add([item("a"), item("b"), item("c")])
    let claimedBeforeClosing = state.claim(AckID("ack-b"), for: .ack)
    #expect(claimedBeforeClosing)

    let closed = state.closeAll()

    #expect(Set(closed) == [AckID("ack-a"), AckID("ack-c")])
    #expect(state.ready.isEmpty)
    #expect(state.held.isEmpty)
    #expect(state.outstandingMessages == 1)
    let claimed = ["a", "b", "c"].map { state.claim(AckID("ack-\($0)"), for: .ack) }
    #expect(claimed == [false, true, false])
    let rejected = state.add([item("d")]).rejected
    #expect(rejected == [AckID("ack-d")])

    state.release(AckID("ack-b"))
    #expect(state.isEmpty)
    let closedAgain = state.closeAll()
    #expect(closedAgain.isEmpty)
  }

  @Test func shouldLetHandlersClaimMessagesNoLongerLeasedBeforeClosing() {
    var state = State(flowControl: flowControl(messages: 1), lease: lease(maxSeconds: 60))
    _ = state.add([item("a")], now: start)
    _ = state.renewLeases(now: start + .seconds(60))

    let claimed = state.claim(AckID("ack-a"), for: .ack)
    #expect(claimed)
  }

  // MARK: - Leasing

  @Test func shouldAcceptNewMessagesWithCurrentAckDeadline() {
    var state = State(flowControl: flowControl(messages: 1), initialAckDeadline: .seconds(30))
    _ = state.add([item("a")])

    let received = state.add([item("a"), item("b")])

    #expect(
      received
        == MessageInventory.Received(
          accepted: [AckID("ack-b")], rejected: [], ackDeadline: .seconds(30)))
  }

  @Test func shouldBaseAckDeadlineOnRecordedLatencies() {
    var state = State(flowControl: flowControl(messages: 1), initialAckDeadline: .seconds(30))
    #expect(state.ackDeadline == .seconds(30))

    state.recordAckLatency(.seconds(12))
    #expect(state.ackDeadline == .seconds(12))

    state.exactlyOnceDelivery = true
    #expect(state.ackDeadline == .seconds(60))
  }

  @Test func shouldRenewLeasesOfHeldAndDispatchedMessages() {
    var state = State(flowControl: flowControl(messages: 1), initialAckDeadline: .seconds(20))
    _ = state.add([item("a"), item("b")], now: start)

    let renewal = state.renewLeases(now: start + .seconds(5))

    #expect(renewal.ackDeadline == .seconds(20))
    #expect(Set(renewal.ids) == [AckID("ack-a"), AckID("ack-b")])
    #expect(renewal.expired.isEmpty)
  }

  @Test func shouldNotRenewLeasesOfClaimedOrReleasedMessagesButKeepRenewingHeldOnes() {
    var state = State(flowControl: flowControl(messages: 3))
    _ = state.add(
      [item("acked"), item("nacked"), item("released"), item("held")], now: start)
    _ = state.claim(AckID("ack-acked"), for: .ack)
    _ = state.claim(AckID("ack-nacked"), for: .nack)
    state.release(AckID("ack-released"), now: start)

    let renewal = state.renewLeases(now: start + .seconds(5))

    #expect(renewal.ids == [AckID("ack-held")])
    #expect(renewal.expired.isEmpty)
    #expect(state.outstandingMessages == 3)
  }

  @Test func shouldRenewLeasesOfAckedMessagesUntilReleasedWithExactlyOnceDelivery() {
    var state = State(flowControl: flowControl(messages: 2))
    state.exactlyOnceDelivery = true
    _ = state.add([item("acked"), item("nacked")], now: start)
    _ = state.claim(AckID("ack-acked"), for: .ack)
    _ = state.claim(AckID("ack-nacked"), for: .nack)

    #expect(state.renewLeases(now: start + .seconds(5)).ids == [AckID("ack-acked")])

    state.release(AckID("ack-acked"), now: start + .seconds(5))
    #expect(state.renewLeases(now: start + .seconds(10)).ids.isEmpty)
  }

  @Test func shouldStopLeasingMessagesDispatchedForMaxLeaseDurationAndFreeTheirCapacity() {
    var state = State(flowControl: flowControl(messages: 1), lease: lease(maxSeconds: 60))
    _ = state.add([item("a"), item("b")], now: start)
    #expect(drain(&state) == [AckID("ack-a")])

    #expect(state.renewLeases(now: start + .seconds(59)).expired.isEmpty)
    let renewal = state.renewLeases(now: start + .seconds(60))

    #expect(renewal.expired == [AckID("ack-a")])
    #expect(renewal.ids == [AckID("ack-b")])
    #expect(drain(&state) == [AckID("ack-b")])
    #expect(state.outstandingMessages == 1)

    state.release(AckID("ack-a"))
    #expect(state.outstandingMessages == 1)
  }

  @Test func shouldMeasureMaxLeaseDurationFromDispatchNotReceipt() {
    var state = State(flowControl: flowControl(messages: 1), lease: lease(maxSeconds: 60))
    _ = state.add([item("a"), item("b")], now: start)

    state.release(AckID("ack-a"), now: start + .seconds(50))
    let renewal = state.renewLeases(now: start + .seconds(100))

    #expect(renewal.ids == [AckID("ack-b")])
    #expect(renewal.expired.isEmpty)
  }

  @Test func shouldNeverExpireHeldMessages() {
    var state = State(flowControl: flowControl(messages: 1), lease: lease(maxSeconds: 60))
    _ = state.add([item("a"), item("b")], now: start)

    let renewal = state.renewLeases(now: start + .seconds(3_600))

    #expect(renewal.expired == [AckID("ack-a")])
    #expect(renewal.ids == [AckID("ack-b")])
  }

  // MARK: - Helpers

  private let start = ContinuousClock.now

  private func lease(maxSeconds: Int) -> PullSubscriberOptions.Lease {
    .init(maxLeaseDuration: .seconds(maxSeconds))
  }

  private func drain(_ state: inout State) -> [AckID] {
    var ids: [AckID] = []
    while let item = state.nextReady() {
      ids.append(item.ackID)
    }
    return ids
  }
}

@Suite(.timeLimit(.minutes(1)))
struct MessageInventoryTests {

  @Test func shouldReturnRightAwayWhenNotPaused() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 3))

    try await inventory.waitUntilReceivable()
  }

  @Test func shouldWaitUntilEnoughCapacityIsReleased() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 2))
    _ = inventory.add([message("a"), message("b")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    inventory.release(AckID("ack-a"))

    try await wait.cancellableValue
  }

  @Test func shouldReportWhetherExactlyOnceDeliveryChanged() {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))

    #expect(!inventory.setExactlyOnceDelivery(false))
    #expect(inventory.setExactlyOnceDelivery(true))
    #expect(inventory.exactlyOnceDelivery)
    #expect(inventory.ackDeadline == .seconds(60))
    #expect(!inventory.setExactlyOnceDelivery(true))
  }

  @Test func shouldThrowWhenWaitingTaskIsCancelled() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    _ = inventory.add([message("a")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    wait.cancel()

    await #expect(throws: CancellationError.self) { try await wait.cancellableValue }
  }

  @Test func shouldThrowWhenReceivingStops() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    _ = inventory.add([message("a")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    _ = inventory.stopReceiving()

    await #expect(throws: CancellationError.self) { try await wait.cancellableValue }
  }

  @Test func shouldYieldDispatchedMessagesInOrderAndEndAfterStopping() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 10))

    let collected = Task {
      var ids: [AckID] = []
      for await item in inventory.dispatched {
        ids.append(item.ackID)
      }
      return ids
    }
    _ = inventory.add([message("a"), message("b")], now: .now)
    _ = inventory.add([message("c")], now: .now)
    _ = inventory.stopReceiving()

    #expect(await collected.cancellableValue == ["ack-a", "ack-b", "ack-c"].map(AckID.init(rawValue:)))
  }

  @Test func shouldYieldAllDispatchedMessagesToCancelledConsumer() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 10))
    _ = inventory.add([message("a"), message("b")], now: .now)

    let collected = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      var ids: [AckID] = []
      for await item in inventory.dispatched {
        ids.append(item.ackID)
        if ids.count == 2 {
          _ = inventory.stopReceiving()
        }
      }
      return ids
    }

    #expect(await collected.cancellableValue == [AckID("ack-a"), AckID("ack-b")])
  }

  @Test func shouldDispatchHeldMessageWhenReleased() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    _ = inventory.add([message("a"), message("b")], now: .now)

    let iterator = inventory.dispatched.makeAsyncIterator()
    #expect(await iterator.next()?.ackID == AckID("ack-a"))

    inventory.release(AckID("ack-a"))
    #expect(await iterator.next()?.ackID == AckID("ack-b"))
    #expect(!inventory.isEmpty)

    inventory.release(AckID("ack-b"))
    #expect(inventory.isEmpty)
  }

  @Test func shouldWaitUntilNonEmptyUntilMessagesAreAdded() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    let wait = Task { try await inventory.waitUntilNonEmpty() }
    try await waitUntil { inventory.hasLeaseWaiter }

    _ = inventory.add([message("a")], now: .now)

    #expect(try await wait.cancellableValue)
    #expect(try await !inventory.waitUntilNonEmpty())
  }

  @Test func shouldWaitUntilEmptyUntilLastMessageIsReleased() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 10))
    try await inventory.waitUntilEmpty()
    _ = inventory.add([message("a"), message("b")], now: .now)

    let wait = Task { try await inventory.waitUntilEmpty() }
    inventory.release(AckID("ack-a"))
    #expect(inventory.claim(AckID("ack-b"), for: .ack))
    #expect(inventory.closeAll().isEmpty)
    inventory.release(AckID("ack-b"))

    try await wait.cancellableValue
  }

  @Test func shouldThrowWhenWaitingUntilEmptyIsCancelled() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 10))
    _ = inventory.add([message("a")], now: .now)
    let wait = Task { try await inventory.waitUntilEmpty() }

    wait.cancel()

    await #expect(throws: CancellationError.self) { try await wait.cancellableValue }
  }

  @Test func shouldWaitUntilMessagesAreAcceptedWithShorterDeadline() async throws {
    let inventory = MessageInventory(
      flowControl: flowControl(messages: 10), initialAckDeadline: .seconds(30))
    let wait = Task { try await inventory.waitUntilAccepted(shorterThan: .seconds(30)) }
    _ = inventory.add([message("a")], now: .now)
    for _ in 0..<100 {
      await Task.yield()
    }
    inventory.recordAckLatency(.seconds(12))

    _ = inventory.add([message("b")], now: .now)

    #expect(try await wait.cancellableValue == .seconds(12))
  }

  @Test func shouldReturnRightAwayWhenMessagesWereAcceptedWithShorterDeadlineSinceRenewal()
    async throws
  {
    let inventory = MessageInventory(
      flowControl: flowControl(messages: 10), initialAckDeadline: .seconds(20))
    _ = inventory.add([message("a")], now: .now)

    #expect(try await inventory.waitUntilAccepted(shorterThan: .seconds(30)) == .seconds(20))

    _ = inventory.renewLeases(now: .now)
    let wait = Task { try await inventory.waitUntilAccepted(shorterThan: .seconds(30)) }
    wait.cancel()
    await #expect(throws: CancellationError.self) { try await wait.cancellableValue }
  }

  @Test func shouldThrowWhenWaitingUntilNonEmptyIsCancelled() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    let wait = Task { try await inventory.waitUntilNonEmpty() }

    wait.cancel()

    await #expect(throws: CancellationError.self) { try await wait.cancellableValue }
  }
}

extension MessageInventory.State {

  fileprivate mutating func add(_ items: [MessageInventory.Item]) -> MessageInventory.Received {
    add(items, now: .now)
  }

  fileprivate mutating func release(_ id: AckID) {
    release(id, now: .now)
  }
}

private func flowControl(messages: Int, bytes: Int = 1_000_000)
  -> PullSubscriberOptions.FlowControl
{
  .init(maxOutstandingMessages: messages, maxOutstandingBytes: bytes)
}

private func message(_ id: String, size: Int = 0) -> Google_Pubsub_V1_ReceivedMessage {
  FakeStreamingPullConnection.message(id: id, size: size)
}

private func item(_ id: String, size: Int = 0) -> MessageInventory.Item {
  MessageInventory.Item(message(id, size: size), receivedAt: .now)
}
