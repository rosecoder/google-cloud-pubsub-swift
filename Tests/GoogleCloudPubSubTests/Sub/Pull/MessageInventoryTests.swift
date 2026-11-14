import Testing

@testable import GoogleCloudPubSub

@Suite struct MessageInventoryStateTests {

  typealias State = MessageInventory.State

  @Test func shouldRequestFreeMessageCapacityCappedPerPull() {
    var state = State(flowControl: flowControl(messages: 10))
    #expect(state.receivableCount == 10)

    _ = state.add([item("a"), item("b"), item("c")])
    #expect(state.receivableCount == 7)

    let large = State(flowControl: flowControl(messages: 5_000))
    #expect(large.receivableCount == State.maxMessagesPerPull)
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
    #expect(state.receivableCount == nil)

    state.release(AckID("ack-a"))
    #expect(state.held.isEmpty)
    #expect(state.receivableCount == 9)
  }

  @Test func shouldPauseAtFullLoadAndResumeBelowEightyPercent() {
    var state = State(flowControl: flowControl(messages: 5))
    _ = state.add((0..<5).map { item("\($0)") })
    #expect(state.isPaused)
    #expect(state.receivableCount == nil)

    state.release(AckID("ack-0"))
    #expect(state.load == 0.8)
    #expect(state.isPaused)
    #expect(state.receivableCount == nil)

    state.release(AckID("ack-1"))
    #expect(!state.isPaused)
    #expect(state.receivableCount == 2)
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
    #expect(state.receivableCount == nil)
    #expect(state.add([item("c")]) == [AckID("ack-c")])
    #expect(state.entries.keys.sorted { $0.rawValue < $1.rawValue } == [AckID("ack-a")])
  }

  @Test func shouldNeverNackWhileReceiving() {
    var state = State(flowControl: flowControl(messages: 1, bytes: 1))

    let nacks = state.add((0..<10).map { item("\($0)", size: 100) })

    #expect(nacks.isEmpty)
    #expect(state.held.count == 9)
  }

  // MARK: - Helpers

  private func drain(_ state: inout State) -> [AckID] {
    var ids: [AckID] = []
    while let item = state.nextReady() {
      ids.append(item.ackID)
    }
    return ids
  }
}

@Suite struct MessageInventoryTests {

  @Test func shouldReturnReceivableCountRightAwayWhenNotPaused() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 3))

    #expect(try await inventory.waitUntilReceivable() == 3)
  }

  @Test func shouldWaitUntilEnoughCapacityIsReleased() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 2))
    _ = inventory.add([message("a"), message("b")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    inventory.release(AckID("ack-a"))

    #expect(try await wait.value == 1)
  }

  @Test func shouldThrowWhenWaitingTaskIsCancelled() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    _ = inventory.add([message("a")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    wait.cancel()

    await #expect(throws: CancellationError.self) { try await wait.value }
  }

  @Test func shouldThrowWhenReceivingStops() async throws {
    let inventory = MessageInventory(flowControl: flowControl(messages: 1))
    _ = inventory.add([message("a")], now: .now)

    let wait = Task { try await inventory.waitUntilReceivable() }
    _ = inventory.stopReceiving()

    await #expect(throws: CancellationError.self) { try await wait.value }
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

    #expect(await collected.value == ["ack-a", "ack-b", "ack-c"].map(AckID.init(rawValue:)))
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

    #expect(await collected.value == [AckID("ack-a"), AckID("ack-b")])
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
}

private func flowControl(messages: Int, bytes: Int = 1_000_000)
  -> PullSubscriberOptions.FlowControl
{
  .init(maxOutstandingMessages: messages, maxOutstandingBytes: bytes)
}

private func message(_ id: String, size: Int = 0) -> Google_Pubsub_V1_ReceivedMessage {
  FakePullTransport.message(id: id, size: size)
}

private func item(_ id: String, size: Int = 0) -> MessageInventory.Item {
  MessageInventory.Item(message(id, size: size), receivedAt: .now)
}
