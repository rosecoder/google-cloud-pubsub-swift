import Foundation
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberFlowControlTests {

  let connection = FakeStreamingPullConnection()
  let handlerGate = Gate()
  let recorder = HandlerRecorder()

  @Test(.timeLimit(.minutes(1)))
  func shouldHandleMessagesConcurrently() async throws {
    let ackTransport = FakeAckTransport()
    connection.deliver([message("a"), message("b")])

    try await withSubscriber(ackTransport: ackTransport) {
      try await waitUntil { recorder.started.count == 2 }
      #expect(recorder.peakConcurrency == 2)

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 2 }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotStartMoreHandlersThanMaxOutstandingMessages() async throws {
    let ackTransport = FakeAckTransport()
    connection.deliver([message("a"), message("b"), message("c")])

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 2)
    ) {
      try await waitUntil { recorder.started.count == 2 }
      await yieldRepeatedly()
      #expect(Set(recorder.started) == ["a", "b"])
      #expect(try await initialRequest().maxOutstandingMessages == 2)

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 3 }
      #expect(recorder.started.sorted() == ["a", "b", "c"])
      #expect(recorder.peakConcurrency <= 2)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldKeepStartingHandlersWhileAcksAreInFlight() async throws {
    let ackGate = Gate()
    let ackTransport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        try await ackGate.wait()
      }
    }
    handlerGate.open()
    connection.deliver(["a", "b", "c", "d", "e"].map { message($0) })

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 2)
    ) {
      defer { ackGate.open() }
      try await waitUntil { Set(ackTransport.ids(of: .acknowledge)).count == 5 }
      #expect(recorder.started.sorted() == ["a", "b", "c", "d", "e"])
      #expect(recorder.peakConcurrency <= 2)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotStartMoreHandlersUntilAcksAreSentWithExactlyOnceDelivery() async throws {
    let ackGate = Gate()
    let ackTransport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        try await ackGate.wait()
      }
    }
    handlerGate.open()
    connection.send(
      .with {
        $0.subscriptionProperties.exactlyOnceDeliveryEnabled = true
        $0.receivedMessages = [message("a"), message("b")]
      })

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 1)
    ) {
      defer { ackGate.open() }
      try await waitUntil { ackTransport.ids(of: .acknowledge) == [AckID("ack-a")] }
      await yieldRepeatedly()
      #expect(recorder.started == ["a"])

      ackGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 2 }
      #expect(recorder.started == ["a", "b"])
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldHandleAtTheRateOfHandlersRegardlessOfAckLatency() async throws {
    let ackTransport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        try await Task.sleep(for: .milliseconds(100))
      }
    }
    let count = 200
    handlerGate.open()
    connection.deliver((0..<count).map { message("\($0)") })

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 10),
      handle: { _ in try await Task.sleep(for: .milliseconds(50)) }
    ) {
      let start = ContinuousClock.now
      try await waitUntil(timeout: .seconds(20)) { recorder.finishedCount == count }
      let rate = Double(count) / (start.duration(to: .now) / .seconds(1))
      // 10 handlers taking 50 ms each handle at most 200 messages per second.
      #expect(rate > 150, "Handled \(rate) messages per second")
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNackMessageWhenHandlerThrows() async throws {
    let ackTransport = FakeAckTransport()
    handlerGate.open()
    connection.deliver([message("good"), message("fail")])

    try await withSubscriber(ackTransport: ackTransport) {
      try await waitUntil {
        ackTransport.ids(of: .acknowledge) == [AckID("ack-good")]
          && ackTransport.ids(of: .modify(.nack)) == [AckID("ack-fail")]
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldHoldMessagesOverByteLimitWithoutNacking() async throws {
    let ackTransport = FakeAckTransport()
    connection.deliver([message("a", size: 8), message("b", size: 8), message("c", size: 8)])

    try await withSubscriber(
      ackTransport: ackTransport,
      flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 10)
    ) {
      try await waitUntil { recorder.started.count == 2 }
      #expect(try await initialRequest().maxOutstandingBytes == 10)

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 3 }
      #expect(recorder.started.sorted() == ["a", "b", "c"])
      #expect(recorder.peakConcurrency == 2)
      #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotReadResponsesWhilePaused() async throws {
    let ackTransport = FakeAckTransport()
    connection.deliver([message("a")])
    connection.deliver([message("b")])
    connection.deliver([message("c")])

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 1)
    ) {
      try await waitUntil { recorder.started == ["a"] && ackTransport.calls.count == 1 }
      for _ in 0..<1_000 {
        await Task.yield()
      }
      #expect(connection.pendingCount == 2)

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 3 }
      #expect(recorder.started == ["a", "b", "c"])
      #expect(connection.streams.count == 1)
    }
  }

  // MARK: - Leasing

  @Test(.timeLimit(.minutes(1)))
  func shouldExtendDeadlineOfEachReceivedBatchRightAway() async throws {
    let ackTransport = FakeAckTransport()
    connection.deliver([message("a"), message("b")])

    try await withSubscriber(ackTransport: ackTransport) {
      try await waitUntil { ackTransport.calls.count == 1 }
      connection.deliver([message("c")])
      try await waitUntil { ackTransport.calls.count == 2 }

      #expect(
        ackTransport.calls == [
          .init(.modify(.extend(.seconds(10))), [AckID("ack-a"), AckID("ack-b")]),
          .init(.modify(.extend(.seconds(10))), [AckID("ack-c")]),
        ])
      handlerGate.open()
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldFreeCapacityOfExpiredMessageWithoutNackingAndStillAckIt() async throws {
    let ackTransport = FakeAckTransport()
    let clock = TestClock()
    connection.deliver([message("slow", size: 10), message("next", size: 10)])

    try await withSubscriber(
      ackTransport: ackTransport,
      flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 10),
      lease: .init(maxLeaseDuration: .seconds(15)),
      clock: clock
    ) {
      try await waitUntil { recorder.started == ["slow"] && ackTransport.calls.count == 1 }

      // Renewals are at most 9 seconds apart, so the second is past the max lease duration. The
      // other sleeper is the stream's keepalive.
      for _ in 0..<2 {
        try await waitUntil { clock.sleeperCount == 2 }
        clock.advance(by: .seconds(9))
      }
      try await waitUntil { recorder.started == ["slow", "next"] && ackTransport.calls.count == 3 }

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 2 }
      #expect(Set(ackTransport.ids(of: .acknowledge)) == [AckID("ack-slow"), AckID("ack-next")])
      #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)
      let extensions = ackTransport.calls.filter { $0.kind == .modify(.extend(.seconds(10))) }
      #expect(
        extensions.map { Set($0.ids) } == [
          [AckID("ack-slow"), AckID("ack-next")],
          [AckID("ack-slow"), AckID("ack-next")],
          [AckID("ack-next")],
        ])
    }
  }

  // MARK: - Helpers

  /// The stream's requests are written concurrently with its responses being read.
  private func initialRequest() async throws -> Google_Pubsub_V1_StreamingPullRequest {
    try await waitUntil { connection.streams.first?.requests.isEmpty == false }
    return try #require(connection.streams.first?.requests.first)
  }

  private func yieldRepeatedly() async {
    for _ in 0..<1_000 {
      await Task.yield()
    }
  }

  /// Leases are only renewed, and shutdown timeouts only pass, when `clock` is advanced.
  private func withSubscriber(
    ackTransport: FakeAckTransport,
    flowControl: PullSubscriberOptions.FlowControl = .init(),
    lease: PullSubscriberOptions.Lease = .init(),
    clock: TestClock = TestClock(),
    handle: @escaping @Sendable (String) async throws -> Void = { _ in },
    _ body: () async throws -> Void
  ) async throws {
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberFlowControlTests"))
    ) { [recorder, handlerGate] message in
      try await recorder.record(message.id) {
        try await handlerGate.wait()
        try await handle(message.id)
        if message.id == "fail" {
          throw HandlerError()
        }
      }
    }
    let subscriber = PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-flow-control-tests",
      options: PullSubscriberOptions(flowControl: flowControl, lease: lease),
      pubSubService: try PubSubService(),
      streamingPullConnection: connection,
      ackTransport: ackTransport,
      clock: clock,
      leaseGenerator: SplitMix64(seed: 1)
    )

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { await subscriber.receiveAndProcess() }
      let result = await Result { try await body() }
      handlerGate.open()
      shutdownTrigger.triggerGracefulShutdown()
      await run.cancellableValue
      try result.get()
    }
  }

  private func message(_ id: String, size: Int = 0) -> Google_Pubsub_V1_ReceivedMessage {
    FakeStreamingPullConnection.message(id: id, size: size)
  }
}

private struct HandlerError: Error {}

final class HandlerRecorder: Sendable {

  private struct State {
    var started: [String] = []
    var running = 0
    var peakConcurrency = 0
    var finishedCount = 0
  }

  private let state = Mutex(State())

  var started: [String] {
    state.withLock { $0.started }
  }

  var peakConcurrency: Int {
    state.withLock { $0.peakConcurrency }
  }

  var finishedCount: Int {
    state.withLock { $0.finishedCount }
  }

  func record(_ id: String, _ body: () async throws -> Void) async rethrows {
    state.withLock { state in
      state.started.append(id)
      state.running += 1
      state.peakConcurrency = max(state.peakConcurrency, state.running)
    }
    defer {
      state.withLock { state in
        state.running -= 1
        state.finishedCount += 1
      }
    }
    try await body()
  }
}
