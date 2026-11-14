import Logging
import ServiceLifecycle
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberShutdownTests {

  let connection = FakeStreamingPullConnection()
  let ackTransport = FakeAckTransport()
  let clock = TestClock()
  let probe = HandlerProbe()

  // MARK: - Nack immediately

  @Test(.timeLimit(.minutes(1)))
  func shouldNackHeldAndInFlightMessagesAndCancelHandlers() async throws {
    connection.deliver([message("a"), message("b")])

    try await withSubscriber(
      .nackImmediately, handle: { _ in try await Task.sleep(for: .seconds(3_600)) }
    ) { shutdown in
      try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
      shutdown()
    }

    let nacked = ackTransport.ids(of: .modify(.nack))
    #expect(nacked.count == 2)
    #expect(Set(nacked) == [AckID("ack-a"), AckID("ack-b")])
    #expect(ackTransport.ids(of: .acknowledge).isEmpty)
    #expect(probe.finished == ["a": true])
  }

  // MARK: - Wait for processing

  @Test(.timeLimit(.minutes(1)))
  func shouldLetHandlersFinishAndKeepLeasingWhileWaiting() async throws {
    let gate = Gate()
    connection.deliver([message("a"), message("b")])

    try await withSubscriber(
      .waitForProcessing(timeout: .seconds(30)), handle: { _ in try await gate.wait() }
    ) { shutdown in
      try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
      shutdown()
      try await waitUntil { ackTransport.ids(of: .modify(.nack)) == [AckID("ack-b")] }

      // The lease and the shutdown timeout are sleeping, and the stream's keepalive has stopped.
      try await waitUntil { !connection.isStreaming && clock.sleeperCount == 2 }
      clock.advance(by: .seconds(9))
      try await waitUntil { extensions.count == 2 }
      #expect(extensions.last?.ids == [AckID("ack-a")])

      gate.open()
    }

    #expect(ackTransport.ids(of: .acknowledge) == [AckID("ack-a")])
    #expect(ackTransport.ids(of: .modify(.nack)) == [AckID("ack-b")])
    #expect(probe.finished == ["a": false])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotReturnBeforeAcksQueuedDuringShutdownAreSent() async throws {
    let handlerGate = Gate()
    let ackGate = Gate()
    let ackTransport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        try await ackGate.wait()
      }
    }
    let returned = Flag()
    connection.deliver([message("a")])
    let subscriber = makeSubscriber(
      .waitForProcessing(timeout: .seconds(30)), ackTransport: ackTransport,
      handle: { _ in try await handlerGate.wait() })

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task {
        await subscriber.receiveAndProcess()
        returned.set()
      }
      defer { ackGate.open() }
      try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
      shutdownTrigger.triggerGracefulShutdown()
      try await waitUntil { !connection.isStreaming }
      handlerGate.open()

      try await waitUntil { ackTransport.ids(of: .acknowledge) == [AckID("ack-a")] }
      await yieldRepeatedly()
      #expect(!returned.isSet)

      ackGate.open()
      await run.cancellableValue
    }

    #expect(probe.finished == ["a": false])
    #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNackRemainingMessagesAndCancelHandlersOneSecondBeforeTimeout() async throws {
    connection.deliver([message("a")])

    try await withSubscriber(
      .waitForProcessing(timeout: .seconds(10)), handle: { _ in try? await Task.sleep(for: .seconds(3_600)) }
    ) { shutdown in
      try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
      shutdown()
      try await waitUntil { !connection.isStreaming && clock.sleeperCount == 2 }
      clock.advance(by: .seconds(8))
      await yieldRepeatedly()
      #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)

      clock.advance(by: .seconds(1))
    }

    #expect(ackTransport.ids(of: .modify(.nack)) == [AckID("ack-a")])
    // The handler returned normally after being cancelled, and its late ack was dropped.
    #expect(ackTransport.ids(of: .acknowledge).isEmpty)
    #expect(probe.finished == ["a": true])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldLimitShutdownTimeoutToMaxLeaseDuration() async throws {
    connection.deliver([message("a")])

    try await withSubscriber(
      .waitForProcessing(timeout: .seconds(30)),
      lease: .init(maxLeaseDuration: .seconds(5)),
      handle: { _ in try await Task.sleep(for: .seconds(3_600)) }
    ) { shutdown in
      try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
      shutdown()
      try await waitUntil { !connection.isStreaming && clock.sleeperCount == 2 }
      clock.advance(by: .seconds(4))
    }

    #expect(ackTransport.ids(of: .modify(.nack)) == [AckID("ack-a")])
    #expect(probe.finished == ["a": true])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotWaitWhenNothingIsOutstanding() async throws {
    try await withSubscriber(.waitForProcessing(timeout: .seconds(30))) { shutdown in
      try await waitUntil { connection.isReading }
      shutdown()
    }

    #expect(ackTransport.calls.isEmpty)
  }

  // MARK: - Cancellation

  @Test(.timeLimit(.minutes(1)))
  func shouldStopWithoutNackingWhenCancelled() async throws {
    connection.deliver([message("a"), message("b")])
    let subscriber = makeSubscriber(
      .waitForProcessing(timeout: .seconds(30)),
      handle: { _ in try await Task.sleep(for: .seconds(3_600)) })

    let run = Task { await subscriber.receiveAndProcess() }
    defer { run.cancel() }
    try await waitUntil { probe.started == ["a"] && ackTransport.calls.count == 1 }
    run.cancel()
    await run.cancellableValue

    #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)
    #expect(ackTransport.ids(of: .acknowledge).isEmpty)
    #expect(probe.finished == ["a": true])
  }

  // MARK: - Shared client

  @Test(.timeLimit(.minutes(1)))
  func shouldShutDownSharedClientOnlyAfterEverySubscriberFinished() async throws {
    let service = try PubSubService()
    let gate = Gate()
    let busy = makeSubscriber(
      .waitForProcessing(timeout: .seconds(30)), pubSubService: service,
      handle: { _ in try await gate.wait() })
    let idle = makeSubscriber(
      .waitForProcessing(timeout: .seconds(30)), pubSubService: service,
      connection: FakeStreamingPullConnection(), handle: { _ in })
    connection.deliver([message("a")])

    try await testGracefulShutdown { shutdownTrigger in
      let busyRun = Task { try await busy.run() }
      defer { busyRun.cancel() }
      let idleRun = Task { try await idle.run() }
      defer { idleRun.cancel() }
      try await waitUntil { probe.started == ["a"] && service.clientUsage.users == 2 }

      shutdownTrigger.triggerGracefulShutdown()
      try await idleRun.cancellableValue
      #expect(service.clientUsage.users == 1)
      #expect(!service.clientUsage.hasBegunShutdown)

      gate.open()
      try await busyRun.cancellableValue
      #expect(service.clientUsage.hasBegunShutdown)
      #expect(ackTransport.ids(of: .acknowledge) == [AckID("ack-a")])
    }
  }

  /// A service group shuts its services down one at a time and waits for each to return, so a
  /// subscriber must not wait for the others to release the shared client.
  @Test(.timeLimit(.minutes(1)), arguments: [2, 10])
  func shouldShutDownSubscribersSharingClientOneAfterAnotherInServiceGroup(
    subscriberCount: Int
  ) async throws {
    let service = try PubSubService()
    let gate = Gate()
    let connections = (0..<subscriberCount).map { _ in FakeStreamingPullConnection() }
    let subscribers = connections.map { connection in
      makeSubscriber(
        .waitForProcessing(timeout: .seconds(30)), pubSubService: service, connection: connection,
        handle: { _ in try await gate.wait() })
    }
    for (index, connection) in connections.enumerated() {
      connection.deliver([message("\(index)")])
    }
    let serviceGroup = ServiceGroup(
      services: subscribers, gracefulShutdownSignals: [],
      logger: Logger(label: "test.pull-subscriber-shutdown"))

    let run = Task { try await serviceGroup.run() }
    defer { run.cancel() }
    try await waitUntil { probe.started.count == subscriberCount }
    await serviceGroup.triggerGracefulShutdown()
    gate.open()

    #expect(try await finishes(run, within: PubSubService.shutdownTimeout + .seconds(5)))
    #expect(
      Set(ackTransport.ids(of: .acknowledge))
        == Set((0..<subscriberCount).map { AckID("ack-\($0)") }))
    #expect(service.clientUsage.users == 0)
    #expect(service.clientUsage.hasBegunShutdown)
  }

  // MARK: - Helpers

  private var extensions: [FakeAckTransport.Call] {
    ackTransport.calls.filter { $0.kind == .modify(.extend(.seconds(10))) }
  }

  private func yieldRepeatedly() async {
    for _ in 0..<1_000 {
      await Task.yield()
    }
  }

  /// Runs a subscriber that holds all but one message, since each is as large as the byte limit,
  /// until `body` has triggered shutdown and
  /// the subscriber has returned.
  private func withSubscriber(
    _ shutdownBehavior: PullSubscriberOptions.ShutdownBehavior,
    lease: PullSubscriberOptions.Lease = .init(),
    handle: @escaping @Sendable (String) async throws -> Void = { _ in },
    _ body: (_ shutdown: () -> Void) async throws -> Void
  ) async throws {
    let subscriber = makeSubscriber(shutdownBehavior, lease: lease, handle: handle)
    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { await subscriber.receiveAndProcess() }
      do {
        try await body { shutdownTrigger.triggerGracefulShutdown() }
      } catch {
        run.cancel()
        await run.cancellableValue
        throw error
      }
      await run.cancellableValue
    }
  }

  private func makeSubscriber(
    _ shutdownBehavior: PullSubscriberOptions.ShutdownBehavior,
    lease: PullSubscriberOptions.Lease = .init(),
    pubSubService: PubSubService? = nil,
    connection: FakeStreamingPullConnection? = nil,
    ackTransport: FakeAckTransport? = nil,
    handle: @escaping @Sendable (String) async throws -> Void
  ) -> PullSubscriber<CallbackHandler<DataMessage>> {
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberShutdownTests"))
    ) { [probe] message in
      try await probe.record(message.id) {
        try await handle(message.id)
      }
    }
    return PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-shutdown-tests",
      options: PullSubscriberOptions(
        flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 10), lease: lease,
        shutdownBehavior: shutdownBehavior),
      pubSubService: try! pubSubService ?? PubSubService(),
      streamingPullConnection: connection ?? self.connection,
      ackTransport: ackTransport ?? self.ackTransport,
      clock: clock,
      leaseGenerator: SplitMix64(seed: 1)
    )
  }

  private func message(_ id: String) -> Google_Pubsub_V1_ReceivedMessage {
    FakeStreamingPullConnection.message(id: id, size: 10)
  }
}

/// Records which handlers started and whether they were cancelled when they returned.
final class HandlerProbe: Sendable {

  private struct State {
    var started: [String] = []
    var finished: [String: Bool] = [:]
  }

  private let state = Mutex(State())

  var started: [String] {
    state.withLock { $0.started }
  }

  /// Whether each finished handler was cancelled, by message ID.
  var finished: [String: Bool] {
    state.withLock { $0.finished }
  }

  func record(_ id: String, _ body: () async throws -> Void) async throws {
    state.withLock { $0.started.append(id) }
    defer {
      let isCancelled = Task.isCancelled
      state.withLock { $0.finished[id] = isCancelled }
    }
    try await body()
  }
}

private final class Flag: Sendable {

  private let value = Mutex(false)

  var isSet: Bool {
    value.withLock { $0 }
  }

  func set() {
    value.withLock { $0 = true }
  }
}
