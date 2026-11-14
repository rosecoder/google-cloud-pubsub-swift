import Logging
import ServiceLifecycle
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberGroupTests {

  let ackTransport = FakeAckTransport()
  let clock = TestClock()
  let probe = HandlerProbe()

  static let subscriberCount = 10
  static let drainDuration = Duration.milliseconds(500)

  // MARK: - Shutdown

  /// A service group signals each service only once the one after it has returned.
  @Test(.timeLimit(.minutes(1)))
  func shouldDrainSubscribersOneAfterAnotherWhenListedIndividually() async throws {
    let service = try PubSubService()
    let subscribers = makeDrainingSubscribers(pubSubService: service)

    let duration = try await shutDown(subscribers)

    #expect(duration >= Self.drainDuration * Self.subscriberCount)
    #expect(Set(ackTransport.ids(of: .acknowledge)) == ackIDs(count: Self.subscriberCount))
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldDrainSubscribersConcurrentlyInGroup() async throws {
    let service = try PubSubService()
    let subscribers = makeDrainingSubscribers(pubSubService: service)

    let duration = try await shutDown([PullSubscriberGroup(subscribers)])

    #expect(duration >= Self.drainDuration)
    #expect(duration < Self.drainDuration * 3)
    #expect(Set(ackTransport.ids(of: .acknowledge)) == ackIDs(count: Self.subscriberCount))
    #expect(probe.finished.values.allSatisfy { !$0 })
    #expect(service.clientUsage.users == 0)
    #expect(service.clientUsage.hasBegunShutdown)
  }

  // MARK: - Failure

  @Test(.timeLimit(.minutes(1)))
  func shouldShutDownOthersGracefullyAndThrowWhenSubscriberFails() async throws {
    let service = try PubSubService()
    let failure = Gate()
    let subscribers = makeSubscribers(count: 3, pubSubService: service) { _ in
      try await gracefulShutdown()
    }
    let serviceGroup = ServiceGroup(
      services: [PullSubscriberGroup([FailingService(gate: failure)] + subscribers)],
      gracefulShutdownSignals: [], logger: Logger(label: "test.pull-subscriber-group"))

    let run = Task { try await serviceGroup.run() }
    try await waitUntil { probe.started.count == 3 }
    failure.open()

    await #expect(throws: SubscriberFailure.self) { try await run.value }
    #expect(probe.finished == ["0": false, "1": false, "2": false])
    #expect(Set(ackTransport.ids(of: .acknowledge)) == ackIDs(count: 3))
    #expect(service.clientUsage.users == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldThrowErrorOfSubscriberFailingBeforeOthersStarted() async throws {
    let service = try PubSubService()
    let subscribers = makeSubscribers(count: 3, pubSubService: service) { _ in
      try await gracefulShutdown()
    }
    let group = PullSubscriberGroup([FailingService(gate: nil)] + subscribers)

    await #expect(throws: SubscriberFailure.self) { try await group.run() }
    #expect(service.clientUsage.users == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRunUntilGracefulShutdownWithoutSubscribers() async throws {
    try await testGracefulShutdown { shutdownTrigger in
      let hasReturned = Mutex(false)
      let run = Task {
        try await PullSubscriberGroup([]).run()
        hasReturned.withLock { $0 = true }
      }
      try await Task.sleep(for: .milliseconds(100))
      #expect(!hasReturned.withLock { $0 })

      shutdownTrigger.triggerGracefulShutdown()
      #expect(try await finishes(run, within: .seconds(5)))
    }
  }

  // MARK: - Cancellation

  @Test(.timeLimit(.minutes(1)))
  func shouldCancelEverySubscriberWhenCancelled() async throws {
    let service = try PubSubService()
    let subscribers = makeSubscribers(count: 3, pubSubService: service) { _ in
      try await Task.sleep(for: .seconds(3_600))
    }
    let group = PullSubscriberGroup(subscribers)

    let run = Task { try await group.run() }
    try await waitUntil { probe.started.count == 3 }
    run.cancel()

    #expect(try await finishes(run, within: .seconds(5)))
    #expect(probe.finished == ["0": true, "1": true, "2": true])
    #expect(ackTransport.ids(of: .acknowledge).isEmpty)
    #expect(service.clientUsage.users == 0)
  }

  // MARK: - Helpers

  private func ackIDs(count: Int) -> Set<AckID> {
    Set((0..<count).map { AckID("ack-\($0)") })
  }

  /// Subscribers with one message each, whose handler takes ``drainDuration`` once its subscriber
  /// is shut down gracefully.
  private func makeDrainingSubscribers(
    pubSubService: PubSubService
  ) -> [PullSubscriber<CallbackHandler<DataMessage>>] {
    makeSubscribers(count: Self.subscriberCount, pubSubService: pubSubService) { _ in
      try await gracefulShutdown()
      try await Task.sleep(for: Self.drainDuration)
    }
  }

  /// Runs `services` in a service group until every handler has started, then shuts it down.
  ///
  /// - Returns: How long it took until every message was acked. Closing the shared client
  ///   afterwards is left out, since it sometimes hangs until ``PubSubService/shutdownTimeout``.
  private func shutDown(_ services: [any Service]) async throws -> Duration {
    let serviceGroup = ServiceGroup(
      services: services, gracefulShutdownSignals: [],
      logger: Logger(label: "test.pull-subscriber-group"))
    let run = Task { try await serviceGroup.run() }
    try await waitUntil { probe.started.count == Self.subscriberCount }

    let start = ContinuousClock.now
    await serviceGroup.triggerGracefulShutdown()
    try await waitUntil(timeout: .seconds(30)) {
      ackTransport.ids(of: .acknowledge).count == Self.subscriberCount
    }
    let duration = start.duration(to: .now)
    #expect(try await finishes(run, within: PubSubService.shutdownTimeout + .seconds(5)))
    return duration
  }

  /// Subscribers with one message each, with IDs from 0.
  private func makeSubscribers(
    count: Int,
    pubSubService: PubSubService,
    handle: @escaping @Sendable (String) async throws -> Void
  ) -> [PullSubscriber<CallbackHandler<DataMessage>>] {
    (0..<count).map { index in
      let connection = FakeStreamingPullConnection()
      connection.deliver([FakeStreamingPullConnection.message(id: "\(index)", size: 10)])
      return makeSubscriber(pubSubService: pubSubService, connection: connection, handle: handle)
    }
  }

  private func makeSubscriber(
    pubSubService: PubSubService,
    connection: FakeStreamingPullConnection,
    handle: @escaping @Sendable (String) async throws -> Void
  ) -> PullSubscriber<CallbackHandler<DataMessage>> {
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberGroupTests"))
    ) { [probe] message in
      try await probe.record(message.id) {
        try await handle(message.id)
      }
    }
    return PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-group-tests",
      options: PullSubscriberOptions(
        flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 10),
        shutdownBehavior: .waitForProcessing(timeout: .seconds(30))),
      pubSubService: pubSubService,
      streamingPullConnection: connection,
      ackTransport: ackTransport,
      clock: clock,
      leaseGenerator: SplitMix64(seed: 1)
    )
  }
}

private struct SubscriberFailure: Error {}

/// Throws once `gate` opens, or right away without one.
private struct FailingService: Service {

  let gate: Gate?

  func run() async throws {
    await gate?.wait()
    throw SubscriberFailure()
  }
}
