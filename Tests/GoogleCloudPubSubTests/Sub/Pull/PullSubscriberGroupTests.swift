import Logging
import ServiceLifecycle
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
    #expect(Set(ackTransport.ids(of: .acknowledge)) == allAckIDs)
  }

  // MARK: - Helpers

  private var allAckIDs: Set<AckID> {
    Set((0..<Self.subscriberCount).map { AckID("ack-\($0)") })
  }

  /// Subscribers with one message each, whose handler takes ``drainDuration`` once its subscriber
  /// is shut down gracefully.
  private func makeDrainingSubscribers(
    pubSubService: PubSubService
  ) -> [PullSubscriber<CallbackHandler<DataMessage>>] {
    (0..<Self.subscriberCount).map { index in
      let connection = FakeStreamingPullConnection()
      connection.deliver([FakeStreamingPullConnection.message(id: "\(index)", size: 10)])
      return makeSubscriber(pubSubService: pubSubService, connection: connection) { _ in
        try await gracefulShutdown()
        try await Task.sleep(for: Self.drainDuration)
      }
    }
  }

  /// Runs `services` in a service group until every handler has started, then shuts it down.
  ///
  /// - Returns: How long the graceful shutdown took.
  private func shutDown(_ services: [any Service]) async throws -> Duration {
    let serviceGroup = ServiceGroup(
      services: services, gracefulShutdownSignals: [],
      logger: Logger(label: "test.pull-subscriber-group"))
    let run = Task { try await serviceGroup.run() }
    try await waitUntil { probe.started.count == Self.subscriberCount }

    let start = ContinuousClock.now
    await serviceGroup.triggerGracefulShutdown()
    #expect(try await finishes(run, within: .seconds(30)))
    return start.duration(to: .now)
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
