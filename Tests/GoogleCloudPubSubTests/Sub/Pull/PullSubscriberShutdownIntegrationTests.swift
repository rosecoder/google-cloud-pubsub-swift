import Logging
import ServiceLifecycle
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct PullSubscriberShutdownIntegrationTests {

  let projectID = "pull-subscriber-shutdown-integration-tests"

  @Test(.tags(.slow), .timeLimit(.minutes(1)))
  func shouldFinishAndAckHandlerRunningAtShutdown() async throws {
    let handled = HandlerProbe()
    let subscription = try await runUntilShutdownDuringHandler(
      options: PullSubscriberOptions(shutdownBehavior: .waitForProcessing(timeout: .seconds(30)))
    ) { id in
      try await handled.record(id) {
        try await Task.sleep(for: .seconds(2))
      }
    }

    #expect(handled.finished.values.map { $0 } == [false])
    // An unacked message would be redelivered once its 10 second ack deadline has expired.
    #expect(try await firstDelivery(to: subscription, within: .seconds(12)) == nil)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRedeliverMessageNackedAtShutdownRightAway() async throws {
    let handled = HandlerProbe()
    let subscription = try await runUntilShutdownDuringHandler(
      options: PullSubscriberOptions(shutdownBehavior: .nackImmediately)
    ) { id in
      try await handled.record(id) {
        try await Task.sleep(for: .seconds(30))
      }
    }

    #expect(handled.finished.values.map { $0 } == [true])
    let delay = try #require(try await firstDelivery(to: subscription, within: .seconds(8)))
    #expect(delay < .seconds(5))
  }

  // MARK: - Helpers

  /// Runs a publisher and a subscriber in a service group, publishes one message and shuts the
  /// group down gracefully while the message is being handled.
  private func runUntilShutdownDuringHandler(
    options: PullSubscriberOptions,
    handle: @escaping @Sendable (String) async throws -> Void
  ) async throws -> Subscription<PlainTextMessage> {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PullSubscriberShutdownIntegrationTests"))
    let (started, startedContinuation) = AsyncStream.makeStream(of: Void.self)
    let handler = CallbackHandler(topic: topic) { message in
      startedContinuation.yield()
      try await handle(message.id)
    }
    let subscriber = PullSubscriber(
      handler: handler, projectID: projectID, options: options, pubSubService: pubSubService)
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let serviceGroup = ServiceGroup(
      services: [publisher, subscriber],
      gracefulShutdownSignals: [],
      logger: Logger(label: "test.pull-subscriber-shutdown"))

    let run = Task { try await serviceGroup.run() }
    try await pubSubService.create(
      subscription: handler.subscription,
      subscriberClient: subscriber.client,
      publisherClient: subscriber.publisherClient,
      projectID: projectID
    )
    _ = try await publisher.publish(to: topic, body: "Hello")
    _ = await started.first { _ in true }

    await serviceGroup.triggerGracefulShutdown()
    try await run.value
    return handler.subscription
  }

  /// - Returns: How long it took until a new subscriber received a message, or `nil` if it
  ///   received none within `limit`.
  private func firstDelivery(
    to subscription: Subscription<PlainTextMessage>, within limit: Duration
  ) async throws -> Duration? {
    let start = ContinuousClock.now
    let (deliveries, deliveryContinuation) = AsyncStream.makeStream(of: Duration.self)
    let handler = CallbackHandler(topic: subscription.topic, subscriptionName: subscription.name) {
      _ in
      deliveryContinuation.yield(start.duration(to: .now))
    }
    let subscriber = PullSubscriber(
      handler: handler, projectID: projectID, pubSubService: try PubSubService())

    return try await testGracefulShutdown { shutdownTrigger in
      let run = Task { try await subscriber.run() }
      let timeout = Task {
        try await Task.sleep(for: limit)
        deliveryContinuation.finish()
      }
      let delay = await deliveries.first { _ in true }
      timeout.cancel()
      shutdownTrigger.triggerGracefulShutdown()
      try await run.value
      return delay
    }
  }
}
