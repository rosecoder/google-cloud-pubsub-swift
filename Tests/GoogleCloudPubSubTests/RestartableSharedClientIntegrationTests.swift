import GRPCCore
import Logging
import ServiceLifecycle
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

/// Using a `PubSubService` again after its clients shut down when their last user stopped.
///
/// Serialized because the tests using `PubSubService.shared` expect to be its only users.
@Suite(.tags(.integration), .serialized)
struct RestartableSharedClientIntegrationTests {

  let projectID = "restartable-shared-client-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveWithDisposablePullSubscriberTwiceOnSharedService() async throws {
    let pubSubService = try PubSubService.shared
    let topic = Topic<PlainTextMessage>(name: uniqueName("RestartableSharedClientIntegrationTests"))

    for round in 0..<2 {
      let generations = pubSubService.clientUsage.startedGenerations
      let subscription = Subscription(
        name: uniqueName("RestartableSharedClientIntegrationTests"), topic: topic)
      let (received, receivedContinuation) = AsyncStream.makeStream(of: String.self)

      try await testGracefulShutdown { shutdownTrigger in
        let subscriberRun = Task {
          try await withDisposablePullSubscriber(
            subscription: subscription, pubSubService: pubSubService, projectID: projectID
          ) { message in
            receivedContinuation.yield(message.body)
          }
        }
        try await waitUntilExists(subscription, pubSubService: pubSubService)

        let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
        _ = try await publisher.publish(to: topic, body: "Round \(round)")
        #expect(await received.first { _ in true } == "Round \(round)")

        shutdownTrigger.triggerGracefulShutdown()
        #expect(try await finishes(subscriberRun, within: .seconds(10)))
      }
      #expect(pubSubService.clientUsage.startedGenerations == generations + 1)
      #expect(pubSubService.clientUsage.hasBegunShutdown)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRunServiceGroupsOneAfterAnotherOnSharedService() async throws {
    for round in 0..<2 {
      let topic = Topic<PlainTextMessage>(
        name: uniqueName("RestartableSharedClientIntegrationTests"))
      let (received, receivedContinuation) = AsyncStream.makeStream(of: String.self)
      let handler = CallbackHandler(topic: topic) { receivedContinuation.yield($0.body) }
      let publisher = try Publisher(projectID: projectID)
      let subscriber = try PullSubscriber(handler: handler, projectID: projectID)
      let serviceGroup = ServiceGroup(
        services: [publisher, subscriber], gracefulShutdownSignals: [],
        logger: Logger(label: "test.restartable-shared-client"))

      let run = Task { try await serviceGroup.run() }
      try await PubSubService.shared.create(subscription: handler.subscription, projectID: projectID)
      _ = try await publisher.publish(to: topic, body: "Round \(round)")
      #expect(await received.first { _ in true } == "Round \(round)")

      await serviceGroup.triggerGracefulShutdown()
      #expect(try await finishes(run, within: .seconds(10)))
      #expect(try PubSubService.shared.clientUsage.hasBegunShutdown)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldPublishAfterEveryUserStopped() async throws {
    let pubSubService = try PubSubService()
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let topic = Topic<PlainTextMessage>(name: uniqueName("RestartableSharedClientIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { try await publisher.run() }
      _ = try await publisher.publish(to: topic, body: "While running")
      shutdownTrigger.triggerGracefulShutdown()
      try await run.value
    }
    #expect(pubSubService.clientUsage.hasBegunShutdown)

    for index in 0..<2 {
      let published = try await publisher.publish(to: topic, body: "After stopping \(index)")
      #expect(!published.id.isEmpty)
    }
    #expect(pubSubService.clientUsage.startedGenerations == 2)
    #expect(pubSubService.clientUsage.users == 0)
    #expect(pubSubService.clientUsage.isLingering)

    try await pubSubService.withClientInUse(.service) { _ in }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldPublishWithNewGenerationAfterLingerExpired() async throws {
    let pubSubService = try PubSubService(lingerDuration: .milliseconds(100))
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let topic = Topic<PlainTextMessage>(name: uniqueName("RestartableSharedClientIntegrationTests"))

    _ = try await publisher.publish(to: topic, body: "Before linger expired")
    try await waitUntil { pubSubService.clientUsage.hasBegunShutdown }
    let published = try await publisher.publish(to: topic, body: "After linger expired")

    #expect(!published.id.isEmpty)
    #expect(pubSubService.clientUsage.startedGenerations == 2)
    try await pubSubService.withClientInUse(.service) { _ in }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotDelayServiceGroupShutdownByLinger() async throws {
    let pubSubService = try PubSubService(lingerDuration: .seconds(60))
    let topic = Topic<PlainTextMessage>(name: uniqueName("RestartableSharedClientIntegrationTests"))
    let (received, receivedContinuation) = AsyncStream.makeStream(of: String.self)
    let handler = CallbackHandler(topic: topic) { receivedContinuation.yield($0.body) }
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let subscriber = PullSubscriber(
      handler: handler, projectID: projectID, pubSubService: pubSubService)

    try await pubSubService.create(subscription: handler.subscription, projectID: projectID)
    #expect(pubSubService.clientUsage.isLingering)

    let serviceGroup = ServiceGroup(
      services: [publisher, subscriber], gracefulShutdownSignals: [],
      logger: Logger(label: "test.restartable-shared-client.linger"))
    let run = Task { try await serviceGroup.run() }
    _ = try await publisher.publish(to: topic, body: "Hello")
    #expect(await received.first { _ in true } == "Hello")

    let start = ContinuousClock.now
    await serviceGroup.triggerGracefulShutdown()
    #expect(try await finishes(run, within: .seconds(10)))
    #expect(start.duration(to: .now) < .seconds(5))
    #expect(pubSubService.clientUsage.startedGenerations == 1)
    #expect(pubSubService.clientUsage.hasBegunShutdown)
  }

  /// Stopping cancels the connections rather than waiting for an RPC its last user left running.
  @Test(.timeLimit(.minutes(1)))
  func shouldCancelRPCLeftRunningWhenStoppingAndStartNewGeneration() async throws {
    let pubSubService = try PubSubService()
    let gate = Gate()
    let (opened, openedContinuation) = AsyncStream.makeStream(of: Void.self)

    let leakedRPC = Mutex<Task<Void, any Error>?>(nil)
    let stopping = Task {
      try await pubSubService.withClientInUse(.service) { clients in
        let rpc = Task {
          try await clients.subscriber.streamingPull(
            metadata: [:], options: .defaults,
            requestProducer: { _ in
              openedContinuation.yield()
              try await gate.wait()
            },
            onResponse: { response in
              for try await _ in response.messages {}
            })
        }
        leakedRPC.withLock { $0 = rpc }
        _ = await opened.first { _ in true }
      }
    }
    defer { gate.open() }

    #expect(try await finishes(stopping, within: .seconds(2)))
    #expect(pubSubService.clientUsage.hasBegunShutdown)
    let rpc = try #require(leakedRPC.withLock { $0 })
    await #expect(throws: (any Error).self) { _ = try await finishes(rpc, within: .seconds(2)) }

    let topic = Topic<PlainTextMessage>(name: uniqueName("RestartableSharedClientIntegrationTests"))
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let published = try await publisher.publish(to: topic, body: "After stopping")
    #expect(!published.id.isEmpty)
    #expect(pubSubService.clientUsage.startedGenerations == 2)
    try await pubSubService.withClientInUse(.service) { _ in }
  }

  // MARK: - Helpers

  private func waitUntilExists(
    _ subscription: Subscription<PlainTextMessage>, pubSubService: PubSubService
  ) async throws {
    try await pubSubService.withClientInUse { clients in
      while true {
        do {
          _ = try await clients.subscriber.getSubscription(
            .with { $0.subscription = subscription.id(projectID: projectID) })
          return
        } catch let error as RPCError where error.code == .notFound {
          try await Task.sleep(for: .milliseconds(50))
        }
      }
    }
  }
}
