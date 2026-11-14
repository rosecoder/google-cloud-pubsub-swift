import GRPCNIOTransportHTTP2Posix
import Logging
import NIOCore
import ServiceLifecycle
import Synchronization
import Testing

@testable import GoogleCloudPubSub

/// Subscribers and publishers sharing a `PubSubService` in service groups, which shut their
/// services down one at a time.
@Suite(.tags(.integration))
struct SharedClientShutdownIntegrationTests {

  let projectID = "shared-client-shutdown-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldShutDownSubscribersAndPublisherInOneServiceGroup() async throws {
    let connections = OpenConnections()
    let pubSubService = try PubSubService(channelDebuggingCallbacks: connections.callbacks)
    let subscribers = makeSubscribers(count: 3, pubSubService: pubSubService)
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let serviceGroup = ServiceGroup(
      services: [publisher] + subscribers.subscribers, gracefulShutdownSignals: [],
      logger: Logger(label: "test.shared-client-shutdown"))

    let run = Task { try await serviceGroup.run() }
    defer { run.cancel() }
    try await publishAndReceiveOneMessageEach(
      subscribers, publisher: publisher, pubSubService: pubSubService)

    let start = ContinuousClock.now
    await serviceGroup.triggerGracefulShutdown()
    #expect(try await finishes(run, within: PubSubService.shutdownTimeout + .seconds(5)))
    #expect(start.duration(to: .now) < .seconds(5))
    #expect(pubSubService.clientUsage.hasBegunShutdown)
    #expect(connections.count == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldDrainSubscriberGroupConcurrentlyBeforePublisherInOneServiceGroup() async throws {
    let connections = OpenConnections()
    let pubSubService = try PubSubService(channelDebuggingCallbacks: connections.callbacks)
    let (drained, drainedContinuation) = AsyncStream.makeStream(of: Void.self)
    let subscribers = makeSubscribers(count: 5, pubSubService: pubSubService) {
      try await gracefulShutdown()
      try await Task.sleep(for: .seconds(1))
      drainedContinuation.yield()
    }
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let serviceGroup = ServiceGroup(
      services: [publisher, PullSubscriberGroup(subscribers.subscribers)],
      gracefulShutdownSignals: [], logger: Logger(label: "test.shared-client-shutdown"))

    let run = Task { try await serviceGroup.run() }
    try await publishAndReceiveOneMessageEach(
      subscribers, publisher: publisher, pubSubService: pubSubService)

    let start = ContinuousClock.now
    await serviceGroup.triggerGracefulShutdown()
    for await _ in drained.prefix(5) {}
    #expect(start.duration(to: .now) < .seconds(3))
    // A busy emulator can delay the final acks until the subscribers' shutdown timeout.
    #expect(try await finishes(run, within: .seconds(35)))
    #expect(pubSubService.clientUsage.hasBegunShutdown)
    #expect(connections.count == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldKeepPublishingAfterSubscribersSharingServiceExited() async throws {
    let connections = OpenConnections()
    let pubSubService = try PubSubService(channelDebuggingCallbacks: connections.callbacks)
    let subscribers = makeSubscribers(count: 3, pubSubService: pubSubService)
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
    let subscriberGroup = ServiceGroup(
      services: subscribers.subscribers, gracefulShutdownSignals: [],
      logger: Logger(label: "test.shared-client-shutdown.subscribers"))
    let publisherGroup = ServiceGroup(
      services: [publisher], gracefulShutdownSignals: [],
      logger: Logger(label: "test.shared-client-shutdown.publisher"))

    let publisherRun = Task { try await publisherGroup.run() }
    defer { publisherRun.cancel() }
    let subscriberRun = Task { try await subscriberGroup.run() }
    defer { subscriberRun.cancel() }
    try await publishAndReceiveOneMessageEach(
      subscribers, publisher: publisher, pubSubService: pubSubService)

    await subscriberGroup.triggerGracefulShutdown()
    #expect(try await finishes(subscriberRun, within: PubSubService.shutdownTimeout + .seconds(5)))
    #expect(!pubSubService.clientUsage.hasBegunShutdown)

    let topic = Topic<PlainTextMessage>(name: uniqueName("SharedClientShutdownIntegrationTests"))
    let published = try await publisher.publish(to: topic, body: "After subscribers")
    #expect(!published.id.isEmpty)

    let start = ContinuousClock.now
    await publisherGroup.triggerGracefulShutdown()
    #expect(try await finishes(publisherRun, within: PubSubService.shutdownTimeout + .seconds(5)))
    #expect(start.duration(to: .now) < .seconds(2))
    #expect(connections.count == 0)
  }

  // MARK: - Helpers

  private struct Subscribers {
    let subscribers: [PullSubscriber<CallbackHandler<PlainTextMessage>>]
    let subscriptions: [Subscription<PlainTextMessage>]
    let received: AsyncStream<String>
  }

  /// Subscribers of their own topic each, which report the topic of every message they receive
  /// before running `handle`.
  private func makeSubscribers(
    count: Int, pubSubService: PubSubService,
    handle: @escaping @Sendable () async throws -> Void = {}
  ) -> Subscribers {
    let (received, receivedContinuation) = AsyncStream.makeStream(of: String.self)
    let handlers = (0..<count).map { _ in
      let topic = Topic<PlainTextMessage>(name: uniqueName("SharedClientShutdownIntegrationTests"))
      return CallbackHandler(topic: topic) { _ in
        receivedContinuation.yield(topic.name)
        try await handle()
      }
    }
    return Subscribers(
      subscribers: handlers.map {
        PullSubscriber(handler: $0, projectID: projectID, pubSubService: pubSubService)
      },
      subscriptions: handlers.map(\.subscription),
      received: received)
  }

  private func publishAndReceiveOneMessageEach(
    _ subscribers: Subscribers, publisher: Publisher, pubSubService: PubSubService
  ) async throws {
    for (subscriber, subscription) in zip(subscribers.subscribers, subscribers.subscriptions) {
      try await pubSubService.create(
        subscription: subscription,
        subscriberClient: subscriber.client,
        publisherClient: subscriber.publisherClient,
        projectID: projectID)
    }
    let topics = subscribers.subscriptions.map(\.topic)
    for topic in topics {
      _ = try await publisher.publish(to: topic, body: "Hello")
    }
    var receivedTopics = Set<String>()
    for await topic in subscribers.received {
      receivedTopics.insert(topic)
      if receivedTopics.count == topics.count {
        break
      }
    }
    #expect(receivedTopics == Set(topics.map(\.name)))
  }
}

/// Counts TCP connections that haven't closed yet.
private final class OpenConnections: Sendable {

  private let open = Mutex<Set<ObjectIdentifier>>([])

  var count: Int {
    open.withLock { $0.count }
  }

  var callbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks {
    .init(
      onCreateTCPConnection: { channel in
        let id = ObjectIdentifier(channel)
        self.open.withLock { _ = $0.insert(id) }
        channel.closeFuture.whenComplete { _ in
          self.open.withLock { _ = $0.remove(id) }
        }
        return channel.eventLoop.makeSucceededVoidFuture()
      },
      onCreateHTTP2Stream: nil)
  }
}
