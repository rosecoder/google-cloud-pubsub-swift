import GRPCCore
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct PullSubscriberIntegrationTests {

  let projectID = "pull-subscriber-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveMessage() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PullSubscriberIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      // Setup publisher
      let publisher = Publisher(
        projectID: projectID,
        pubSubService: pubSubService
      )
      let publisherRunTask = Task {
        try await publisher.run()
      }

      // Setup callback handler
      let callbackContinuation = Mutex<CheckedContinuation<PlainTextMessage.Incoming, Never>?>(nil)
      let callbackTask = Task<PlainTextMessage.Incoming, Never> {
        await withCheckedContinuation { continuation in
          callbackContinuation.withLock {
            $0 = continuation
          }
        }
      }

      // Setup subscriber
      let handler = CallbackHandler(
        topic: topic,
        callback: { message in
          callbackContinuation.withLock {
            $0?.resume(returning: message)
          }
        }
      )
      let subscriber = PullSubscriber(
        handler: handler,
        projectID: projectID,
        pubSubService: pubSubService
      )
      let subscriberRunTask = Task {
        try await subscriber.run()
      }
      try await pubSubService.create(
        subscription: handler.subscription,
        subscriberClient: subscriber.client,
        publisherClient: subscriber.publisherClient,
        projectID: projectID
      )

      // Publish message
      let publishedMessage = try await publisher.publish(to: topic, body: "Hello")

      // Wait
      let receivedMessage = await callbackTask.value

      // Assert
      #expect(publishedMessage.id == receivedMessage.id)
      #expect(receivedMessage.body == "Hello")

      // Teardown
      shutdownTrigger.triggerGracefulShutdown()
      try await withThrowingDiscardingTaskGroup { group in
        group.addTask {
          publisherRunTask.cancel()
          try await publisherRunTask.value
        }
        group.addTask {
          subscriberRunTask.cancel()
          try await subscriberRunTask.value
        }
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldCreateMissingTopicAndSubscriptionWithInjectedService() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PullSubscriberIntegrationTests"))
    let subscriptionName = uniqueName("PullSubscriberIntegrationTests")

    try await testGracefulShutdown { shutdownTrigger in
      // Setup callback handler
      let (messageStream, messageContinuation) = AsyncStream.makeStream(
        of: PlainTextMessage.Incoming.self)

      // Setup subscriber for a topic and subscription that don't exist yet
      let handler = CallbackHandler(
        topic: topic,
        subscriptionName: subscriptionName,
        callback: { message in
          messageContinuation.yield(message)
        }
      )
      let subscriber = PullSubscriber(
        handler: handler,
        projectID: projectID,
        pubSubService: pubSubService
      )
      let subscriberRunTask = Task {
        try await subscriber.run()
      }

      // Wait for the subscriber to create topic and subscription
      try await waitUntilSubscriptionExists(
        id: handler.subscription.id(projectID: projectID),
        client: subscriber.client
      )
      let createdTopic = try await subscriber.publisherClient.getTopic(
        .with { $0.topic = topic.id(projectID: projectID) })
      #expect(createdTopic.name == topic.id(projectID: projectID))

      // Publish message without letting the publisher create the topic
      let publisher = Publisher(
        projectID: projectID,
        createTopicsIfNeeded: false,
        pubSubService: pubSubService
      )
      let publishedMessage = try await publisher.publish(to: topic, body: "Hello")

      // Wait
      let receivedMessage = try #require(await messageStream.first(where: { _ in true }))

      // Assert
      #expect(publishedMessage.id == receivedMessage.id)
      #expect(receivedMessage.body == "Hello")

      // Teardown
      messageContinuation.finish()
      shutdownTrigger.triggerGracefulShutdown()
      subscriberRunTask.cancel()
      try await subscriberRunTask.value
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldLimitConcurrentHandlersToMaxOutstandingMessages() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PullSubscriberIntegrationTests"))
    let messageCount = 50
    let recorder = HandlerRecorder()
    let (handledStream, handledContinuation) = AsyncStream.makeStream(of: String.self)

    try await testGracefulShutdown { shutdownTrigger in
      let handler = CallbackHandler(topic: topic) { message in
        await recorder.record(message.id) {
          try? await Task.sleep(for: .milliseconds(10))
        }
        handledContinuation.yield(message.id)
      }
      let subscriber = PullSubscriber(
        handler: handler,
        projectID: projectID,
        options: PullSubscriberOptions(flowControl: .init(maxOutstandingMessages: 5)),
        pubSubService: pubSubService
      )
      let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
      let publisherRunTask = Task {
        try await publisher.run()
      }
      try await pubSubService.create(
        subscription: handler.subscription,
        subscriberClient: subscriber.client,
        publisherClient: subscriber.publisherClient,
        projectID: projectID
      )
      let subscriberRunTask = Task {
        try await subscriber.run()
      }

      let published = try await publisher.publish(
        to: topic, bodies: (0..<messageCount).map { "Message \($0)" })

      var handled = Set<String>()
      for await id in handledStream {
        handled.insert(id)
        if handled.count == messageCount {
          break
        }
      }

      #expect(handled == Set(published.map(\.id)))
      #expect(recorder.peakConcurrency <= 5)
      #expect(recorder.peakConcurrency > 1)

      shutdownTrigger.triggerGracefulShutdown()
      try await subscriberRunTask.value
      publisherRunTask.cancel()
      try await publisherRunTask.value
    }
  }

  private func waitUntilSubscriptionExists(
    id: String,
    client: Google_Pubsub_V1_Subscriber.ClientProtocol
  ) async throws {
    while true {
      do {
        _ = try await client.getSubscription(.with { $0.subscription = id })
        return
      } catch let error as RPCError where error.code == .notFound {
        try await Task.sleep(for: .milliseconds(50))
      }
    }
  }
}
