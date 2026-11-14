import Logging
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct WithDisposablePullSubscriberIntegrationTests {

  let projectID = "disposable-pull-subscriber-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveMessage() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(
      name: uniqueName("WithDisposablePullSubscriberIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      // Setup publisher
      let publisher = Publisher(
        projectID: projectID,
        pubSubService: pubSubService
      )
      let publisherRunTask = Task {
        try await publisher.run()
      }
      defer { publisherRunTask.cancel() }

      // Setup message channel
      let (messageStream, messageContinuation) = AsyncStream.makeStream(
        of: PlainTextMessage.Incoming.self)

      // Setup subscriber
      let subscription = Subscription(
        name: uniqueName("WithDisposablePullSubscriberIntegrationTests"),
        topic: topic
      )
      let subscriberRunTask = Task {
        try await withDisposablePullSubscriber(
          subscription: subscription,
          pubSubService: pubSubService,
          projectID: projectID
        ) { message in
          messageContinuation.yield(message)
        }
      }
      defer { subscriberRunTask.cancel() }

      // Publish message once the subscription exists, as it wouldn't receive it otherwise
      try await waitUntilSubscriptionExists(
        id: subscription.id(projectID: projectID),
        client: Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
      )
      let publishedMessage = try await publisher.publish(to: topic, body: "Hello")

      // Wait for message
      let receivedMessage = try #require(await messageStream.first(where: { _ in true }))

      // Assert
      #expect(publishedMessage.id == receivedMessage.id)
      #expect(receivedMessage.body == "Hello")

      // Teardown
      messageContinuation.finish()
      shutdownTrigger.triggerGracefulShutdown()
      do {
        try await withThrowingDiscardingTaskGroup { group in
          group.addTask {
            publisherRunTask.cancel()
            try await publisherRunTask.cancellableValue
          }
          group.addTask {
            subscriberRunTask.cancel()
            try await subscriberRunTask.cancellableValue
          }
        }
      } catch is CancellationError {
        return
      }
    }
  }
}
