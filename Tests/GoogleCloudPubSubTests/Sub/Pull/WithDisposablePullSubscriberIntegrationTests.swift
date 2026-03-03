import Logging
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

extension Topics {

  fileprivate static let test = Topic<PlainTextMessage>(
    name: "WithDisposablePullSubscriberIntegrationTests"
  )
}

@Suite(.tags(.integration))
struct WithDisposablePullSubscriberIntegrationTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveMessage() async throws {
    let pubSubService = try PubSubService()

    try await testGracefulShutdown { shutdownTrigger in
      // Setup publisher
      let publisher = Publisher(
        projectID: "disposable-pull-subscriber-integration-tests",
        pubSubService: pubSubService
      )
      let publisherRunTask = Task {
        try await publisher.run()
      }

      // Setup message channel
      let (messageStream, messageContinuation) = AsyncStream.makeStream(
        of: PlainTextMessage.Incoming.self)

      // Setup subscriber
      let subscription = Subscription(
        name: "test-\(Int.random(in: 0 ... .max))",
        topic: Topics.test
      )
      let subscriberRunTask = Task {
        try await withDisposablePullSubscriber(
          subscription: subscription,
          pubSubService: pubSubService,
          projectID: "disposable-pull-subscriber-integration-tests"
        ) { message in
          messageContinuation.yield(message)
        }
      }

      // Publish message
      try await Task.sleep(for: .milliseconds(200))
      let publishedMessage = try await publisher.publish(to: Topics.test, body: "Hello")

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
            try await publisherRunTask.value
          }
          group.addTask {
            subscriberRunTask.cancel()
            try await subscriberRunTask.value
          }
        }
      } catch is CancellationError {
        return
      }
    }
  }
}
