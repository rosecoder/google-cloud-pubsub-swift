import ServiceLifecycleTestKit
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct PublisherIntegrationTests {

  @Test func shouldPublish() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PublisherIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      // Setup
      let publisher = Publisher(
        projectID: "publisher-integration-tests",
        pubSubService: pubSubService
      )
      let publisherRunTask = Task {
        try await publisher.run()
      }

      // Execute
      let message = try await publisher.publish(to: topic, body: "Hello")

      // Assert
      #expect(message.id.isEmpty == false)

      // Teardown
      publisherRunTask.cancel()
      shutdownTrigger.triggerGracefulShutdown()
      try await publisherRunTask.value
    }
  }

  @Test func shouldPublishMessageLargerThanDefaultGRPCMessageLimit() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PublisherIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      let publisher = Publisher(
        projectID: "publisher-integration-tests",
        pubSubService: pubSubService
      )
      let publisherRunTask = Task {
        try await publisher.run()
      }

      let message = try await publisher.publish(
        to: topic, body: String(repeating: "a", count: 5 * 1024 * 1024))

      #expect(message.id.isEmpty == false)

      publisherRunTask.cancel()
      shutdownTrigger.triggerGracefulShutdown()
      try await publisherRunTask.value
    }
  }
}
