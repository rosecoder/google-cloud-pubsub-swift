import ServiceLifecycleTestKit
import Testing

@testable import GoogleCloudPubSub

extension Topics {

  fileprivate static let test = Topic<PlainTextMessage>(name: "PublisherIntegrationTests")
}

@Suite(.tags(.integration))
struct PublisherIntegrationTests {

  @Test func shouldPublish() async throws {
    let pubSubService = try PubSubService()

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
      let message = try await publisher.publish(to: Topics.test, body: "Hello")

      // Assert
      #expect(message.id.isEmpty == false)

      // Teardown
      publisherRunTask.cancel()
      shutdownTrigger.triggerGracefulShutdown()
      try await publisherRunTask.value
    }
  }
}
