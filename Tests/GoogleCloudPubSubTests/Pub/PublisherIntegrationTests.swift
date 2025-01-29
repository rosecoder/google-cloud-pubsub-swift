import GoogleCloudPubSub
import ServiceLifecycleTestKit
import Testing

extension Topics {

  fileprivate static let test = Topic<PlainTextMessage>(name: "PublisherIntegrationTests")
}

@Suite(.tags(.integration))
struct PublisherIntegrationTests {

  @Test func shouldPublish() async throws {
    try await testGracefulShutdown { shutdownTrigger in
      // Setup
      let publisher = try await Publisher()
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
