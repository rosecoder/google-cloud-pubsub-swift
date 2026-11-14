import Foundation
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

private struct FakePushMessage: Encodable {

  let message: Message
  let subscription: String

  struct Message: Encodable {

    let messageId: String
    let data: Data
    let publishTime: Date
    let attributes: [String: String]
  }
}

@Suite(.tags(.integration))
struct PushSubscriberIntegrationTests {

  let port = 45678

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveMessage() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PushSubscriberIntegrationTests"))

    try await testGracefulShutdown { shutdownTrigger in
      // Setup callback handler
      let (messageStream, messageContinuation) = AsyncStream.makeStream(
        of: PlainTextMessage.Incoming.self)

      // Setup subscriber
      let handler = CallbackHandler(
        topic: topic,
        callback: { message in
          messageContinuation.yield(message)
        }
      )
      let subscriber = PushSubscriber(
        projectID: "push-subscriber-integration-tests",
        port: port,
        shouldFallbackToPull: false,
        pubSubService: pubSubService
      )
      subscriber.register(handler: handler)
      let subscriberRunTask = Task {
        try await subscriber.run()
      }
      defer { subscriberRunTask.cancel() }

      // Publish message
      let publishedMessage = FakePushMessage(
        message: .init(
          messageId: "123",
          data: "Hello".data(using: .utf8)!,
          publishTime: Date(),
          attributes: [:]
        ),
        subscription: handler.subscription.id(projectID: "push-subscriber-integration-tests")
      )
      do {
        let encoder = JSONEncoder()

        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        encoder.dateEncodingStrategy = .formatted(dateFormatter)

        var urlRequest = URLRequest(url: URL(string: "http://localhost:\(port)/")!)
        urlRequest.httpMethod = "POST"
        urlRequest.httpBody = try encoder.encode(publishedMessage)
        _ = try await URLSession.shared.data(for: urlRequest)
      }

      // Wait
      let receivedMessage = try #require(await messageStream.first(where: { _ in true }))

      // Assert
      #expect(publishedMessage.message.messageId == receivedMessage.id)
      #expect(receivedMessage.body == "Hello")

      // Teardown
      messageContinuation.finish()
      shutdownTrigger.triggerGracefulShutdown()
      try await subscriberRunTask.cancellableValue
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldPullForHandlerRegisteredWhileRunningUsingPull() async throws {
    let projectID = "push-subscriber-integration-tests"
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("PushSubscriberIntegrationTests"))
    let (messages, messageContinuation) = AsyncStream.makeStream(of: String.self)
    let handler = CallbackHandler(topic: topic) { message in
      messageContinuation.yield(message.body)
    }

    try await testGracefulShutdown { shutdownTrigger in
      let subscriber = PushSubscriber(
        projectID: projectID, shouldFallbackToPull: true, pubSubService: pubSubService)
      let subscriberRunTask = Task {
        try await subscriber.run()
      }
      defer { subscriberRunTask.cancel() }
      try await Task.sleep(for: .milliseconds(100))
      subscriber.register(handler: handler)

      let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)
      try await waitUntilSubscriptionExists(
        id: handler.subscription.id(projectID: projectID),
        client: Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient))
      _ = try await publisher.publish(to: topic, body: "Hello")

      #expect(await messages.first { _ in true } == "Hello")

      shutdownTrigger.triggerGracefulShutdown()
      try await subscriberRunTask.cancellableValue
    }
  }
}
