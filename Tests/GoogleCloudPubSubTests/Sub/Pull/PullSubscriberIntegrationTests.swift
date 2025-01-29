import GoogleCloudPubSub
import ServiceLifecycleTestKit
import Synchronization
import Testing

extension Topics {

  fileprivate static let test = Topic<PlainTextMessage>(name: "PullSubscriberIntegrationTests")
}

@Suite(.tags(.integration))
struct PullSubscriberIntegrationTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveMessage() async throws {
    try await testGracefulShutdown { shutdownTrigger in
      // Setup publisher
      let publisher = try await Publisher()
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
      let subscriber = try await PullSubscriber(
        handler: CallbackHandler(
          topic: Topics.test,
          callback: { message in
            callbackContinuation.withLock {
              $0?.resume(returning: message)
            }
          }
        ))
      let subscriberRunTask = Task {
        try await subscriber.run()
      }

      // Publish message
      let publishedMessage = try await publisher.publish(to: Topics.test, body: "Hello")

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
}
