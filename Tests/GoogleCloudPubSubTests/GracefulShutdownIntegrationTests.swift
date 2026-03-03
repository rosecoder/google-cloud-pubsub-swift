import Logging
import ServiceLifecycle
import Testing

@testable import GoogleCloudPubSub

extension Topics {

  fileprivate static let gracefulShutdown = Topic<PlainTextMessage>(name: "GracefulShutdownTests")
}

/// Reproduces a crash where both Publisher and PushSubscriber (with pull fallback) are used
/// together in a ServiceGroup and a graceful shutdown (e.g. SIGINT) causes Publisher to fail
/// with "Service finished unexpectedly", crashing the process.
@Suite(.tags(.integration))
struct GracefulShutdownIntegrationTests {

  private struct ShutdownTimedOut: Error {}

  @Test(.timeLimit(.minutes(1)))
  func publisherAndPushSubscriberShouldShutdownGracefully() async throws {
    let pubSubService = try PubSubService()

    let subscriber = PushSubscriber(
      projectID: "graceful-shutdown-tests",
      shouldFallbackToPull: true,
      pubSubService: pubSubService
    )
    subscriber.register(
      handler: CallbackHandler(topic: Topics.gracefulShutdown, callback: { _ in })
    )

    let publisher = Publisher(
      projectID: "graceful-shutdown-tests",
      pubSubService: pubSubService
    )

    let serviceGroup = ServiceGroup(
      services: [publisher, subscriber],
      gracefulShutdownSignals: [],
      logger: Logger(label: "test.graceful-shutdown")
    )

    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        try await serviceGroup.run()
      }

      // Let services start and settle.
      try await Task.sleep(for: .milliseconds(300))

      await serviceGroup.triggerGracefulShutdown()

      // Fail explicitly if shutdown doesn't complete within 5 seconds.
      group.addTask {
        try await Task.sleep(for: .seconds(5))
        throw ShutdownTimedOut()
      }

      try await group.next()
      group.cancelAll()
    }
  }
}
