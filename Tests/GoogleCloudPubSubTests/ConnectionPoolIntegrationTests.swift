import Foundation
import GRPCNIOTransportHTTP2Posix
import NIOCore
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct ConnectionPoolIntegrationTests {

  let projectID = "connection-pool-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldSpreadSubscriberStreamsOverConnections() async throws {
    let result = try await receiveOneMessageEach(subscriberCount: 5, maxStreamsPerConnection: 2)

    #expect(result.activeStreamCounts == [2, 2, 1])
    #expect(result.openConnections == 3)
  }

  @Test(
    .tags(.slow), .timeLimit(.minutes(2)),
    .enabled(
      if: ProcessInfo.processInfo.environment["PUBSUB_RUN_LONG_TESTS"] != nil,
      "Creates 135 subscriptions, so only runs when PUBSUB_RUN_LONG_TESTS is set"))
  func shouldReceiveOnMoreSubscriptionsThanOneConnectionAllows() async throws {
    let result = try await receiveOneMessageEach(subscriberCount: 135, maxStreamsPerConnection: 80)

    #expect(result.activeStreamCounts == [80, 55])
    #expect(result.openConnections == 2)
  }

  // MARK: - Helpers

  private struct Result {
    let activeStreamCounts: [Int]
    let openConnections: Int
  }

  /// Runs subscribers of their own topic on one shared service until each has received a message.
  ///
  /// - Returns: The streams per pooled client and the number of open TCP connections once all
  ///   subscribers streamed.
  private func receiveOneMessageEach(subscriberCount: Int, maxStreamsPerConnection: Int)
    async throws -> Result
  {
    // Failed connection attempts are closed right away, such as to `::1` when the emulator only
    // listens on IPv4.
    let openConnections = Mutex<Set<ObjectIdentifier>>([])
    let pubSubService = try PubSubService(
      maxStreamsPerConnection: maxStreamsPerConnection,
      channelDebuggingCallbacks: .init(
        onCreateTCPConnection: { channel in
          let id = ObjectIdentifier(channel)
          openConnections.withLock { _ = $0.insert(id) }
          channel.closeFuture.whenComplete { _ in
            openConnections.withLock { _ = $0.remove(id) }
          }
          return channel.eventLoop.makeSucceededVoidFuture()
        },
        onCreateHTTP2Stream: nil))
    let (received, receivedContinuation) = AsyncStream.makeStream(of: String.self)
    let topics = (0..<subscriberCount).map { _ in
      Topic<PlainTextMessage>(name: uniqueName("ConnectionPoolIntegrationTests"))
    }
    let handlers = topics.map { topic in
      CallbackHandler(topic: topic) { _ in receivedContinuation.yield(topic.name) }
    }
    let subscribers = handlers.map { handler in
      PullSubscriber(handler: handler, projectID: projectID, pubSubService: pubSubService)
    }
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)

    return try await testGracefulShutdown { shutdownTrigger in
      let publisherRun = Task { try await publisher.run() }
      let subscriberRuns = subscribers.map { subscriber in
        Task { try await subscriber.run() }
      }
      for (handler, subscriber) in zip(handlers, subscribers) {
        try await pubSubService.create(
          subscription: handler.subscription,
          subscriberClient: subscriber.client,
          publisherClient: subscriber.publisherClient,
          projectID: projectID)
      }
      try await waitUntil {
        pubSubService.pool.activeStreamCounts.reduce(0, +) == subscriberCount
      }
      let activeStreamCounts = pubSubService.pool.activeStreamCounts
      let openConnectionCount = openConnections.withLock { $0.count }

      for topic in topics {
        _ = try await publisher.publish(to: topic, body: "Hello")
      }
      var receivedTopics = Set<String>()
      for await topic in received {
        receivedTopics.insert(topic)
        if receivedTopics.count == subscriberCount {
          break
        }
      }
      #expect(receivedTopics == Set(topics.map(\.name)))

      shutdownTrigger.triggerGracefulShutdown()
      for run in subscriberRuns {
        try await run.value
      }
      publisherRun.cancel()
      try await publisherRun.value
      return Result(activeStreamCounts: activeStreamCounts, openConnections: openConnectionCount)
    }
  }
}
