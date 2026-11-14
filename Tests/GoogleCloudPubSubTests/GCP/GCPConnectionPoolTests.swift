import Foundation
import GRPCNIOTransportHTTP2Posix
import NIOCore
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration, .gcp, .slow), .enabledAgainstGCP)
struct GCPConnectionPoolTests {

  @Test(.timeLimit(.minutes(10)))
  func shouldReceiveOnEverySubscriptionOfOneProcess() async throws {
    let topicCount = 9
    let subscriptionsPerTopic = 15

    try await withGCPResources { resources in
      let topics = Mutex<[Topic<PlainTextMessage>]>([])
      try await forEachConcurrently(Array(0..<topicCount)) { _ in
        let topic = try await resources.topic("pool", of: PlainTextMessage.self)
        topics.withLock { $0.append(topic) }
      }
      let subscriptions = Mutex<[Subscription<PlainTextMessage>]>([])
      let subscriptionTopics = topics.withLock { topics in
        topics.flatMap { Array(repeating: $0, count: subscriptionsPerTopic) }
      }
      try await forEachConcurrently(subscriptionTopics) { topic in
        let subscription = try await resources.subscription("pool", on: topic)
        subscriptions.withLock { $0.append(subscription) }
      }

      let openConnections = Mutex<Set<ObjectIdentifier>>([])
      let service = try PubSubService(
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
      let received = Mutex<Set<String>>([])
      let subscribers = subscriptions.withLock { $0 }.map { subscription in
        PullSubscriber(
          handler: CallbackHandler(subscription: subscription) { _ in
            received.withLock { _ = $0.insert(subscription.name) }
          },
          projectID: resources.projectID, pubSubService: service)
      }
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)

      try await withRunningSubscribers(subscribers, publishingWith: publisher) {
        try await poll(timeout: .seconds(60), "Opening every stream") {
          service.pool.activeStreamCounts.reduce(0, +) == subscribers.count
        }
        let activeStreamCounts = service.pool.activeStreamCounts
        let connectionCount = openConnections.withLock { $0.count }

        let start = ContinuousClock.now
        for topic in topics.withLock({ $0 }) {
          _ = try await publisher.publish(to: topic, body: "Hello")
        }
        try await poll(timeout: .seconds(60), "Delivery on every subscription") {
          received.withLock { $0.count } == subscribers.count
        }
        GCP.logger.info(
          "\(subscribers.count) subscriptions received in \(start.duration(to: .now)) with streams \(activeStreamCounts) over \(connectionCount) connections"
        )
        #expect(activeStreamCounts == [80, 55])
        #expect(connectionCount == 2)
      }
    }
  }
}
