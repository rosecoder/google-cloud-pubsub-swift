import Foundation
import GRPCCore
import GRPCProtobuf
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration, .gcp), .enabledAgainstGCP)
struct GCPPullSubscriberTests {

  @Test(.tags(.slow), .timeLimit(.minutes(3)))
  func shouldReceiveAndAckEveryMessageWithoutRedelivery() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("smoke", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("smoke", on: topic)
      let service = try PubSubService()
      let deliveries = Deliveries()
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { deliveries.record($0.id) },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)
      let messageCount = 50

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        let start = ContinuousClock.now
        let published = try await publisher.publish(
          to: topic, bodies: (0..<messageCount).map { "Message \($0)" })
        try await poll(timeout: .seconds(60), "Delivery of every message") {
          deliveries.distinct == messageCount
        }
        GCP.logger.info("Received \(messageCount) messages in \(start.duration(to: .now))")
        #expect(deliveries.ids() == Set(published.map(\.id)))

        try await Task.sleep(for: .seconds(30))
        #expect(deliveries.redelivered.isEmpty)
      }
    }
  }

  @Test(.timeLimit(.minutes(3)))
  func shouldReceiveBacklogOfLargeMessages() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("large", of: DataMessage.self)
      let subscription = try await resources.subscription("large", on: topic)
      let messageCount = 8
      let body = Data(repeating: 0x61, count: 900 * 1024)
      _ = try await resources.publisherClient.publish(
        .with {
          $0.topic = topic.id(projectID: resources.projectID)
          $0.messages = (0..<messageCount).map { _ in .with { $0.data = body } }
        })

      let service = try PubSubService()
      let deliveries = Deliveries()
      let connection = RecordingStreamingPullConnection(
        base: Google_Pubsub_V1_Subscriber.Client(wrapping: service.grpcClient))
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { deliveries.record($0.id) },
        projectID: resources.projectID, pubSubService: service,
        streamingPullConnection: connection)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        let start = ContinuousClock.now
        try await poll(timeout: .seconds(90), "Delivery of every large message") {
          deliveries.distinct == messageCount
        }
        GCP.logger.info(
          "Received \(deliveries.distinct) of \(messageCount) large messages in \(start.duration(to: .now))"
        )
      }
      let failedStreams = connection.streams.filter { $0.code != nil }
      GCP.logger.info(
        "\(failedStreams.count) streams failed while receiving large messages: \(failedStreams)")
      #expect(failedStreams.isEmpty)
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(2)))
  func shouldLeaseMessageWhileHandlerRunsPastAckDeadline() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("lease", of: PlainTextMessage.self)
      let subscription = try await resources.subscription(
        "lease", on: topic, acknowledgeDeadline: 10)
      let service = try PubSubService()
      let deliveries = Deliveries()
      let handled = Mutex(false)
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { message in
          if deliveries.record(message.id) == 1 {
            try await Task.sleep(for: .seconds(25))
          }
          handled.withLock { $0 = true }
        },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        _ = try await publisher.publish(to: topic, body: "Slow")
        try await poll(timeout: .seconds(60), "Handling of the slow message") {
          handled.withLock { $0 }
        }

        // A message whose lease lapsed or whose ack was lost is redelivered within one deadline.
        try await Task.sleep(for: .seconds(15))
        #expect(deliveries.total == 1)
      }
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(2)))
  func shouldRedeliverNackedMessagesButNotAckedOnes() async throws {
    struct Rejected: Error {}

    try await withGCPResources { resources in
      let topic = try await resources.topic("nack", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("nack", on: topic)
      let service = try PubSubService()
      let deliveries = Deliveries()
      let firstDeliveries = Mutex<[String: ContinuousClock.Instant]>([:])
      let redeliveryDelays = Mutex<[Duration]>([])
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { message in
          if deliveries.record(message.id) == 1 {
            firstDeliveries.withLock { $0[message.id] = .now }
            throw Rejected()
          }
          if let first = firstDeliveries.withLock({ $0[message.id] }) {
            redeliveryDelays.withLock { $0.append(first.duration(to: .now)) }
          }
        },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)
      let messageCount = 5

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        _ = try await publisher.publish(
          to: topic, bodies: (0..<messageCount).map { "Message \($0)" })
        try await poll(timeout: .seconds(60), "Redelivery of every nacked message") {
          deliveries.total == 2 * messageCount
        }
        let delays = redeliveryDelays.withLock { $0 }
        GCP.logger.info("Nacked messages were redelivered after \(delays)")
        #expect(delays.allSatisfy { $0 < .seconds(5) })

        try await Task.sleep(for: .seconds(15))
        #expect(deliveries.total == 2 * messageCount)
      }
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(2)))
  func shouldAckInFlightMessagesOnGracefulShutdown() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("shutdown", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("shutdown", on: topic)
      let deliveries = Deliveries()
      let completed = Mutex(0)
      let messageCount = 20

      let service = try PubSubService()
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { message in
          deliveries.record(message.id)
          try await Task.sleep(for: .seconds(2))
          completed.withLock { $0 += 1 }
        },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)
      var shutdownStart = ContinuousClock.now
      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        _ = try await publisher.publish(
          to: topic, bodies: (0..<messageCount).map { "Message \($0)" })
        try await poll(timeout: .seconds(30), "Start of every handler") {
          deliveries.distinct == messageCount
        }
        shutdownStart = .now
      }
      GCP.logger.info(
        "Shut down after \(shutdownStart.duration(to: .now)) with \(completed.withLock { $0 }) handlers completed"
      )
      #expect(completed.withLock { $0 } == messageCount)

      let redeliveries = Deliveries()
      let restartedService = try PubSubService()
      let restarted = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { redeliveries.record($0.id) },
        projectID: resources.projectID, pubSubService: restartedService)
      let restartedPublisher = Publisher(
        projectID: resources.projectID, pubSubService: restartedService)
      try await withRunningSubscribers([restarted], publishingWith: restartedPublisher) {
        try await Task.sleep(for: .seconds(15))
      }
      #expect(redeliveries.total == 0)
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(2)))
  func shouldReceiveAndAckWithExactlyOnceDelivery() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("exactly-once", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("exactly-once", on: topic) {
        $0.enableExactlyOnceDelivery = true
      }
      let service = try PubSubService()
      let deliveries = Deliveries()
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { deliveries.record($0.id) },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)
      let messageCount = 10

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        let published = try await publisher.publish(
          to: topic, bodies: (0..<messageCount).map { "Message \($0)" })
        try await poll(timeout: .seconds(60), "Delivery of every message") {
          deliveries.distinct == messageCount
        }
        #expect(deliveries.ids() == Set(published.map(\.id)))

        try await Task.sleep(for: .seconds(15))
        #expect(deliveries.redelivered.isEmpty)
      }
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(2)))
  func shouldReportExpiredAckIDWithExactlyOnceDelivery() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("expired-ack", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("expired-ack", on: topic) {
        $0.enableExactlyOnceDelivery = true
      }
      let subscriptionID = subscription.id(projectID: resources.projectID)
      _ = try await resources.publisherClient.publish(
        .with {
          $0.topic = topic.id(projectID: resources.projectID)
          $0.messages = [.with { $0.data = Data("Expiring".utf8) }]
        })
      var received: [Google_Pubsub_V1_ReceivedMessage] = []
      while received.isEmpty {
        received = try await resources.subscriberClient.pull(
          .with {
            $0.subscription = subscriptionID
            $0.maxMessages = 1
          }
        ).receivedMessages
      }

      try await Task.sleep(for: .seconds(15))
      let expired = await #expect(throws: RPCError.self) {
        _ = try await resources.subscriberClient.acknowledge(
          .with {
            $0.subscription = subscriptionID
            $0.ackIds = [received[0].ackID]
          })
      }
      GCP.logger.info("Ack of expired ID: \(String(describing: expired)) \(details(of: expired))")
      let errorInfo = try expired?.unpackGoogleRPCStatus()?.details.compactMap(\.errorInfo).first
      #expect(expired?.code == .invalidArgument)
      #expect(errorInfo?.reason == "EXACTLY_ONCE_ACKID_FAILURE")
      #expect(errorInfo?.metadata == [received[0].ackID: "PERMANENT_FAILURE_INVALID_ACK_ID"])

      let invalid = await #expect(throws: RPCError.self) {
        _ = try await resources.subscriberClient.acknowledge(
          .with {
            $0.subscription = subscriptionID
            $0.ackIds = ["invalid-ack-id"]
          })
      }
      GCP.logger.info("Ack of invalid ID: \(String(describing: invalid)) \(details(of: invalid))")
      #expect(invalid?.code == .invalidArgument)
    }
  }

  private func details(of error: RPCError?) -> String {
    guard let error else {
      return ""
    }
    do {
      guard let status = try error.unpackGoogleRPCStatus() else {
        return "without status details, metadata \(error.metadata)"
      }
      return "with status details \(status.details)"
    } catch {
      return "with undecodable status details: \(error)"
    }
  }
}
