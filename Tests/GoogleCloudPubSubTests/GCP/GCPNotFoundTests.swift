import Foundation
import GRPCCore
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration, .gcp), .enabledAgainstGCP)
struct GCPNotFoundTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldFailPublishToMissingTopicWithNotFound() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("missing", of: PlainTextMessage.self, create: false)

      let error = await #expect(throws: RPCError.self) {
        _ = try await resources.publisherClient.publish(
          .with {
            $0.topic = topic.id(projectID: resources.projectID)
            $0.messages = [.with { $0.data = Data("Hello".utf8) }]
          })
      }
      GCP.logger.info("Publish to missing topic: \(String(describing: error))")
      #expect(error?.code == .notFound)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func publisherShouldCreateMissingTopic() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("auto-create", of: PlainTextMessage.self, create: false)
      let service = try PubSubService()
      let publisher = Publisher(
        projectID: resources.projectID, createTopicsIfNeeded: true, pubSubService: service)

      try await withRunningSubscribers(
        [PullSubscriber<CallbackHandler<PlainTextMessage>>](), publishingWith: publisher
      ) {
        await withKnownIssue(
          "Publisher only creates topics on the emulator's \"Topic not found\" message, while Pub/Sub responds with \"Resource not found (resource=…)\""
        ) {
          let message = try await publisher.publish(to: topic, body: "Hello")
          #expect(!message.id.isEmpty)
        }
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldFailStreamingPullOnMissingSubscriptionWithNotFound() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("missing", of: PlainTextMessage.self, create: false)
      let subscription = try await resources.subscription("missing", on: topic, create: false)
      let subscriptionID = subscription.id(projectID: resources.projectID)

      let error = await #expect(throws: RPCError.self) {
        try await resources.subscriberClient.streamingPull(
          metadata: ["x-goog-request-params": "subscription=\(subscriptionID)"],
          requestProducer: { writer in
            try await writer.write(
              .with {
                $0.subscription = subscriptionID
                $0.streamAckDeadlineSeconds = 10
              })
          },
          onResponse: { response in
            for try await _ in response.messages {}
          })
      }
      GCP.logger.info("StreamingPull on missing subscription: \(String(describing: error))")
      #expect(error?.code == .notFound)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldFailCreateSubscriptionOnMissingTopicWithNotFound() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("missing", of: PlainTextMessage.self, create: false)

      let error = await #expect(throws: RPCError.self) {
        _ = try await resources.subscription("missing", on: topic)
      }
      GCP.logger.info("Create subscription on missing topic: \(String(describing: error))")
      #expect(error?.code == .notFound)
    }
  }

  @Test(.timeLimit(.minutes(2)))
  func pullSubscriberShouldCreateMissingTopicAndSubscription() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("auto-create", of: PlainTextMessage.self, create: false)
      let subscription = try await resources.subscription(
        "auto-create", on: topic, create: false)
      let service = try PubSubService()
      let deliveries = Deliveries()
      let subscriber = PullSubscriber(
        handler: CallbackHandler(subscription: subscription) { deliveries.record($0.id) },
        projectID: resources.projectID, pubSubService: service)
      let publisher = Publisher(
        projectID: resources.projectID, createTopicsIfNeeded: false, pubSubService: service)

      try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        let subscriptionID = subscription.id(projectID: resources.projectID)
        try await waitUntilSubscriptionExists(id: subscriptionID, client: resources.subscriberClient)
        let message = try await publisher.publish(to: topic, body: "Hello")
        try await poll(timeout: .seconds(60), "Delivery on the created subscription") {
          deliveries.count(of: message.id) == 1
        }
      }
    }
  }

  private func waitUntilSubscriptionExists(
    id: String,
    client: some Google_Pubsub_V1_Subscriber.ClientProtocol
  ) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(60))
    while true {
      do {
        _ = try await client.getSubscription(.with { $0.subscription = id })
        return
      } catch let error as RPCError where error.code == .notFound {
        guard ContinuousClock.now < deadline else {
          throw error
        }
        try await Task.sleep(for: .milliseconds(500))
      }
    }
  }
}
