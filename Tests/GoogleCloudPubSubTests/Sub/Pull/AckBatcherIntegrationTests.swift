import GRPCCore
import Logging
import ServiceLifecycleTestKit
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct AckBatcherIntegrationTests {

  let projectID = "ack-batcher-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldNackExtendAndAckThroughGRPC() async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("AckBatcherIntegrationTests"))
    let subscription = Subscription(name: uniqueName("AckBatcherIntegrationTests"), topic: topic)
    let subscriberClient = Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)

    try await testGracefulShutdown { shutdownTrigger in
      let publisherRunTask = Task {
        try await publisher.run()
      }
      try await pubSubService.create(
        subscription: subscription,
        subscriberClient: subscriberClient,
        publisherClient: publisher.client,
        projectID: projectID
      )
      let batcher = AckBatcher(
        transport: GRPCAckTransport(
          client: subscriberClient, subscriptionID: subscription.id(projectID: projectID)),
        logger: Logger(label: "test"))

      try await withThrowingDiscardingTaskGroup { group in
        group.addTask { await batcher.run() }
        defer { batcher.finish() }

        let published = try await publisher.publish(to: topic, body: "Hello")

        let first = try await pullOne(subscription: subscription, client: subscriberClient)
        #expect(first.message.messageID == published.id)
        try await batcher.nack(AckID(first.ackID))

        // Only a nack makes the message available again before its 10 second ack deadline.
        let redelivered = try await pullOne(subscription: subscription, client: subscriberClient)
        #expect(redelivered.message.messageID == published.id)
        try await batcher.modifyDeadline(AckID(redelivered.ackID), to: .seconds(30))
        try await batcher.ack(AckID(redelivered.ackID))
      }

      shutdownTrigger.triggerGracefulShutdown()
      publisherRunTask.cancel()
      try await publisherRunTask.value
    }
  }

  private func pullOne(
    subscription: Subscription<PlainTextMessage>,
    client: Google_Pubsub_V1_Subscriber.ClientProtocol
  ) async throws -> Google_Pubsub_V1_ReceivedMessage {
    while true {
      let response = try await client.pull(
        .with {
          $0.subscription = subscription.id(projectID: projectID)
          $0.maxMessages = 1
        })
      if let message = response.receivedMessages.first {
        return message
      }
    }
  }
}
