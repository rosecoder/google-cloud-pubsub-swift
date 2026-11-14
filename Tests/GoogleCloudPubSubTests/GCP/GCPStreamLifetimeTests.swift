import Foundation
import GRPCCore
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration, .gcp, .slow), .enabledAgainstGCP, .enabledForLongTests)
struct GCPStreamLifetimeTests {

  /// Publishes throughout the 900 second lifetime of a StreamingPull stream, so messages are
  /// published while it is closed and reopened.
  @Test(.timeLimit(.minutes(25)))
  func shouldReceiveEveryMessageAcrossStreamTimeout() async throws {
    try await withGCPResources { resources in
      let topic = try await resources.topic("lifetime", of: PlainTextMessage.self)
      let subscription = try await resources.subscription("lifetime", on: topic)
      let service = try PubSubService()
      let deliveries = Deliveries()
      let handler = CallbackHandler(subscription: subscription) { message in
        deliveries.record(message.id)
      }
      let connection = RecordingStreamingPullConnection(
        base: Google_Pubsub_V1_Subscriber.Client(wrapping: service.grpcClient))
      let subscriber = PullSubscriber(
        handler: handler, projectID: resources.projectID, pubSubService: service,
        streamingPullConnection: connection)
      let publisher = Publisher(projectID: resources.projectID, pubSubService: service)
      let start = ContinuousClock.now

      let published = try await withRunningSubscribers([subscriber], publishingWith: publisher) {
        var published: [(id: String, at: ContinuousClock.Instant)] = []
        while start.duration(to: .now) < .seconds(17 * 60) {
          let message = try await publisher.publish(to: topic, body: "\(published.count)")
          published.append((message.id, .now))
          try await Task.sleep(for: .seconds(5))
        }
        try await poll(timeout: .seconds(60), "Delivery of every published message") {
          deliveries.ids().isSuperset(of: published.map(\.id))
        }
        return published
      }

      for stream in connection.streams {
        GCP.logger.info("Stream opened at \(start.duration(to: stream.opened)): \(stream)")
      }
      let latencies = published.compactMap { message in
        deliveries.firstDelivered(message.id).map { message.at.duration(to: $0) }
      }
      GCP.logger.info(
        "Published \(published.count), delivered \(deliveries.distinct) distinct / \(deliveries.total) total, max latency \(latencies.max() ?? .zero), redelivered \(deliveries.redelivered)"
      )
      #expect(connection.streams.contains { $0.duration >= .seconds(15 * 60) })
      #expect(Set(published.map(\.id)).subtracting(deliveries.ids()).isEmpty)
      #expect(deliveries.redelivered.isEmpty)
    }
  }
}
