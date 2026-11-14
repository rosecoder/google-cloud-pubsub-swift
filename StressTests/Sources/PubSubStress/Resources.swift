import Foundation
import GRPCCore
import Synchronization

@testable import GoogleCloudPubSub

/// Topics and subscriptions of one run. Subscription `i` subscribes to topic `i % topics.count`.
struct Resources: Sendable {

  let topics: [Topic<DataMessage>]
  let subscriptions: [Subscription<DataMessage>]

  init(config: Config) {
    let topics = (0..<config.topicCount).map {
      Topic<DataMessage>(name: config.namePrefix + "t\($0)", labels: ["purpose": "stress"])
    }
    self.topics = topics
    self.subscriptions = (0..<config.subscriptions).map {
      Subscription(
        name: config.namePrefix + "s\($0)",
        topic: topics[$0 % topics.count],
        labels: ["purpose": "stress"],
        acknowledgeDeadline: config.ackDeadline,
        expirationPolicyDuration: 24 * 3600,
        messageRetentionDuration: config.retention)
    }
  }

  func topicIndex(ofSubscription index: Int) -> Int {
    index % topics.count
  }
}

/// Creates and deletes resources over its own connections, independent of the services under test.
struct Admin: Sendable {

  let service: PubSubService
  let projectID: String

  var subscriberClient: Google_Pubsub_V1_Subscriber.Client<some ClientTransport> {
    Google_Pubsub_V1_Subscriber.Client(wrapping: service.grpcClient)
  }

  var publisherClient: Google_Pubsub_V1_Publisher.Client<some ClientTransport> {
    Google_Pubsub_V1_Publisher.Client(wrapping: service.grpcClient)
  }

  func create(_ resources: Resources) async throws {
    try await forEachConcurrently(resources.topics) { topic in
      try await service.create(topic: topic, publisherClient: publisherClient, projectID: projectID)
    }
    try await forEachConcurrently(resources.subscriptions) { subscription in
      try await service.create(
        subscription: subscription, createTopicIfNeeded: false, subscriberClient: subscriberClient,
        publisherClient: publisherClient, projectID: projectID)
    }
  }

  /// Deletes the run's resources, ignoring ones that don't exist.
  func delete(_ resources: Resources) async {
    await deleteSubscriptions(resources.subscriptions.map { $0.id(projectID: projectID) })
    await deleteTopics(resources.topics.map { $0.id(projectID: projectID) })
  }

  /// Deletes every topic and subscription of any stress run in the project.
  func deleteAllStressResources() async throws -> (topics: [String], subscriptions: [String]) {
    let subscriptions = try await listSubscriptions()
    await deleteSubscriptions(subscriptions)
    let topics = try await listTopics()
    await deleteTopics(topics)
    return (topics, subscriptions)
  }

  func listTopics() async throws -> [String] {
    var names: [String] = []
    var pageToken = ""
    repeat {
      let response = try await publisherClient.listTopics(
        .with {
          $0.project = "projects/\(projectID)"
          $0.pageSize = 1000
          $0.pageToken = pageToken
        })
      names += response.topics.map(\.name).filter { Self.isStressResource($0) }
      pageToken = response.nextPageToken
    } while !pageToken.isEmpty
    return names
  }

  func listSubscriptions() async throws -> [String] {
    var names: [String] = []
    var pageToken = ""
    repeat {
      let response = try await subscriberClient.listSubscriptions(
        .with {
          $0.project = "projects/\(projectID)"
          $0.pageSize = 1000
          $0.pageToken = pageToken
        })
      names += response.subscriptions.map(\.name).filter { Self.isStressResource($0) }
      pageToken = response.nextPageToken
    } while !pageToken.isEmpty
    return names
  }

  private static func isStressResource(_ name: String) -> Bool {
    name.split(separator: "/").last?.hasPrefix(Config.namePrefix) == true
  }

  private func deleteSubscriptions(_ ids: [String]) async {
    await forEachConcurrentlyLogging(ids) { id in
      _ = try await subscriberClient.deleteSubscription(.with { $0.subscription = id })
    }
  }

  private func deleteTopics(_ ids: [String]) async {
    await forEachConcurrentlyLogging(ids) { id in
      _ = try await publisherClient.deleteTopic(.with { $0.topic = id })
    }
  }

  private func forEachConcurrentlyLogging(
    _ ids: [String], _ body: @escaping @Sendable (String) async throws -> Void
  ) async {
    try? await forEachConcurrently(ids) { id in
      for attempt in 1...5 {
        do {
          try await body(id)
          return
        } catch let error as RPCError where error.code == .notFound {
          return
        } catch {
          print("Failed to delete \(id) (attempt \(attempt)): \(error)")
          try? await Task.sleep(for: .seconds(attempt))
        }
      }
    }
  }
}

func forEachConcurrently<Element: Sendable>(
  _ elements: [Element], width: Int = 16, _ body: @escaping @Sendable (Element) async throws -> Void
) async throws {
  let next = Atomic<Int>(0)
  try await withThrowingDiscardingTaskGroup { group in
    for _ in 0..<min(width, elements.count) {
      group.addTask {
        while true {
          let index = next.wrappingAdd(1, ordering: .relaxed).oldValue
          guard index < elements.count else { return }
          try await body(elements[index])
        }
      }
    }
  }
}
