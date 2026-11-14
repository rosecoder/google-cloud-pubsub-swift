import Foundation
import GoogleCloudAuth
import GoogleCloudPubSub
import NIOPosix
import Synchronization

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Topics and subscriptions of one run. Subscription `i` subscribes to topic `i % topics.count`.
struct Resources: Sendable {

  let topics: [Topic<DataMessage>]
  let subscriptions: [Subscription<DataMessage>]
  let ackDeadline: Int
  let retention: Int

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
    self.ackDeadline = Int(config.ackDeadline)
    self.retention = Int(config.retention)
  }

  func topicIndex(ofSubscription index: Int) -> Int {
    index % topics.count
  }
}

struct AdminError: Error, CustomStringConvertible {
  let status: Int
  let body: String
  var description: String { "HTTP \(status): \(body.prefix(300))" }
}

/// Creates, lists and deletes resources through Pub/Sub's REST API, since the library has no public
/// admin API. Independent of the connections under test.
struct Admin: Sendable {

  let projectID: String
  private let baseURL: String
  private let authorization: Authorization?

  init(projectID: String) {
    self.projectID = projectID
    if let host = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"] {
      baseURL = "http://\(host)/v1"
      authorization = nil
    } else {
      baseURL = "https://pubsub.googleapis.com/v1"
      authorization = Authorization(
        scopes: ["https://www.googleapis.com/auth/pubsub"],
        eventLoopGroup: MultiThreadedEventLoopGroup.singleton)
    }
  }

  private func request(
    _ method: String, _ path: String, body: [String: Any]? = nil
  ) async throws -> (status: Int, json: [String: Any]) {
    var request = URLRequest(url: URL(string: baseURL + "/" + path)!)
    request.httpMethod = method
    request.timeoutInterval = 60
    if let authorization {
      request.setValue(
        "Bearer \(try await authorization.accessToken())", forHTTPHeaderField: "Authorization")
    }
    if let body {
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
      request.httpBody = try JSONSerialization.data(withJSONObject: body)
    }
    let (data, response) = try await URLSession.shared.data(for: request)
    let status = (response as? HTTPURLResponse)?.statusCode ?? 0
    let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] ?? [:]
    if status >= 300 {
      throw AdminError(status: status, body: String(decoding: data, as: UTF8.self))
    }
    return (status, json)
  }

  private func withRetries(_ body: () async throws -> Void) async throws {
    for attempt in 1...5 {
      do {
        try await body()
        return
      } catch let error as AdminError where error.status == 409 || error.status == 404 {
        return  // Already exists, or already deleted.
      } catch {
        if attempt == 5 { throw error }
        print("Admin request failed (attempt \(attempt)): \(error)")
        try? await Task.sleep(for: .seconds(attempt))
      }
    }
  }

  func create(_ resources: Resources) async throws {
    try await forEachConcurrently(resources.topics) { topic in
      try await withRetries {
        _ = try await request(
          "PUT", "projects/\(projectID)/topics/\(topic.name)", body: ["labels": topic.labels])
      }
    }
    try await forEachConcurrently(resources.subscriptions) { subscription in
      try await withRetries {
        _ = try await request(
          "PUT", "projects/\(projectID)/subscriptions/\(subscription.name)",
          body: [
            "topic": subscription.topic.id(projectID: projectID),
            "labels": subscription.labels,
            "ackDeadlineSeconds": resources.ackDeadline,
            "messageRetentionDuration": "\(resources.retention)s",
            "expirationPolicy": ["ttl": "86400s"],
          ])
      }
    }
  }

  /// Deletes the run's resources, ignoring ones that don't exist.
  func delete(_ resources: Resources) async {
    await deleteAll(
      resources.subscriptions.map { "projects/\(projectID)/subscriptions/\($0.name)" })
    await deleteAll(resources.topics.map { "projects/\(projectID)/topics/\($0.name)" })
  }

  /// Deletes every topic and subscription of any stress run in the project.
  func deleteAllStressResources() async throws -> (topics: [String], subscriptions: [String]) {
    let subscriptions = try await listSubscriptions()
    await deleteAll(subscriptions)
    let topics = try await listTopics()
    await deleteAll(topics)
    return (topics, subscriptions)
  }

  func listTopics() async throws -> [String] {
    try await list("topics")
  }

  func listSubscriptions() async throws -> [String] {
    try await list("subscriptions")
  }

  private func list(_ kind: String) async throws -> [String] {
    var names: [String] = []
    var pageToken = ""
    repeat {
      let response = try await request(
        "GET", "projects/\(projectID)/\(kind)?pageSize=1000&pageToken=\(pageToken)")
      let items = response.json[kind] as? [Any] ?? []
      names += items.compactMap { ($0 as? [String: Any])?["name"] as? String }
        .filter(Self.isStressResource)
      pageToken = response.json["nextPageToken"] as? String ?? ""
    } while !pageToken.isEmpty
    return names
  }

  private static func isStressResource(_ name: String) -> Bool {
    name.split(separator: "/").last?.hasPrefix(Config.namePrefix) == true
  }

  private func deleteAll(_ ids: [String]) async {
    try? await forEachConcurrently(ids) { id in
      do {
        try await withRetries { _ = try await request("DELETE", id) }
      } catch {
        print("Failed to delete \(id): \(error)")
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
