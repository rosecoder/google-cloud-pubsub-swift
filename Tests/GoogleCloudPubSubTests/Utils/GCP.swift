import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import Logging
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

/// Configuration of tests against real Google Cloud Pub/Sub, which only run when
/// `PUBSUB_GCP_PROJECT_ID` is set and `PUBSUB_EMULATOR_HOST` is not.
enum GCP {

  static var projectID: String? {
    let environment = ProcessInfo.processInfo.environment
    guard environment["PUBSUB_EMULATOR_HOST"] == nil else {
      return nil
    }
    return environment["PUBSUB_GCP_PROJECT_ID"].flatMap { $0.isEmpty ? nil : $0 }
  }

  static var isRunningLongTests: Bool {
    ProcessInfo.processInfo.environment["PUBSUB_RUN_LONG_TESTS"] != nil
  }

  /// Shared by every resource of one test process, so leftovers of a run can be found by name.
  static let runID = String(UUID().uuidString.lowercased().prefix(8))

  /// Every topic and subscription created by these tests starts with this prefix.
  static let resourcePrefix = "pubsub-swift-test-"

  static func resourceName(_ label: String) -> String {
    "\(resourcePrefix)\(runID)-\(label)-\(UUID().uuidString.lowercased().prefix(8))"
  }

  static let logger = Logger(label: "pubsub.gcp-tests")
}

extension Trait where Self == ConditionTrait {

  static var enabledAgainstGCP: Self {
    .enabled(
      if: GCP.projectID != nil,
      "Requires PUBSUB_GCP_PROJECT_ID and no PUBSUB_EMULATOR_HOST")
  }

  static var enabledForLongTests: Self {
    .enabled(if: GCP.isRunningLongTests, "Only runs when PUBSUB_RUN_LONG_TESTS is set")
  }
}

/// Runs `body` with resources in the GCP project, deleting every topic and subscription it
/// created or tracked afterwards, also when `body` throws.
func withGCPResources<T: Sendable>(
  _ body: (GCPResources) async throws -> T
) async throws -> T {
  let projectID = try #require(GCP.projectID)
  let service = try PubSubService()
  let serviceRun = Task { try await service.run() }
  let resources = GCPResources(projectID: projectID, service: service)

  let result: Result<T, any Error>
  do {
    result = .success(try await body(resources))
  } catch {
    result = .failure(error)
  }
  // Unstructured so a test cancelled by its time limit still deletes its resources.
  await Task { await resources.deleteAll() }.value
  serviceRun.cancel()
  try? await serviceRun.value
  return try result.get()
}

/// Creates topics and subscriptions over its own connection, which stays usable while the
/// services under test shut down.
final class GCPResources: Sendable {

  let projectID: String
  let publisherClient: Google_Pubsub_V1_Publisher.Client<HTTP2ClientTransport.Posix>
  let subscriberClient: Google_Pubsub_V1_Subscriber.Client<HTTP2ClientTransport.Posix>
  private let topicIDs = Mutex<[String]>([])
  private let subscriptionIDs = Mutex<[String]>([])

  init(projectID: String, service: PubSubService) {
    self.projectID = projectID
    self.publisherClient = Google_Pubsub_V1_Publisher.Client(wrapping: service.grpcClient)
    self.subscriberClient = Google_Pubsub_V1_Subscriber.Client(wrapping: service.grpcClient)
  }

  /// Returns a topic that is deleted at the end of the test, creating it unless `create` is false.
  func topic<Message>(
    _ label: String, of _: Message.Type = Message.self, create: Bool = true
  ) async throws -> Topic<Message> {
    let topic = Topic<Message>(name: GCP.resourceName(label))
    let id = topic.id(projectID: projectID)
    topicIDs.withLock { $0.append(id) }
    if create {
      _ = try await publisherClient.createTopic(.with { $0.name = id })
    }
    return topic
  }

  /// Returns a subscription that is deleted at the end of the test, creating it unless `create`
  /// is false.
  func subscription<Message>(
    _ label: String,
    on topic: Topic<Message>,
    acknowledgeDeadline: Int32 = 10,
    create: Bool = true,
    configure: (inout Google_Pubsub_V1_Subscription) -> Void = { _ in }
  ) async throws -> Subscription<Message> {
    let subscription = Subscription(
      name: GCP.resourceName(label), topic: topic,
      acknowledgeDeadline: TimeInterval(acknowledgeDeadline),
      expirationPolicyDuration: 24 * 60 * 60, messageRetentionDuration: 10 * 60)
    let id = subscription.id(projectID: projectID)
    subscriptionIDs.withLock { $0.append(id) }
    guard create else {
      return subscription
    }
    _ = try await subscriberClient.createSubscription(
      .with {
        $0.name = id
        $0.topic = topic.id(projectID: projectID)
        $0.ackDeadlineSeconds = acknowledgeDeadline
        $0.messageRetentionDuration = .with { $0.seconds = 10 * 60 }
        $0.expirationPolicy = .with { $0.ttl = .with { $0.seconds = 24 * 60 * 60 } }
        configure(&$0)
      })
    return subscription
  }

  fileprivate func deleteAll() async {
    try? await forEachConcurrently(subscriptionIDs.withLock { $0 }) { id in
      await self.deleteIgnoringNotFound(id) {
        _ = try await self.subscriberClient.deleteSubscription(.with { $0.subscription = id })
      }
    }
    try? await forEachConcurrently(topicIDs.withLock { $0 }) { id in
      await self.deleteIgnoringNotFound(id) {
        _ = try await self.publisherClient.deleteTopic(.with { $0.topic = id })
      }
    }
  }

  private func deleteIgnoringNotFound(
    _ id: String, _ delete: @Sendable () async throws -> Void
  ) async {
    do {
      try await delete()
    } catch let error as RPCError where error.code == .notFound {
    } catch {
      Issue.record("Failed to delete \(id): \(error)")
    }
  }
}

/// Admin RPCs take seconds each, so many resources are created and deleted a batch at a time.
func forEachConcurrently<Element: Sendable>(
  _ elements: [Element],
  maxConcurrency: Int = 20,
  _ body: @escaping @Sendable (Element) async throws -> Void
) async throws {
  try await withThrowingTaskGroup(of: Void.self) { group in
    for (index, element) in elements.enumerated() {
      if index >= maxConcurrency {
        try await group.next()
      }
      group.addTask { try await body(element) }
    }
    try await group.waitForAll()
  }
}

/// Runs the subscribers and the publisher while `body` runs, then shuts them down gracefully.
///
/// The subscribers run as tasks rather than services of one `ServiceGroup`, whose graceful
/// shutdown hangs with more than one subscriber sharing a `PubSubService`.
func withRunningSubscribers<Handler: _Handler, T: Sendable>(
  _ subscribers: [PullSubscriber<Handler>],
  publishingWith publisher: Publisher,
  _ body: () async throws -> T
) async throws -> T {
  try await testGracefulShutdown { shutdownTrigger in
    let publisherRun = Task { try await publisher.run() }
    let subscriberRuns = subscribers.map { subscriber in
      Task { try await subscriber.run() }
    }
    let result: Result<T, any Error>
    do {
      result = .success(try await body())
    } catch {
      result = .failure(error)
    }
    shutdownTrigger.triggerGracefulShutdown()
    for run in subscriberRuns {
      try await run.value
    }
    publisherRun.cancel()
    try await publisherRun.value
    return try result.get()
  }
}

struct PollTimeoutError: Error, CustomStringConvertible {
  let description: String
}

/// Polls `condition` until it holds, without busy waiting as the waits are long.
func poll(
  timeout: Duration,
  _ description: String,
  _ condition: () -> Bool
) async throws {
  let deadline = ContinuousClock.now.advanced(by: timeout)
  while !condition() {
    guard ContinuousClock.now < deadline else {
      throw PollTimeoutError(description: "\(description) not met within \(timeout)")
    }
    try await Task.sleep(for: .milliseconds(100))
  }
}

/// Counts how often each message was delivered to a handler.
final class Deliveries: Sendable {

  private let counts = Mutex<[String: Int]>([:])
  private let firstDelivery = Mutex<[String: ContinuousClock.Instant]>([:])

  /// - Returns: How often the message has been delivered, including this delivery.
  @discardableResult
  func record(_ id: String) -> Int {
    let now = ContinuousClock.now
    firstDelivery.withLock { if $0[id] == nil { $0[id] = now } }
    return counts.withLock {
      $0[id, default: 0] += 1
      return $0[id]!
    }
  }

  var distinct: Int {
    counts.withLock { $0.count }
  }

  var total: Int {
    counts.withLock { $0.values.reduce(0, +) }
  }

  var redelivered: [String: Int] {
    counts.withLock { $0.filter { $0.value > 1 } }
  }

  func count(of id: String) -> Int {
    counts.withLock { $0[id, default: 0] }
  }

  func firstDelivered(_ id: String) -> ContinuousClock.Instant? {
    firstDelivery.withLock { $0[id] }
  }

  func ids() -> Set<String> {
    counts.withLock { Set($0.keys) }
  }
}

/// Opens StreamingPull streams on `base` and records how each of them ended.
final class RecordingStreamingPullConnection: StreamingPullConnection {

  struct Stream: Sendable, CustomStringConvertible {
    let opened: ContinuousClock.Instant
    let duration: Duration
    let code: RPCError.Code?
    let error: String?

    var description: String {
      "stream after \(duration): \(code.map { "\($0)" } ?? "-") \(error ?? "closed without error")"
    }
  }

  private let base: any StreamingPullConnection
  private let recorded = Mutex<[Stream]>([])

  init(base: any StreamingPullConnection) {
    self.base = base
  }

  var streams: [Stream] {
    recorded.withLock { $0 }
  }

  func streamingPull<Result: Sendable>(
    metadata: Metadata,
    options: CallOptions,
    requestProducer:
      @Sendable @escaping (RPCWriter<Google_Pubsub_V1_StreamingPullRequest>) async throws -> Void,
    onResponse:
      @Sendable @escaping (StreamingClientResponse<Google_Pubsub_V1_StreamingPullResponse>)
      async throws -> Result
  ) async throws -> Result {
    let opened = ContinuousClock.now
    do {
      let result = try await base.streamingPull(
        metadata: metadata, options: options, requestProducer: requestProducer,
        onResponse: onResponse)
      record(opened: opened, error: nil)
      return result
    } catch {
      record(opened: opened, error: error)
      throw error
    }
  }

  private func record(opened: ContinuousClock.Instant, error: (any Error)?) {
    let stream = Stream(
      opened: opened, duration: opened.duration(to: .now), code: (error as? RPCError)?.code,
      error: error.map { "\($0)" })
    GCP.logger.info("StreamingPull \(stream)")
    recorded.withLock { $0.append(stream) }
  }
}
