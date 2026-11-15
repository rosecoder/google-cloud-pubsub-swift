import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import Logging
import NIO
import ServiceLifecycle
import SwiftProtobuf
import Tracing

public final class Publisher: PublisherProtocol, Service {

  private let logger = Logger(label: "pubsub.publisher")

  private let pubSubService: PubSubService

  public enum ConfigurationError: Error {
    case missingProjectID
  }

  public let projectID: String

  /// If true, topics will be created if they don't exist.
  ///
  /// Note: A topic is first created after a publish fails
  public let createTopicsIfNeeded: Bool

  public convenience init(createTopicsIfNeeded: Bool = true) async throws {
    guard let projectID = await (ServiceContext.current ?? .topLevel).projectID else {
      throw ConfigurationError.missingProjectID
    }
    try self.init(projectID: projectID, createTopicsIfNeeded: createTopicsIfNeeded)
  }

  let retryPolicy: RetryPolicy

  public convenience init(projectID: String, createTopicsIfNeeded: Bool = true) throws {
    self.init(
      projectID: projectID,
      createTopicsIfNeeded: createTopicsIfNeeded,
      pubSubService: try PubSubService.shared
    )
  }

  init(
    projectID: String,
    createTopicsIfNeeded: Bool = true,
    retryPolicy: RetryPolicy = RetryPolicy(),
    pubSubService: PubSubService
  ) {
    self.projectID = projectID
    self.createTopicsIfNeeded = createTopicsIfNeeded
    self.retryPolicy = retryPolicy
    self.pubSubService = pubSubService
  }

  /// Keeps the client running until graceful shutdown or cancellation. Subscribers sharing the
  /// client keep it running for publishing from their handlers after this has returned.
  ///
  /// Publishing while neither this nor anything else sharing the client runs starts the client,
  /// which keeps running for 30 seconds after the last publish.
  public func run() async throws {
    do {
      try await pubSubService.withClientInUse(.service) { _ in
        try? await gracefulShutdown()
      }
    } catch is CancellationError {}
  }

  // MARK: - Publish

  /// Transient failures are retried as described by ``RetryPolicy``, so a call may take up to
  /// ``RetryPolicy/totalTimeout`` plus one call timeout. A call that failed may still have
  /// published its messages, and retries may publish them more than once.
  @discardableResult
  public func publish<Message>(
    to topic: Topic<Message>,
    messages: [Message.Outgoing],
    file: String = #fileID,
    function: String = #function,
    line: UInt = #line
  ) async throws -> [PublishedMessage] {
    guard !messages.isEmpty else {
      return []
    }

    return try await pubSubService.withClientInUse { clients in
      try await Self.withRetries(
        policy: retryPolicy, clock: .continuous, logger: logger,
        file: file, function: function, line: line
      ) {
        logger.debug("Publishing \(messages.count) message(s)...")

        let response: Google_Pubsub_V1_PublishResponse
        do {
          response = try await publish(to: topic, messages: messages, client: clients.publisher)
        } catch let error as RPCError where createTopicsIfNeeded && Self.isMissingTopic(error) {
          logger.info("Topic not found, creating it...")
          try await pubSubService.create(
            topic: topic,
            publisherClient: clients.publisher,
            projectID: projectID
          )
          response = try await publish(
            to: topic, messages: messages, client: clients.publisher, createTopicIfNeeded: false)
        }

        return response
          .messageIds
          .map { PublishedMessage(id: $0) }
      }
    }
  }

  /// Runs `operation` until it succeeds, fails in a way that isn't retried, or the policy's time
  /// is up. Only the final failure is logged above debug level.
  static func withRetries<C: Clock, T>(
    policy: RetryPolicy,
    clock: C,
    logger: Logger,
    file: String = #fileID,
    function: String = #function,
    line: UInt = #line,
    operation: () async throws -> T
  ) async throws -> T where C.Duration == Duration {
    var backoff = policy.backoff
    let giveUpAt = clock.now.advanced(by: policy.totalTimeout)
    var attempt = 1
    while true {
      do {
        return try await operation()
      } catch {
        let failure = PublishFailure(error, isSenderCancelled: Task.isCancelled)
        guard failure != .cancelled else {
          throw error
        }
        let delay = backoff.next()
        guard failure.isRetryable, clock.now.advanced(by: delay) <= giveUpAt else {
          logger.warning(
            "Publishing failed after \(attempt) attempt(s): \(error)",
            file: file, function: function, line: line)
          throw error
        }
        logger.debug(
          "Publish attempt \(attempt) failed, retrying in \(delay): \(error)",
          file: file, function: function, line: line)
        try await clock.sleep(for: delay)
        attempt += 1
      }
    }
  }

  /// Only the code is stable: the emulator says "Topic not found", while Pub/Sub says
  /// "Resource not found (resource=…)".
  static func isMissingTopic(_ error: RPCError) -> Bool {
    error.code == .notFound
  }

  private func publish<Message>(
    to topic: Topic<Message>,
    messages: [Message.Outgoing],
    client: some Google_Pubsub_V1_Publisher.ClientProtocol,
    createTopicIfNeeded: Bool = true
  ) async throws -> Google_Pubsub_V1_PublishResponse {
    try await withSpan(
      "pubsub-publish",
      ofKind: .producer
    ) { span in
      span.attributes["pubsub/topic"] = topic.name
      let request = Google_Pubsub_V1_PublishRequest.with {
        $0.topic = topic.id(projectID: projectID)
        $0.messages = messages.map { message in
          Google_Pubsub_V1_PubsubMessage.with {
            $0.data = message.data
            $0.attributes = message.attributes
            //                            if let trace = context.trace {
            //                                $0.attributes["__traceID"] = trace.id.stringValue
            //                                $0.attributes["__spanID"] = (trace.rootSpan?.id ?? trace.spanID).stringValue
            //                            }
          }
        }
      }
      return try await client.publish(request)
    }
  }
}
