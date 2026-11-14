import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import Logging
import NIO
import RetryableTask
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

  public convenience init(projectID: String, createTopicsIfNeeded: Bool = true) throws {
    self.init(
      projectID: projectID,
      createTopicsIfNeeded: createTopicsIfNeeded,
      pubSubService: try PubSubService.shared
    )
  }

  init(projectID: String, createTopicsIfNeeded: Bool = true, pubSubService: PubSubService) {
    self.projectID = projectID
    self.createTopicsIfNeeded = createTopicsIfNeeded
    self.pubSubService = pubSubService
  }

  /// Keeps the client running until graceful shutdown or cancellation. Subscribers sharing the
  /// client keep it running for publishing from their handlers after this has returned.
  ///
  /// Publishing while neither this nor anything else sharing the client runs starts the client
  /// for each call.
  public func run() async throws {
    do {
      try await pubSubService.withClientInUse { _ in
        try? await gracefulShutdown()
      }
    } catch is CancellationError {}
  }

  // MARK: - Publish

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
      try await withRetryableTask(
        logger: logger,
        operation: {
          logger.debug("Publishing \(messages.count) message(s)...")

          let response: Google_Pubsub_V1_PublishResponse
          do {
            response = try await publish(to: topic, messages: messages, client: clients.publisher)
          } catch let error as RPCError
            where error.code == .notFound && error.message == "Topic not found"
            && createTopicsIfNeeded
          {
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
        }, file: file, function: function, line: line)
    }
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
