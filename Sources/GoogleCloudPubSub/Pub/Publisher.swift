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

  let client: Google_Pubsub_V1_Publisher.ClientProtocol
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

  public init(projectID: String, createTopicsIfNeeded: Bool = true) throws {
    let pubSubService = try PubSubService.shared
    self.projectID = projectID
    self.createTopicsIfNeeded = createTopicsIfNeeded
    self.client = Google_Pubsub_V1_Publisher.Client(wrapping: pubSubService.grpcClient)
    self.pubSubService = pubSubService
  }

  public func run() async throws {
    try await pubSubService.run()
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
    try await withRetryableTask(
      logger: logger,
      operation: {
        logger.debug("Publishing \(messages.count) message(s)...")

        let response: Google_Pubsub_V1_PublishResponse
        do {
          response = try await publish(to: topic, messages: messages)
        } catch let error as RPCError
          where error.code == .notFound && error.message == "Topic not found"
          && createTopicsIfNeeded
        {
          logger.info("Topic not found, creating it...")
          try await pubSubService.create(
            topic: topic,
            publisherClient: client,
            projectID: projectID
          )
          response = try await publish(to: topic, messages: messages, createTopicIfNeeded: false)
        }

        return response
          .messageIds
          .map { PublishedMessage(id: $0) }
      }, file: file, function: function, line: line)
  }

  private func publish<Message>(
    to topic: Topic<Message>,
    messages: [Message.Outgoing],
    createTopicIfNeeded: Bool = true
  ) async throws -> Google_Pubsub_V1_PublishResponse {
    try await withSpan(
      "pubsub-publish",
      ofKind: .producer
    ) { span in
      span.attributes["pubsub/topic"] = topic.name
      return try await client.publish(
        .with {
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
        })
    }
  }
}
