import Foundation
import GRPCCore
import GoogleCloudAuth
import Logging
import NIO
import RetryableTask
import ServiceLifecycle
import Tracing

public final class PullSubscriber<Handler: _Handler>: Service {

  private let logger: Logger

  private let handler: Handler
  let client: Google_Pubsub_V1_Subscriber.ClientProtocol
  let pubSubService: PubSubService

  public enum ConfigurationError: Error {
    case missingProjectID
  }

  public let projectID: String

  public let deleteSubscriptionOnShutdown: Bool

  public convenience init(handler: Handler, deleteSubscriptionOnShutdown: Bool = false) async throws
  {
    guard let projectID = await (ServiceContext.current ?? .topLevel).projectID else {
      throw ConfigurationError.missingProjectID
    }
    try self.init(
      handler: handler, projectID: projectID,
      deleteSubscriptionOnShutdown: deleteSubscriptionOnShutdown)
  }

  public init(handler: Handler, projectID: String, deleteSubscriptionOnShutdown: Bool = false)
    throws
  {
    let pubSubService = try PubSubService.shared
    self.projectID = projectID
    self.logger = Logger(label: "pubsub.subscriber." + handler.subscription.name)
    self.handler = handler
    self.client = Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
    self.pubSubService = pubSubService
    self.deleteSubscriptionOnShutdown = deleteSubscriptionOnShutdown
  }

  public func run() async throws {
    logger.debug("Subscribed to \(handler.subscription.name)")

    let pubSubServiceRun = Task {
      try await pubSubService.run()
    }

    let blockerTask: Task<Void, Never> = Task {
      while !Task.isCancelled {
        try? await Task.sleep(nanoseconds: .max / 2)
      }
    }
    await pubSubService.registerBlockerForGRPCShutdown(task: blockerTask)

    await cancelWhenGracefulShutdown {
      await self.continuesPull()
    }

    if deleteSubscriptionOnShutdown {
      logger.debug("Deleting subscription...")
      try await Task.detached {
        try await self.pubSubService.delete(
          subscription: self.handler.subscription,
          subscriberClient: self.client,
          projectID: self.projectID
        )
      }.value
      logger.debug("Deleted subscription.")
    }

    blockerTask.cancel()
    try await pubSubServiceRun.value
  }

  private func continuesPull() async {
    var retryCount: UInt64 = 0
    while !Task.isCancelled {
      do {
        try await singlePull()
        retryCount = 0
      } catch {
        if Task.isCancelled {
          break
        }

        if let error = error as? RPCError, error.code == .notFound {
          do {
            try await pubSubService.create(
              subscription: handler.subscription,
              subscriberClient: client,
              publisherClient: Publisher().client,
              projectID: projectID
            )
          } catch {
            logger.error("Failed to create subscription: \(error)")
          }
        }

        var delay: UInt64
        let log:
          (
            @autoclosure () -> Logger.Message, @autoclosure () -> Logger.Metadata?, String, String,
            UInt
          ) -> Void
        switch error as? ChannelError {
        case .ioOnClosedChannel:
          log = logger.debug
          delay = 50_000_000  // 50 ms
        default:
          switch (error as? RPCError)?.code ?? .unknown {
          case .unavailable:
            log = logger.debug
            delay = 200_000_000  // 200 ms
          case .deadlineExceeded:
            log = logger.debug
            delay = 50_000_000  // 50 ms
          default:
            log = logger.warning
            delay = 1_000_000_000  // 1 sec
          }
        }
        delay *= (retryCount + 1)

        log(
          "Pull failed for \(handler.subscription.name) (retry in \(delay / 1_000_000)ms): \(error)",
          nil, #file, #function, #line)

        try? await Task.sleep(nanoseconds: delay)

        retryCount += 1
      }
    }
  }

  // MARK: - Pull

  private func singlePull() async throws {
    var options = GRPCCore.CallOptions.defaults
    options.timeout = .seconds(3600)

    let response = try await client.pull(
      .with {
        $0.subscription = handler.subscription.id(projectID: projectID)
        $0.maxMessages = 1_000
      }, options: options)

    guard !response.receivedMessages.isEmpty else {
      return
    }

    await Task.detached {  // Run detached so we don't forward the cancellation. Let handling of messages continue.
      await withDiscardingTaskGroup { group in
        for message in response.receivedMessages {
          group.addTask {
            await self.handle(message: message)
          }
        }
      }
    }.value
  }

  private func handle(message: Google_Pubsub_V1_ReceivedMessage) async {
    await withSpan(handler.subscription.name, ofKind: .consumer) { span in
      span.attributes["message"] = message.message.messageID

      var logger = self.logger
      logger[metadataKey: "subscription"] = .string(handler.subscription.name)
      span.attributes["message"] = message.message.messageID
      logger.debug("Handling incoming message. Decoding...")

      let decodedMessage: Handler.Message.Incoming
      do {
        let rawMessage = message.message
        decodedMessage = try .init(
          id: rawMessage.messageID,
          published: rawMessage.publishTime.date,
          data: rawMessage.data,
          attributes: rawMessage.attributes,
          logger: &logger,
          span: span
        )
        try Task.checkCancellation()
      } catch {
        await handleHandler(error: error, message: message, logger: logger, span: span)
        return
      }

      span.addEvent("message-decoded")
      logger.debug("Handling incoming message. Running handler...")

      let context = HandlerContext(logger: logger, span: span)
      do {
        try await handler.handle(message: decodedMessage, context: context)
        logger = context.logger
      } catch {
        logger = context.logger
        await handleHandler(error: error, message: message, logger: logger, span: span)
        return
      }

      logger.debug("Handling successful.")
      span.setStatus(SpanStatus(code: .ok))

      do {
        try await acknowledge(
          id: message.ackID, subscriptionID: handler.subscription.id(projectID: projectID),
          logger: logger)
      } catch {
        logger.critical("Failed to acknowledge message: \(error)")
      }
    }
  }

  func handleHandler(
    error: Error, message: Google_Pubsub_V1_ReceivedMessage, logger: Logger, span: any Span
  ) async {
    if !(error is CancellationError) {
      logger.error("\(error)")
    }
    span.recordError(error)

    do {
      try await unacknowledge(
        id: message.ackID, subscriptionID: handler.subscription.id(projectID: projectID),
        logger: logger)
    } catch {
      logger.error("Failed to unacknowledge message: \(error)")
    }
  }

  // MARK: - Acknowledge

  private func acknowledge(id: String, subscriptionID: String, logger: Logger) async throws {
    try await withRetryableTask(logger: logger) {
      _ = try await client.acknowledge(
        Google_Pubsub_V1_AcknowledgeRequest.with {
          $0.subscription = subscriptionID
          $0.ackIds = [id]
        })
    }
  }

  private func unacknowledge(id: String, subscriptionID: String, logger: Logger) async throws {
    try await withRetryableTask(logger: logger) {
      _ = try await client.modifyAckDeadline(
        Google_Pubsub_V1_ModifyAckDeadlineRequest.with {
          $0.subscription = subscriptionID
          $0.ackIds = [id]
          $0.ackDeadlineSeconds = 0
        })
    }
  }
}
