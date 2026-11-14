import Foundation
import GRPCCore
import GoogleCloudAuth
import Logging
import NIO
import ServiceLifecycle
import Tracing

public final class PullSubscriber<Handler: _Handler>: Service {

  private let logger: Logger

  private let handler: Handler
  let client: Google_Pubsub_V1_Subscriber.ClientProtocol
  let publisherClient: Google_Pubsub_V1_Publisher.ClientProtocol
  let pubSubService: PubSubService
  let ackBatcher: AckBatcher<ContinuousClock>

  public enum ConfigurationError: Error {
    case missingProjectID
  }

  public let projectID: String

  public let deleteSubscriptionOnShutdown: Bool

  public let options: PullSubscriberOptions

  public convenience init(
    handler: Handler,
    deleteSubscriptionOnShutdown: Bool = false,
    options: PullSubscriberOptions = PullSubscriberOptions()
  ) async throws {
    guard let projectID = await (ServiceContext.current ?? .topLevel).projectID else {
      throw ConfigurationError.missingProjectID
    }
    try self.init(
      handler: handler, projectID: projectID,
      deleteSubscriptionOnShutdown: deleteSubscriptionOnShutdown, options: options)
  }

  public convenience init(
    handler: Handler,
    projectID: String,
    deleteSubscriptionOnShutdown: Bool = false,
    options: PullSubscriberOptions = PullSubscriberOptions()
  ) throws {
    self.init(
      handler: handler,
      projectID: projectID,
      deleteSubscriptionOnShutdown: deleteSubscriptionOnShutdown,
      options: options,
      pubSubService: try PubSubService.shared
    )
  }

  init(
    handler: Handler,
    projectID: String,
    deleteSubscriptionOnShutdown: Bool = false,
    options: PullSubscriberOptions = PullSubscriberOptions(),
    pubSubService: PubSubService
  ) {
    let logger = Logger(label: "pubsub.subscriber." + handler.subscription.name)
    let client = Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
    self.projectID = projectID
    self.logger = logger
    self.handler = handler
    self.client = client
    self.publisherClient = Google_Pubsub_V1_Publisher.Client(wrapping: pubSubService.grpcClient)
    self.pubSubService = pubSubService
    self.ackBatcher = AckBatcher(
      transport: GRPCAckTransport(
        client: client, subscriptionID: handler.subscription.id(projectID: projectID)),
      logger: logger)
    self.deleteSubscriptionOnShutdown = deleteSubscriptionOnShutdown
    self.options = options
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

    await withDiscardingTaskGroup { group in
      group.addTask {
        await self.ackBatcher.run()
      }
      await cancelWhenGracefulShutdown {
        await self.continuesPull()
      }
      // Pulling only returns once all received messages are handled, so every ack has been
      // submitted by now and is sent before the gRPC client is allowed to shut down.
      ackBatcher.finish()
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
    pubSubServiceRun.cancel()
    try await pubSubServiceRun.value
  }

  private func continuesPull() async {
    var backoff = ExponentialBackoff()
    while !Task.isCancelled {
      do {
        try await singlePull()
        backoff.reset()
      } catch {
        if Task.isCancelled {
          break
        }

        if let error = error as? RPCError, error.code == .notFound {
          do {
            try await pubSubService.create(
              subscription: handler.subscription,
              subscriberClient: client,
              publisherClient: publisherClient,
              projectID: projectID
            )
          } catch {
            logger.error("Failed to create subscription: \(error)")
          }
        }

        let log:
          (
            @autoclosure () -> Logger.Message, @autoclosure () -> Logger.Metadata?, String, String,
            UInt
          ) -> Void
        switch error as? ChannelError {
        case .ioOnClosedChannel:
          log = logger.debug
        default:
          switch (error as? RPCError)?.code ?? .unknown {
          case .unavailable:
            #if DEBUG
              // Common issue with emulator that connections are closed with unavailable.
              // Log as trace to avoid flooding the logs.
              log = logger.trace
            #else
              log = logger.debug
            #endif
          case .deadlineExceeded:
            log = logger.debug
            // Idle pulls can end with deadline exceeded, so it must not escalate the backoff.
            backoff.reset()
          default:
            log = logger.warning
          }
        }
        let delay = backoff.next()

        log(
          "Pull failed for \(handler.subscription.name) (retry in \(delay)): \(error)",
          nil, #file, #function, #line)

        try? await Task.sleep(for: delay)
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
        try await ackBatcher.ack(AckID(message.ackID))
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
      try await ackBatcher.nack(AckID(message.ackID))
    } catch {
      logger.error("Failed to unacknowledge message: \(error)")
    }
  }
}
