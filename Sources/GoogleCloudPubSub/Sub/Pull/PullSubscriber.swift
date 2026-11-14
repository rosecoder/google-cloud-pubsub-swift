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
  let pullTransport: any PullTransport
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
    pubSubService: PubSubService,
    pullTransport: (any PullTransport)? = nil,
    ackTransport: (any AckTransport)? = nil
  ) {
    let logger = Logger(label: "pubsub.subscriber." + handler.subscription.name)
    let client = Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
    let subscriptionID = handler.subscription.id(projectID: projectID)
    self.projectID = projectID
    self.logger = logger
    self.handler = handler
    self.client = client
    self.publisherClient = Google_Pubsub_V1_Publisher.Client(wrapping: pubSubService.grpcClient)
    self.pubSubService = pubSubService
    self.pullTransport =
      pullTransport ?? GRPCPullTransport(client: client, subscriptionID: subscriptionID)
    self.ackBatcher = AckBatcher(
      transport: ackTransport ?? GRPCAckTransport(client: client, subscriptionID: subscriptionID),
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

    await receiveAndProcess()

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

  /// Receives messages until graceful shutdown and handles each in its own child task.
  ///
  /// Returns once every received message has been handled and its ack or nack has been sent, so
  /// the gRPC client is not shut down before.
  func receiveAndProcess() async {
    let inventory = MessageInventory(flowControl: options.flowControl)

    await withDiscardingTaskGroup { ackGroup in
      ackGroup.addTask {
        await self.ackBatcher.run()
      }
      await withDiscardingTaskGroup { handlerGroup in
        handlerGroup.addTask {
          await cancelWhenGracefulShutdown {
            await self.receive(into: inventory)
          }
          self.ackBatcher.enqueueNacks(inventory.stopReceiving())
        }
        for await item in inventory.dispatched {
          handlerGroup.addTask {
            await self.process(item, inventory: inventory)
          }
        }
      }
      ackBatcher.finish()
    }
  }

  private func receive(into inventory: MessageInventory) async {
    var backoff = ExponentialBackoff()
    while true {
      let maxMessages: Int
      do {
        maxMessages = try await inventory.waitUntilReceivable()
      } catch {
        return
      }

      do {
        let messages = try await pullTransport.pull(maxMessages: maxMessages)
        ackBatcher.enqueueNacks(inventory.add(messages, now: .now))
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

  // MARK: - Handling

  private func process(_ item: MessageInventory.Item, inventory: MessageInventory) async {
    await handle(message: item.message)
    inventory.release(item.ackID)
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
      } catch is CancellationError {
        logger.debug("Cancelled while waiting for acknowledgement, which is still sent.")
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
    } catch is CancellationError {
      logger.debug("Cancelled while waiting for unacknowledgement, which is still sent.")
    } catch {
      logger.error("Failed to unacknowledge message: \(error)")
    }
  }
}
