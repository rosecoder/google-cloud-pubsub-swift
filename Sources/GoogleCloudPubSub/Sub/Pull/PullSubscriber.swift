import Foundation
import GRPCCore
import GoogleCloudAuth
import Logging
import ServiceLifecycle
import Tracing

public final class PullSubscriber<Handler: _Handler>: Service {

  private let logger: Logger

  private let handler: Handler
  let client: Google_Pubsub_V1_Subscriber.ClientProtocol
  let publisherClient: Google_Pubsub_V1_Publisher.ClientProtocol
  let pubSubService: PubSubService
  let receiver: StreamingPullReceiver
  let ackBatcher: AckBatcher<ContinuousClock>
  private let now: @Sendable () -> ContinuousClock.Instant
  private let sleep: @Sendable (_ deadline: ContinuousClock.Instant) async throws -> Void
  private let runLeases:
    @Sendable (
      _ inventory: MessageInventory, _ logger: Logger,
      _ extendDeadlines: @escaping @Sendable (_ ids: [AckID], _ deadline: AckDeadline) async -> Void
    ) async -> Void

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

  convenience init(
    handler: Handler,
    projectID: String,
    deleteSubscriptionOnShutdown: Bool = false,
    options: PullSubscriberOptions = PullSubscriberOptions(),
    pubSubService: PubSubService,
    streamingPullConnection: (any StreamingPullConnection)? = nil,
    ackTransport: (any AckTransport)? = nil
  ) {
    self.init(
      handler: handler,
      projectID: projectID,
      deleteSubscriptionOnShutdown: deleteSubscriptionOnShutdown,
      options: options,
      pubSubService: pubSubService,
      streamingPullConnection: streamingPullConnection,
      ackTransport: ackTransport,
      clock: ContinuousClock(),
      leaseGenerator: SystemRandomNumberGenerator())
  }

  /// - Parameter clock: Times lease renewals, keepalives, reconnects and shutdown, and is the time
  ///   messages are received and released at. Acks and nacks are batched with a `ContinuousClock`
  ///   regardless.
  init<SubscriberClock: Clock>(
    handler: Handler,
    projectID: String,
    deleteSubscriptionOnShutdown: Bool = false,
    options: PullSubscriberOptions = PullSubscriberOptions(),
    pubSubService: PubSubService,
    streamingPullConnection: (any StreamingPullConnection)? = nil,
    ackTransport: (any AckTransport)? = nil,
    clock: SubscriberClock,
    leaseGenerator: some RandomNumberGenerator & Sendable
  )
  where
    SubscriberClock.Duration == Duration, SubscriberClock.Instant == ContinuousClock.Instant
  {
    let now: @Sendable () -> ContinuousClock.Instant = { clock.now }
    let sleep: @Sendable (ContinuousClock.Instant) async throws -> Void = {
      try await clock.sleep(until: $0, tolerance: nil)
    }
    self.now = now
    self.sleep = sleep
    self.runLeases = { inventory, logger, extendDeadlines in
      await LeaseManager(
        inventory: inventory,
        clock: clock,
        generator: leaseGenerator,
        logger: logger,
        extendDeadlines: extendDeadlines
      ).run()
    }
    let logger = Logger(label: "pubsub.subscriber." + handler.subscription.name)
    let client = Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient)
    let subscriptionID = handler.subscription.id(projectID: projectID)
    self.projectID = projectID
    self.logger = logger
    self.handler = handler
    let publisherClient = Google_Pubsub_V1_Publisher.Client(wrapping: pubSubService.grpcClient)
    self.client = client
    self.publisherClient = publisherClient
    self.pubSubService = pubSubService
    let withConnection: StreamingPullReceiver.WithConnection
    if let streamingPullConnection {
      withConnection = { body in try await body(streamingPullConnection) }
    } else {
      withConnection = { body in
        try await pubSubService.pool.withStreamSlot { client in
          try await body(Google_Pubsub_V1_Subscriber.Client(wrapping: client))
        }
      }
    }
    self.receiver = StreamingPullReceiver(
      withConnection: withConnection,
      subscriptionID: subscriptionID,
      clientID: UUID().uuidString,
      flowControl: options.flowControl,
      logger: logger,
      now: now,
      sleep: sleep,
      createSubscription: { [subscription = handler.subscription] in
        try await pubSubService.create(
          subscription: subscription,
          subscriberClient: client,
          publisherClient: publisherClient,
          projectID: projectID)
      })
    self.ackBatcher = AckBatcher(
      transport: ackTransport ?? GRPCAckTransport(client: client, subscriptionID: subscriptionID),
      logger: logger)
    self.deleteSubscriptionOnShutdown = deleteSubscriptionOnShutdown
    self.options = options
  }

  /// Receives and handles messages until graceful shutdown, then settles outstanding messages as
  /// configured by ``PullSubscriberOptions/shutdownBehavior``.
  ///
  /// When used in a `ServiceGroup`, its graceful shutdown timeout should be at least the
  /// `waitForProcessing` timeout plus 1 second.
  ///
  /// Cancelling the task instead stops everything right away without nacking: handlers are
  /// cancelled, unsent acks are dropped and outstanding messages are redelivered once their ack
  /// deadline expires.
  public func run() async throws {
    logger.debug("Subscribed to \(handler.subscription.name)")

    try await pubSubService.withClientInUse {
      await self.receiveAndProcess()
      try await self.deleteSubscriptionIfNeeded()
    }
  }

  private func deleteSubscriptionIfNeeded() async throws {
    guard deleteSubscriptionOnShutdown else {
      return
    }
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

  /// Receives messages until graceful shutdown and handles each in its own child task.
  ///
  /// Returns once every handler has returned and the remaining acks and nacks have been sent or
  /// the shutdown deadline has passed, so the gRPC client is not shut down before.
  func receiveAndProcess() async {
    let inventory = MessageInventory(
      flowControl: options.flowControl,
      lease: options.lease,
      initialAckDeadline: AckDeadline(.seconds(handler.subscription.acknowledgeDeadline)))

    // Handlers finish, queueing their acks and nacks, before leasing stops, and both before the
    // remaining acks are flushed.
    await withDiscardingTaskGroup { ackGroup in
      ackGroup.addTask {
        await self.ackBatcher.run()
      }
      let flushDeadline = await withDiscardingTaskGroup { leaseGroup in
        leaseGroup.addTask {
          await self.runLeases(inventory, self.logger) { [ackBatcher = self.ackBatcher] ids, deadline in
            await ackBatcher.modifyDeadlines(ids, to: deadline)
          }
        }
        let flushDeadline = await withDiscardingTaskGroup { handlerGroup in
          handlerGroup.addTask {
            await cancelWhenGracefulShutdown {
              await self.receiver.run(into: inventory, ackBatcher: self.ackBatcher)
            }
            let held = inventory.stopReceiving()
            if !Task.isCancelled {
              self.ackBatcher.enqueueNacks(held)
            }
          }
          for await item in inventory.dispatched {
            handlerGroup.addTask {
              await self.process(item, inventory: inventory)
            }
          }
          let settlement = await self.settle(inventory)
          if settlement.cancelsHandlers {
            handlerGroup.cancelAll()
          }
          return settlement.flushDeadline
        }
        leaseGroup.cancelAll()
        return flushDeadline
      }
      ackBatcher.finish(flushBy: .now.advanced(by: now().duration(to: flushDeadline)))
    }
  }

  // MARK: - Shutdown

  /// Time left after waiting for handlers to send the final acks and nacks, as in Node's client.
  static var finalFlushTimeout: Duration {
    .seconds(1)
  }

  private struct Settlement {
    let cancelsHandlers: Bool
    let flushDeadline: ContinuousClock.Instant
  }

  /// Settles the outstanding messages once receiving has stopped.
  private func settle(_ inventory: MessageInventory) async -> Settlement {
    let start = now()
    guard !Task.isCancelled else {
      _ = inventory.closeAll()
      return Settlement(cancelsHandlers: true, flushDeadline: start)
    }

    switch options.shutdownBehavior {
    case .nackImmediately:
      nackOutstanding(inventory)
      return Settlement(
        cancelsHandlers: true, flushDeadline: start.advanced(by: Self.finalFlushTimeout))

    case .waitForProcessing(let timeout):
      let timeout = min(timeout, options.lease.maxLeaseDuration)
      if timeout < Self.finalFlushTimeout {
        logger.warning(
          "The shutdown timeout \(timeout) leaves less than \(Self.finalFlushTimeout) to nack unfinished messages."
        )
      }
      let flushDeadline = start.advanced(by: timeout)
      let isEmpty = await waitUntilEmpty(
        inventory, until: flushDeadline.advanced(by: .zero - Self.finalFlushTimeout))
      if Task.isCancelled {
        _ = inventory.closeAll()
        return Settlement(cancelsHandlers: true, flushDeadline: now())
      }
      if isEmpty {
        return Settlement(cancelsHandlers: false, flushDeadline: flushDeadline)
      }
      nackOutstanding(inventory)
      return Settlement(cancelsHandlers: true, flushDeadline: flushDeadline)
    }
  }

  private func nackOutstanding(_ inventory: MessageInventory) {
    let ids = inventory.closeAll()
    if !ids.isEmpty {
      logger.warning("Nacking \(ids.count) messages still being handled at shutdown.")
    }
    ackBatcher.enqueueNacks(ids)
  }

  /// - Returns: Whether the inventory became empty before `deadline`.
  private func waitUntilEmpty(
    _ inventory: MessageInventory, until deadline: ContinuousClock.Instant
  ) async -> Bool {
    await withTaskGroup(of: Bool.self) { group in
      group.addTask {
        (try? await inventory.waitUntilEmpty()) != nil
      }
      group.addTask {
        try? await self.sleep(deadline)
        return false
      }
      defer { group.cancelAll() }
      return await group.next()!
    }
  }

  // MARK: - Handling

  private func process(_ item: MessageInventory.Item, inventory: MessageInventory) async {
    let (outcome, logger) = await handle(item.message)
    if let outcome {
      await respond(to: item, with: outcome, inventory: inventory, logger: logger)
    }
    inventory.release(item.ackID, now: now())
  }

  /// - Returns: How the message should be settled, or `nil` if its handler was cancelled by
  ///   shutdown, and the logger the handler left.
  private func handle(
    _ message: Google_Pubsub_V1_ReceivedMessage
  ) async -> (MessageInventory.Outcome?, Logger) {
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
        return (outcome(of: error, logger: logger, span: span), logger)
      }

      span.addEvent("message-decoded")
      logger.debug("Handling incoming message. Running handler...")

      let context = HandlerContext(logger: logger, span: span)
      do {
        try await handler.handle(message: decodedMessage, context: context)
      } catch {
        return (outcome(of: error, logger: context.logger, span: span), context.logger)
      }
      logger = context.logger

      logger.debug("Handling successful.")
      span.setStatus(SpanStatus(code: .ok))
      return (.ack, logger)
    }
  }

  func outcome(of error: Error, logger: Logger, span: any Span) -> MessageInventory.Outcome? {
    span.recordError(error)
    if error is CancellationError, Task.isCancelled {
      logger.debug("Handler cancelled by shutdown.")
      return nil
    }
    if !(error is CancellationError) {
      logger.error("\(error)")
    }
    return .nack
  }

  /// Queues the ack or nack of a message without waiting for it to be sent, so its capacity can
  /// be released right away. The ``AckBatcher`` logs failures.
  ///
  /// With exactly-once delivery an ack can fail, so this waits until it has been sent, and the
  /// message stays leased until then.
  private func respond(
    to item: MessageInventory.Item, with outcome: MessageInventory.Outcome,
    inventory: MessageInventory, logger: Logger
  ) async {
    guard inventory.claim(item.ackID, for: outcome) else {
      logger.debug("Dropped \(outcome) of message already nacked at shutdown.")
      return
    }
    if outcome == .ack {
      inventory.recordAckLatency(item.receivedAt.duration(to: now()))
    }
    let exactlyOnceDelivery = inventory.exactlyOnceDelivery
    switch outcome {
    case .ack where !exactlyOnceDelivery:
      ackBatcher.enqueueAcks(CollectionOfOne(item.ackID))
    case .nack where !exactlyOnceDelivery:
      ackBatcher.enqueueNacks(CollectionOfOne(item.ackID))
    case .ack:
      await waitForAck(of: item.ackID, logger: logger)
    case .nack:
      await waitForNack(of: item.ackID, logger: logger)
    }
  }

  private func waitForAck(of id: AckID, logger: Logger) async {
    do {
      try await ackBatcher.ack(id)
    } catch is CancellationError {
      logger.debug("Cancelled while waiting for acknowledgement, which is still sent.")
    } catch AckBatcherError.flushDeadlineExceeded {
      logger.warning("Acknowledgement not sent before the shutdown deadline.")
    } catch {
      logger.critical("Failed to acknowledge message: \(error)")
    }
  }

  private func waitForNack(of id: AckID, logger: Logger) async {
    do {
      try await ackBatcher.nack(id)
    } catch is CancellationError {
      logger.debug("Cancelled while waiting for unacknowledgement, which is still sent.")
    } catch AckBatcherError.flushDeadlineExceeded {
      logger.debug("Unacknowledgement not sent before the shutdown deadline.")
    } catch {
      logger.error("Failed to unacknowledge message: \(error)")
    }
  }
}
