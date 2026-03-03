import Logging
import ServiceContextModule

public func withDisposablePullSubscriber<Message: _Message>(
  subscription: Subscription<Message>,
  body: @Sendable @escaping (Message.Incoming) async throws -> Void
) async throws {
  guard let projectID = await (ServiceContext.current ?? .topLevel).projectID else {
    throw Publisher.ConfigurationError.missingProjectID
  }
  try await withDisposablePullSubscriber(
    subscription: subscription,
    pubSubService: PubSubService.shared,
    projectID: projectID,
    body: body
  )
}

func withDisposablePullSubscriber<Message: _Message>(
  subscription: Subscription<Message>,
  pubSubService: PubSubService,
  projectID: String,
  body: @Sendable @escaping (Message.Incoming) async throws -> Void
) async throws {
  let logger = Logger(label: "pubsub.disposable-subscriber." + subscription.name)

  let handler = await CallbackHandler(subscription: subscription, handler: body)
  let subscriber = PullSubscriber(
    handler: handler,
    projectID: projectID,
    deleteSubscriptionOnShutdown: true,
    pubSubService: pubSubService
  )

  let publisher = Publisher(
    projectID: projectID,
    pubSubService: pubSubService
  )

  logger.debug("Creating subscription...")
  try await subscriber.pubSubService.create(
    subscription: subscription,
    subscriberClient: subscriber.client,
    publisherClient: publisher.client,
    projectID: projectID
  )

  logger.debug("Running subscriber...")
  try await subscriber.run()
}

private final class CallbackHandler<Message: _Message>: Handler {

  let subscription: Subscription<Message>
  let handler: @Sendable (Incoming) async throws -> Void

  init(
    subscription: Subscription<Message>,
    handler: @Sendable @escaping (Incoming) async throws -> Void
  ) async {
    self.subscription = subscription
    self.handler = handler
  }

  func handle(message: Incoming, context: Context) async throws {
    try await handler(message)
  }
}
