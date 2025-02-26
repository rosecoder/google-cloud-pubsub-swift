import Logging

public func withDisposablePullSubscriber<Message: _Message>(
    subscription: Subscription<Message>,
    body: @Sendable @escaping (Message.Incoming) async throws -> Void
) async throws {

    let logger = Logger(label: "pubsub.disposable-subscriber." + subscription.name)

    let handler = await CallbackHandler(subscription: subscription, handler: body)
    let subscriber = try await PullSubscriber(handler: handler)

    let pubSubServiceRunTask = Task {
        try await subscriber.pubSubService.run()
    }

    logger.debug("Creating subscription...")

    try await subscriber.pubSubService.create(
        subscription: subscription,
        subscriberClient: subscriber.client,
        publisherClient: Publisher().client,
        projectID: subscriber.projectID
    )

    do {
        logger.debug("Running subscriber...")
        try await withThrowingDiscardingTaskGroup { group in
            group.addTask {
                try await subscriber.run()
            }
            group.addTask {
                try await pubSubServiceRunTask.value
            }
        }
    } catch {
        let wasCancelled = error is CancellationError
        logger.debug("Running is done, deleting subscription...")
        do {
            try await subscriber.pubSubService.delete(
                subscription: subscription,
                subscriberClient: subscriber.client,
                projectID: subscriber.projectID
            )
        } catch {
            if wasCancelled {
                throw error
            }
            // throw parent error instead
        }
        throw error
    }
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
