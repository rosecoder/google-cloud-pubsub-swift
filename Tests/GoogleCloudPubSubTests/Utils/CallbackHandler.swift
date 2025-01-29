import GoogleCloudPubSub

struct CallbackHandler<Message: GoogleCloudPubSub.Message>: Handler {

  let subscription: Subscription<Message>
  let callback: @Sendable (Incoming) async throws -> Void

  init(
    topic: Topic<Message>,
    callback: @escaping @Sendable (Incoming) async throws -> Void
  ) {
    self.subscription = Subscription(name: "test", topic: topic)
    self.callback = callback
  }

  func handle(message: Incoming, context: Context) async throws {
    try await callback(message)
  }
}
