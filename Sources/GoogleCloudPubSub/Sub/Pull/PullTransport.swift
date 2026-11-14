import GRPCCore

/// Receives messages from a single subscription.
protocol PullTransport: Sendable {

  func pull(maxMessages: Int) async throws -> [Google_Pubsub_V1_ReceivedMessage]
}

struct GRPCPullTransport: PullTransport {

  let client: Google_Pubsub_V1_Subscriber.ClientProtocol

  /// Fully qualified subscription name, `projects/{project}/subscriptions/{subscription}`.
  let subscriptionID: String

  func pull(maxMessages: Int) async throws -> [Google_Pubsub_V1_ReceivedMessage] {
    var options = GRPCCore.CallOptions.defaults
    options.timeout = .seconds(3600)

    return try await client.pull(
      .with {
        $0.subscription = subscriptionID
        $0.maxMessages = Int32(maxMessages)
      }, options: options
    ).receivedMessages
  }
}
