import GRPCCore

@testable import GoogleCloudPubSub

/// Polls until a subscriber has created the subscription, after which published messages reach it.
func waitUntilSubscriptionExists(
  id: String,
  client: Google_Pubsub_V1_Subscriber.ClientProtocol
) async throws {
  while true {
    do {
      _ = try await client.getSubscription(.with { $0.subscription = id })
      return
    } catch let error as RPCError where error.code == .notFound {
      try await Task.sleep(for: .milliseconds(50))
    }
  }
}
