import GRPCCore

/// Sends acknowledgements and ack deadline modifications for a single subscription.
protocol AckTransport: Sendable {

  func acknowledge(_ ids: [AckID]) async throws

  func modifyAckDeadline(_ ids: [AckID], to modification: AckDeadlineModification) async throws
}

/// A change of the ack deadline of received messages.
enum AckDeadlineModification: Hashable, Sendable, CustomStringConvertible {

  /// Make the messages available for redelivery right away.
  case nack

  /// Extend the deadline to the given duration from now.
  case extend(AckDeadline)

  /// Value of `ackDeadlineSeconds` in Pub/Sub requests.
  var seconds: Int32 {
    switch self {
    case .nack: 0
    case .extend(let deadline): deadline.seconds
    }
  }

  var description: String {
    switch self {
    case .nack: "nack"
    case .extend(let deadline): "extend(\(deadline))"
    }
  }
}

struct GRPCAckTransport: AckTransport {

  let client: Google_Pubsub_V1_Subscriber.ClientProtocol

  /// Fully qualified subscription name, `projects/{project}/subscriptions/{subscription}`.
  let subscriptionID: String

  func acknowledge(_ ids: [AckID]) async throws {
    _ = try await client.acknowledge(
      .with {
        $0.subscription = subscriptionID
        $0.ackIds = ids.map(\.rawValue)
      })
  }

  func modifyAckDeadline(_ ids: [AckID], to modification: AckDeadlineModification) async throws {
    _ = try await client.modifyAckDeadline(
      .with {
        $0.subscription = subscriptionID
        $0.ackIds = ids.map(\.rawValue)
        $0.ackDeadlineSeconds = modification.seconds
      })
  }
}
