import Foundation
import GRPCCore
import GoogleCloudServiceContext
import Logging
import ServiceContextModule

public struct Subscriptions {}

public struct Subscription<Message: _Message>: Sendable, Equatable, Hashable {

  public let name: String
  public let topic: Topic<Message>

  public let labels: [String: String]

  public let retainAcknowledgedMessages: Bool
  public let acknowledgeDeadline: TimeInterval
  public let expirationPolicyDuration: TimeInterval
  public let messageRetentionDuration: TimeInterval

  public struct DeadLetterPolicy: Sendable, Equatable, Hashable {

    public let topic: Topic<Message>
    public let maxDeliveryAttempts: Int32

    public init(topic: Topic<Message>, maxDeliveryAttempts: Int32) {
      self.topic = topic
      self.maxDeliveryAttempts = maxDeliveryAttempts
    }
  }

  public let deadLetterPolicy: DeadLetterPolicy?

  public init(
    name: String,
    topic: Topic<Message>,
    labels: [String: String] = [:],
    retainAcknowledgedMessages: Bool = false,
    acknowledgeDeadline: TimeInterval = 10,
    expirationPolicyDuration: TimeInterval = 3600 * 24 * 31,
    messageRetentionDuration: TimeInterval = 3600 * 24 * 6,
    deadLetterPolicy: DeadLetterPolicy? = nil
  ) {
    self.name = name
    self.topic = topic
    self.labels = labels
    self.retainAcknowledgedMessages = retainAcknowledgedMessages
    self.acknowledgeDeadline = acknowledgeDeadline
    self.expirationPolicyDuration = expirationPolicyDuration
    self.messageRetentionDuration = messageRetentionDuration
    self.deadLetterPolicy = deadLetterPolicy
  }

  public func id(projectID: String) -> String {
    "projects/\(projectID)/subscriptions/\(name)"
  }

  // MARK: - Hashable

  public var rawValue: String {
    name
  }
}

#if DEBUG
  extension Subscription {

    func createIfNeeded(
      subscriberClient: Google_Pubsub_V1_Subscriber.ClientProtocol,
      publisherClient: Google_Pubsub_V1_Publisher.ClientProtocol,
      createTopicIfNeeded: Bool = true,
      projectID: String
    ) async throws {
      do {
        _ = try await subscriberClient.createSubscription(
          .with {
            $0.name = id(projectID: projectID)
            $0.labels = labels
            $0.topic = topic.id(projectID: projectID)
            $0.ackDeadlineSeconds = Int32(acknowledgeDeadline)
            $0.retainAckedMessages = retainAcknowledgedMessages
            $0.messageRetentionDuration = .with {
              $0.seconds = Int64(messageRetentionDuration)
            }
            $0.expirationPolicy = .with {
              $0.ttl = .with {
                $0.seconds = Int64(expirationPolicyDuration)
              }
            }
            if let deadLetterPolicy = deadLetterPolicy {
              $0.deadLetterPolicy = .with {
                $0.deadLetterTopic = deadLetterPolicy.topic.id(projectID: projectID)
                $0.maxDeliveryAttempts = deadLetterPolicy.maxDeliveryAttempts
              }
            }
          })
      } catch {
        switch (error as? RPCError)?.code {
        case .alreadyExists:
          break
        case .notFound:
          if !createTopicIfNeeded {
            throw error
          }
          try await topic.createIfNeeded(client: publisherClient, projectID: projectID)
          try await createIfNeeded(
            subscriberClient: subscriberClient,
            publisherClient: publisherClient,
            createTopicIfNeeded: false,
            projectID: projectID
          )
        default:
          throw error
        }
      }
    }
  }
#endif
