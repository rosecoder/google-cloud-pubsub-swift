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
