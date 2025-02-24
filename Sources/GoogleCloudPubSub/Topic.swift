import Foundation
import GRPCCore
import GoogleCloudServiceContext
import ServiceContextModule

public struct Topics {}

public struct Topic<Message: _Message>: Sendable, Equatable, Hashable {

  public let name: String
  public let labels: [String: String]

  public init(name: String, labels: [String: String] = [:]) {
    self.name = name
    self.labels = labels
  }

  // MARK: - Identifiable

  public func id(projectID: String) -> String {
    "projects/\(projectID)/topics/\(name)"
  }

  // MARK: - Hashable

  public var rawValue: String {
    name
  }
}
