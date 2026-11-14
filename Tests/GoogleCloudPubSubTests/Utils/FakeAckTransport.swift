import Synchronization

@testable import GoogleCloudPubSub

final class FakeAckTransport: AckTransport {

  struct Call: Hashable, Sendable {

    enum Kind: Hashable, Sendable {
      case acknowledge
      case modify(AckDeadlineModification)
    }

    let kind: Kind
    let ids: [AckID]

    init(_ kind: Kind, _ ids: [AckID]) {
      self.kind = kind
      self.ids = ids
    }
  }

  struct NormalizedCall: Hashable {
    let kind: Call.Kind
    let ids: Set<AckID>

    init(_ kind: Call.Kind, _ ids: Set<String>) {
      self.kind = kind
      self.ids = Set(ids.map(AckID.init(rawValue:)))
    }
  }

  /// Decides the outcome of a call. `attempt` counts earlier identical calls.
  typealias Respond = @Sendable (_ call: Call, _ attempt: Int) async throws -> Void

  private let respond: Respond
  private let recorded = Mutex<[Call]>([])

  init(respond: @escaping Respond = { _, _ in }) {
    self.respond = respond
  }

  var calls: [Call] {
    recorded.withLock { $0 }
  }

  var normalizedCalls: Set<NormalizedCall> {
    Set(calls.map { NormalizedCall($0.kind, Set($0.ids.map(\.rawValue))) })
  }

  func ids(of kind: Call.Kind) -> [AckID] {
    calls.filter { $0.kind == kind }.flatMap(\.ids)
  }

  func acknowledge(_ ids: [AckID]) async throws {
    try await record(Call(.acknowledge, ids))
  }

  func modifyAckDeadline(_ ids: [AckID], to modification: AckDeadlineModification) async throws {
    try await record(Call(.modify(modification), ids))
  }

  private func record(_ call: Call) async throws {
    let attempt = recorded.withLock { calls in
      defer { calls.append(call) }
      return calls.count { $0 == call }
    }
    try await respond(call, attempt)
  }
}
