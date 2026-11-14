import Foundation
import Synchronization

@testable import GoogleCloudPubSub

/// Returns messages supplied with ``deliver(_:)``, at most as many as each pull requests, and
/// otherwise waits until messages are delivered or the pull is cancelled.
final class FakePullTransport: PullTransport {

  typealias Message = Google_Pubsub_V1_ReceivedMessage
  private typealias Continuation = CheckedContinuation<[Message], any Error>

  private struct State {
    var pending: [Message] = []
    var requestedCounts: [Int] = []
    var waiter: (maxMessages: Int, continuation: Continuation)?

    mutating func take(_ count: Int) -> [Message] {
      let taken = Array(pending.prefix(count))
      pending.removeFirst(taken.count)
      return taken
    }
  }

  private let state = Mutex(State())

  /// `maxMessages` of every pull, including one still waiting.
  var requestedCounts: [Int] {
    state.withLock { $0.requestedCounts }
  }

  var isWaiting: Bool {
    state.withLock { $0.waiter != nil }
  }

  func deliver(_ messages: [Message]) {
    let resumption = state.withLock { state -> (Continuation, [Message])? in
      state.pending.append(contentsOf: messages)
      guard let waiter = state.waiter else {
        return nil
      }
      state.waiter = nil
      return (waiter.continuation, state.take(waiter.maxMessages))
    }
    if let (continuation, messages) = resumption {
      continuation.resume(returning: messages)
    }
  }

  func pull(maxMessages: Int) async throws -> [Message] {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result = state.withLock { state -> Result<[Message], any Error>? in
          state.requestedCounts.append(maxMessages)
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if !state.pending.isEmpty {
            return .success(state.take(maxMessages))
          }
          state.waiter = (maxMessages, continuation)
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      state.withLock { state in
        defer { state.waiter = nil }
        return state.waiter?.continuation
      }?.resume(throwing: CancellationError())
    }
  }

  static func message(id: String, size: Int = 0) -> Message {
    .with {
      $0.ackID = "ack-\(id)"
      $0.message.messageID = id
      $0.message.data = Data(repeating: 0, count: size)
    }
  }
}
