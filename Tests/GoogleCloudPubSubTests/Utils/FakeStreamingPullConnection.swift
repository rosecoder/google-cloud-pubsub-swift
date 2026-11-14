import DequeModule
import Foundation
import GRPCCore
import Synchronization

@testable import GoogleCloudPubSub

/// Records every stream and replays the responses and stream endings queued with ``send(_:)``,
/// ``deliver(_:)`` and ``end(throwing:)`` in order, each to whichever stream reads next.
///
/// A response is only taken from the queue when the stream reads it, so ``pendingCount`` shows
/// what a subscriber has not read yet.
final class FakeStreamingPullConnection: StreamingPullConnection {

  typealias Request = Google_Pubsub_V1_StreamingPullRequest
  typealias Response = Google_Pubsub_V1_StreamingPullResponse
  typealias Message = Google_Pubsub_V1_ReceivedMessage
  typealias BodyPart = StreamingClientResponse<Response>.Contents.BodyPart

  struct Stream: Sendable {
    let metadata: Metadata
    let options: CallOptions
    fileprivate(set) var requests: [Request] = []
  }

  fileprivate enum Event: Sendable {
    case response(Response)
    case end((any Error)?)
  }

  private struct State {
    var streams: [Stream] = []
    var isStreaming = false
    var events: Deque<Event> = []
    var reader: CheckedContinuation<Event, any Error>?
  }

  private let state = Mutex(State())

  var streams: [Stream] {
    state.withLock { $0.streams }
  }

  /// Whether a stream is open.
  var isStreaming: Bool {
    state.withLock { $0.isStreaming }
  }

  /// Whether a stream is waiting for a response.
  var isReading: Bool {
    state.withLock { $0.reader != nil }
  }

  /// Number of queued responses and stream endings not yet read.
  var pendingCount: Int {
    state.withLock { $0.events.count }
  }

  func deliver(_ messages: [Message]) {
    send(.with { $0.receivedMessages = messages })
  }

  func send(_ response: Response) {
    enqueue(.response(response))
  }

  /// Ends a stream, normally if `error` is `nil`.
  func end(throwing error: (any Error)? = nil) {
    enqueue(.end(error))
  }

  private func enqueue(_ event: Event) {
    state.withLock { state -> CheckedContinuation<Event, any Error>? in
      guard let reader = state.reader else {
        state.events.append(event)
        return nil
      }
      state.reader = nil
      return reader
    }?.resume(returning: event)
  }

  func streamingPull<Result: Sendable>(
    metadata: Metadata,
    options: CallOptions,
    requestProducer: @Sendable @escaping (RPCWriter<Request>) async throws -> Void,
    onResponse: @Sendable @escaping (StreamingClientResponse<Response>) async throws -> Result
  ) async throws -> Result {
    let index = state.withLock { state in
      state.streams.append(Stream(metadata: metadata, options: options))
      state.isStreaming = true
      return state.streams.count - 1
    }
    defer { state.withLock { $0.isStreaming = false } }

    return try await withTaskGroup(of: Void.self) { group in
      group.addTask {
        try? await requestProducer(RPCWriter(wrapping: Writer(connection: self, index: index)))
      }
      let response = StreamingClientResponse<Response>(
        metadata: [:], bodyParts: RPCAsyncSequence(wrapping: BodyParts(connection: self)))
      let result = await Swift.Result { try await onResponse(response) }
      group.cancelAll()
      return result
    }.get()
  }

  fileprivate func record(_ request: Request, index: Int) {
    state.withLock { $0.streams[index].requests.append(request) }
  }

  fileprivate func read() async throws -> Event {
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result = state.withLock { state -> Swift.Result<Event, any Error>? in
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if let event = state.events.popFirst() {
            return .success(event)
          }
          state.reader = continuation
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      state.withLock { state in
        defer { state.reader = nil }
        return state.reader
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

private struct Writer: RPCWriterProtocol {

  let connection: FakeStreamingPullConnection
  let index: Int

  func write(_ element: FakeStreamingPullConnection.Request) async throws {
    connection.record(element, index: index)
  }

  func write(contentsOf elements: some Sequence<FakeStreamingPullConnection.Request>) async throws {
    for element in elements {
      try await write(element)
    }
  }
}

private struct BodyParts: AsyncSequence, Sendable {

  typealias Element = FakeStreamingPullConnection.BodyPart

  let connection: FakeStreamingPullConnection

  func makeAsyncIterator() -> Iterator {
    Iterator(connection: connection)
  }

  struct Iterator: AsyncIteratorProtocol {

    let connection: FakeStreamingPullConnection
    var hasEnded = false

    mutating func next() async throws -> Element? {
      guard !hasEnded else {
        return nil
      }
      switch try await connection.read() {
      case .response(let response):
        return .message(response)
      case .end(let error):
        hasEnded = true
        if let error {
          throw error
        }
        return nil
      }
    }
  }
}
