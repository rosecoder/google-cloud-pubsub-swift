import Foundation
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberFlowControlTests {

  let pullTransport = FakePullTransport()
  let handlerGate = Gate()
  let recorder = HandlerRecorder()

  @Test(.timeLimit(.minutes(1)))
  func shouldHandleMessagesConcurrently() async throws {
    let ackTransport = FakeAckTransport()
    pullTransport.deliver([message("a"), message("b")])

    try await withSubscriber(ackTransport: ackTransport) {
      try await waitUntil { recorder.started.count == 2 }
      #expect(recorder.peakConcurrency == 2)

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 2 }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotStartMoreHandlersThanMaxOutstandingMessagesUntilAcked() async throws {
    let ackGate = Gate()
    let ackTransport = FakeAckTransport { _, _ in await ackGate.wait() }
    handlerGate.open()
    pullTransport.deliver([message("a"), message("b"), message("c")])

    try await withSubscriber(
      ackTransport: ackTransport, flowControl: .init(maxOutstandingMessages: 2)
    ) {
      try await waitUntil { ackTransport.calls.count == 1 }
      #expect(Set(recorder.started) == ["a", "b"])
      #expect(pullTransport.requestedCounts == [2])

      ackGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 3 }
      #expect(recorder.started.sorted() == ["a", "b", "c"])
      #expect(recorder.peakConcurrency <= 2)
      #expect(pullTransport.requestedCounts.allSatisfy { $0 <= 2 })
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNackMessageWhenHandlerThrows() async throws {
    let ackTransport = FakeAckTransport()
    handlerGate.open()
    pullTransport.deliver([message("good"), message("fail")])

    try await withSubscriber(ackTransport: ackTransport) {
      try await waitUntil {
        ackTransport.ids(of: .acknowledge) == [AckID("ack-good")]
          && ackTransport.ids(of: .modify(.nack)) == [AckID("ack-fail")]
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldHoldMessagesOverByteLimitWithoutNacking() async throws {
    let ackTransport = FakeAckTransport()
    pullTransport.deliver([message("a", size: 8), message("b", size: 8), message("c", size: 8)])

    try await withSubscriber(
      ackTransport: ackTransport,
      flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 10)
    ) {
      try await waitUntil { recorder.started.count == 2 }
      #expect(pullTransport.requestedCounts == [10])

      handlerGate.open()
      try await waitUntil { ackTransport.ids(of: .acknowledge).count == 3 }
      #expect(recorder.started.sorted() == ["a", "b", "c"])
      #expect(recorder.peakConcurrency == 2)
      #expect(ackTransport.ids(of: .modify(.nack)).isEmpty)
    }
  }

  // MARK: - Helpers

  private func withSubscriber(
    ackTransport: FakeAckTransport,
    flowControl: PullSubscriberOptions.FlowControl = .init(),
    _ body: () async throws -> Void
  ) async throws {
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberFlowControlTests"))
    ) { [recorder, handlerGate] message in
      try await recorder.record(message.id) {
        await handlerGate.wait()
        if message.id == "fail" {
          throw HandlerError()
        }
      }
    }
    let subscriber = PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-flow-control-tests",
      options: PullSubscriberOptions(flowControl: flowControl),
      pubSubService: try PubSubService(),
      pullTransport: pullTransport,
      ackTransport: ackTransport
    )

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { await subscriber.receiveAndProcess() }
      do {
        try await body()
      } catch {
        handlerGate.open()
        shutdownTrigger.triggerGracefulShutdown()
        await run.value
        throw error
      }
      shutdownTrigger.triggerGracefulShutdown()
      await run.value
    }
  }

  private func message(_ id: String, size: Int = 0) -> Google_Pubsub_V1_ReceivedMessage {
    FakePullTransport.message(id: id, size: size)
  }
}

private struct HandlerError: Error {}

final class HandlerRecorder: Sendable {

  private struct State {
    var started: [String] = []
    var running = 0
    var peakConcurrency = 0
  }

  private let state = Mutex(State())

  var started: [String] {
    state.withLock { $0.started }
  }

  var peakConcurrency: Int {
    state.withLock { $0.peakConcurrency }
  }

  func record(_ id: String, _ body: () async throws -> Void) async rethrows {
    state.withLock { state in
      state.started.append(id)
      state.running += 1
      state.peakConcurrency = max(state.peakConcurrency, state.running)
    }
    defer {
      state.withLock { $0.running -= 1 }
    }
    try await body()
  }
}
