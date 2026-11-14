import GRPCCore
import Logging
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberLoggingTests {

  let connection = FakeStreamingPullConnection()
  let clock = TestClock()
  let logs = LogRecorder()

  @Test(.timeLimit(.minutes(1)))
  func shouldAttributeReceiverLogsToSubscription() async throws {
    try await withSubscriber(ackTransport: FakeAckTransport()) {
      try await waitUntil { connection.isReading }
      connection.end()
      try await waitUntil { entry(startingWith: "StreamingPull ended") != nil }
    }

    #expect(entry(startingWith: "StreamingPull ended")?.metadata["subscription"] == "logged")
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldLogFailedAcknowledgementAsError() async throws {
    let ackTransport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        throw RPCError(code: .permissionDenied, message: "")
      }
    }

    try await withSubscriber(ackTransport: ackTransport) {
      connection.deliver([FakeStreamingPullConnection.message(id: "a")])
      try await waitUntil { entry(startingWith: "Failed to acknowledge") != nil }
    }

    let entry = entry(startingWith: "Failed to acknowledge")
    #expect(entry?.level == .error)
    #expect(entry?.metadata["subscription"] == "logged")
  }

  // MARK: - Helpers

  private func entry(startingWith prefix: String) -> LogRecorder.Entry? {
    logs.entries.first { $0.message.hasPrefix(prefix) }
  }

  private func withSubscriber(
    ackTransport: FakeAckTransport, _ body: () async throws -> Void
  ) async throws {
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberLoggingTests")),
      subscriptionName: "logged"
    ) { _ in }
    let subscriber = PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-logging-tests",
      pubSubService: try PubSubService(),
      streamingPullConnection: connection,
      ackTransport: ackTransport,
      clock: clock,
      leaseGenerator: SplitMix64(seed: 1),
      logger: logs.logger(label: "test.pull-subscriber-logging"))

    let run = Task { await subscriber.receiveAndProcess() }
    let result = await Result { try await body() }
    run.cancel()
    await run.value
    try result.get()
  }
}
