import GRPCCore
import Logging
import NIOCore
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct StreamingPullReceiverTests {

  let connection = FakeStreamingPullConnection()
  let clock = TestClock()
  let subscriptionCreations = Counter()

  @Test(.timeLimit(.minutes(1)))
  func shouldOpenStreamWithInitialRequestRoutingHeaderAndTimeout() async throws {
    let flowControl = PullSubscriberOptions.FlowControl(
      maxOutstandingMessages: 5, maxOutstandingBytes: 1_000)
    try await withReceiver(flowControl: flowControl) { _ in
      try await waitUntilStreamIsOpen()
    }

    let stream = try #require(connection.streams.first)
    #expect(
      stream.requests == [
        .with {
          $0.subscription = Self.subscriptionID
          $0.streamAckDeadlineSeconds = 10
          $0.clientID = "client-1"
          $0.maxOutstandingMessages = 5
          $0.maxOutstandingBytes = 1_000
        }
      ])
    #expect(
      Array(stream.metadata[stringValues: "x-goog-request-params"])
        == ["subscription=\(Self.subscriptionID)"])
    #expect(stream.options.timeout == .seconds(900))
    #expect(stream.options.waitForReady == true)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldReconnectWithSameClientID() async throws {
    try await withReceiver { _ in
      try await waitUntilStreamIsOpen()
      _ = try await reconnect(after: nil)
    }

    let initialRequests = connection.streams.map(\.requests.first)
    #expect(initialRequests.count == 2)
    #expect(initialRequests[0]?.clientID == "client-1")
    #expect(initialRequests[0] == initialRequests[1])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldSendKeepAliveEvery30Seconds() async throws {
    try await withReceiver { _ in
      try await waitUntilStreamIsOpen()
      try await waitUntil { clock.sleeperCount == 1 }
      clock.advance(by: .seconds(29))
      await yieldRepeatedly()
      #expect(requests.count == 1)

      clock.advance(by: .seconds(1))
      try await waitUntil { requests.count == 2 && clock.sleeperCount == 1 }
      clock.advance(by: .seconds(30))
      try await waitUntil { requests.count == 3 }
    }

    #expect(requests.dropFirst() == [.init(), .init()])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldGrowBackoffUntilDataIsReceivedOrStreamEndsCleanly() async throws {
    let unavailable = RPCError(code: .unavailable, message: "")
    let ackTransport = FakeAckTransport()

    try await withReceiver(ackTransport: ackTransport) { _ in
      try await waitUntilStreamIsOpen()
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(100)))
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(200)))
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(400)))

      connection.deliver([FakeStreamingPullConnection.message(id: "a")])
      try await waitUntil { ackTransport.calls.count == 1 }
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(100)))
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(200)))
      #expect(
        try await reconnect(after: RPCError(code: .deadlineExceeded, message: ""))
          .isAbout(.milliseconds(100)))
      #expect(try await reconnect(after: unavailable).isAbout(.milliseconds(200)))
      #expect(try await reconnect(after: nil).isAbout(.milliseconds(100)))
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldCreateSubscriptionWhenNotFound() async throws {
    try await withReceiver { _ in
      try await waitUntilStreamIsOpen()
      _ = try await reconnect(after: RPCError(code: .notFound, message: ""))
    }

    #expect(subscriptionCreations.value == 1)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRetryPermissionErrorsAfterMaximumBackoff() async throws {
    try await withReceiver { _ in
      try await waitUntilStreamIsOpen()
      let delay = try await reconnect(after: RPCError(code: .permissionDenied, message: ""))
      #expect(delay == .seconds(60))
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldUpdateStreamAckDeadlineWhenExactlyOnceDeliveryChanges() async throws {
    try await withReceiver { inventory in
      try await waitUntilStreamIsOpen()
      connection.send(properties(exactlyOnceDelivery: true))
      try await waitUntil { requests.count == 2 }
      #expect(inventory.exactlyOnceDelivery)

      connection.send(properties(exactlyOnceDelivery: true))
      connection.send(properties(exactlyOnceDelivery: false))
      try await waitUntil { requests.count == 3 }
    }

    #expect(
      requests.dropFirst() == [
        .with { $0.streamAckDeadlineSeconds = 60 },
        .with { $0.streamAckDeadlineSeconds = 10 },
      ])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldOnlyLeaseMessagesWhoseReceiptIsConfirmedWithExactlyOnceDelivery() async throws {
    let ackTransport = FakeAckTransport { call, _ in
      if call.ids.contains(AckID("ack-rejected")) {
        throw RPCError(code: .failedPrecondition, message: "")
      }
    }

    try await withReceiver(ackTransport: ackTransport) { inventory in
      try await waitUntilStreamIsOpen()
      connection.send(properties(exactlyOnceDelivery: true))
      connection.deliver([FakeStreamingPullConnection.message(id: "a")])
      try await waitUntil { !inventory.isEmpty }
      connection.deliver([FakeStreamingPullConnection.message(id: "rejected")])
      try await waitUntil { ackTransport.calls.count == 2 && connection.isReading }

      let dispatched = inventory.dispatched.makeAsyncIterator()
      #expect(await dispatched.next()?.ackID == AckID("ack-a"))
      inventory.release(AckID("ack-a"))
      #expect(inventory.isEmpty)
    }

    #expect(
      ackTransport.calls == [
        .init(.modify(.extend(.seconds(60))), [AckID("ack-a")]),
        .init(.modify(.extend(.seconds(60))), [AckID("ack-rejected")]),
      ])
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStopWhenNoConnectionIsAvailableAfterShutdown() async throws {
    let inventory = MessageInventory(flowControl: .init())
    let receiver = makeReceiver(flowControl: .init()) { _ in
      throw RuntimeError(code: .clientIsStopped, message: "")
    }

    await receiver.run(
      into: inventory, ackBatcher: AckBatcher(transport: FakeAckTransport(), logger: logger))

    #expect(clock.sleeperCount == 0)
  }

  @Test func shouldRetryEveryErrorUnlessCancelled() {
    typealias Retry = StreamingPullRetry
    let cases: [(error: (any Error)?, retry: Retry)] = [
      (nil, Retry(delay: .initial, logLevel: .debug)),
      (CancellationError(), Retry(delay: .initial, logLevel: .debug)),
      (rpc(.deadlineExceeded), Retry(delay: .initial, logLevel: .debug)),
      (rpc(.unavailable), Retry(delay: .increasing, logLevel: .trace)),
      (rpc(.resourceExhausted), Retry(delay: .increasing, logLevel: .warning)),
      (rpc(.aborted), Retry(delay: .increasing, logLevel: .warning)),
      (rpc(.internalError), Retry(delay: .increasing, logLevel: .warning)),
      (rpc(.unknown), Retry(delay: .increasing, logLevel: .warning)),
      (rpc(.cancelled), Retry(delay: .increasing, logLevel: .warning)),
      (ChannelError.ioOnClosedChannel, Retry(delay: .increasing, logLevel: .debug)),
      (ChannelError.connectTimeout(.seconds(1)), Retry(delay: .increasing, logLevel: .warning)),
      (rpc(.notFound), Retry(delay: .increasing, createsSubscription: true, logLevel: .warning)),
      (rpc(.permissionDenied), Retry(delay: .maximum, logLevel: .error)),
      (rpc(.unauthenticated), Retry(delay: .maximum, logLevel: .error)),
      (rpc(.invalidArgument), Retry(delay: .maximum, logLevel: .error)),
      (rpc(.failedPrecondition), Retry(delay: .maximum, logLevel: .error)),
    ]

    #expect(
      StreamingPullRetryPolicy.retry(
        after: RuntimeError(code: .clientIsStopped, message: ""), isCancelled: false) == nil)
    for (error, retry) in cases {
      #expect(
        StreamingPullRetryPolicy.retry(after: error, isCancelled: false) == retry,
        "\(String(describing: error))")
      #expect(StreamingPullRetryPolicy.retry(after: error, isCancelled: true) == nil)
    }
  }

  // MARK: - Helpers

  private static let subscriptionID = "projects/p/subscriptions/s"

  private var requests: [Google_Pubsub_V1_StreamingPullRequest] {
    connection.streams.last?.requests ?? []
  }

  /// Ends the current stream and lets the receiver open the next one.
  ///
  /// - Returns: The backoff delay before reconnecting.
  private func reconnect(after error: (any Error)?) async throws -> Duration {
    let count = connection.streams.count
    connection.end(throwing: error)
    try await waitUntil { !connection.isStreaming && clock.sleeperCount == 1 }
    let delay = try #require(clock.nextWakeUp)
    clock.advance(by: delay)
    try await waitUntil { connection.streams.count == count + 1 }
    try await waitUntilStreamIsOpen()
    return delay
  }

  /// Waits until the stream reads responses and has written its initial request, which happen
  /// concurrently.
  private func waitUntilStreamIsOpen() async throws {
    try await waitUntil { connection.isReading && !requests.isEmpty }
  }

  private func properties(exactlyOnceDelivery: Bool) -> Google_Pubsub_V1_StreamingPullResponse {
    .with { $0.subscriptionProperties.exactlyOnceDeliveryEnabled = exactlyOnceDelivery }
  }

  private func rpc(_ code: RPCError.Code) -> RPCError {
    RPCError(code: code, message: "")
  }

  private func yieldRepeatedly() async {
    for _ in 0..<1_000 {
      await Task.yield()
    }
  }

  private func withReceiver(
    flowControl: PullSubscriberOptions.FlowControl = .init(),
    ackTransport: FakeAckTransport = FakeAckTransport(),
    _ body: (MessageInventory) async throws -> Void
  ) async throws {
    let inventory = MessageInventory(flowControl: flowControl, initialAckDeadline: .seconds(10))
    let ackBatcher = AckBatcher(transport: ackTransport, logger: logger)
    let receiver = makeReceiver(flowControl: flowControl) { [connection] body in
      try await body(connection)
    }

    let batcherRun = Task { await ackBatcher.run() }
    let run = Task { await receiver.run(into: inventory, ackBatcher: ackBatcher) }
    let result = await Result { try await body(inventory) }
    run.cancel()
    await run.value
    ackBatcher.finish()
    await batcherRun.value
    try result.get()
  }

  private var logger: Logger {
    Logger(label: "test.streaming-pull-receiver")
  }

  private func makeReceiver(
    flowControl: PullSubscriberOptions.FlowControl,
    withConnection: @escaping StreamingPullReceiver.WithConnection
  ) -> StreamingPullReceiver {
    StreamingPullReceiver(
      withConnection: withConnection,
      subscriptionID: Self.subscriptionID,
      clientID: "client-1",
      flowControl: flowControl,
      logger: logger,
      now: { [clock] in clock.now },
      sleep: { [clock] in try await clock.sleep(until: $0, tolerance: nil) },
      createSubscription: { [subscriptionCreations] in subscriptionCreations.increment() })
  }
}

final class Counter: Sendable {

  private let count = Mutex(0)

  var value: Int {
    count.withLock { $0 }
  }

  func increment() {
    count.withLock { $0 += 1 }
  }
}

extension Duration {

  /// Whether it is within the ±20% jitter of `expected`.
  fileprivate func isAbout(_ expected: Duration) -> Bool {
    (expected * 0.8...expected * 1.2).contains(self)
  }
}
