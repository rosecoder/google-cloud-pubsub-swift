import GRPCCore
import Logging
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.timeLimit(.minutes(1)))
struct AckBatcherTests {

  typealias Configuration = AckBatcher<TestClock>.Configuration

  let clock = TestClock()

  // MARK: - Batching

  @Test func shouldCoalesceRequestsIntoOneRPC() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      async let a: Void = batcher.ack(AckID("a"))
      async let b: Void = batcher.ack(AckID("b"))
      async let c: Void = batcher.ack(AckID("c"))
      try await waitUntil { batcher.queuedCount == 3 }

      clock.advance(by: .milliseconds(100))
      _ = try await (a, b, c)

      #expect(transport.normalizedCalls == [.init(.acknowledge, ["a", "b", "c"])])
    }
  }

  @Test func shouldFlushOnlyWhenMaxDelayHasPassedSinceFirstRequest() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      async let a: Void = batcher.ack(AckID("a"))
      try await waitUntil { clock.sleeperCount == 1 }

      clock.advance(by: .milliseconds(99))
      #expect(clock.sleeperCount == 1)
      #expect(transport.calls.isEmpty)

      clock.advance(by: .milliseconds(1))
      try await a

      #expect(transport.calls == [.init(.acknowledge, [AckID("a")])])
    }
  }

  @Test func shouldFlushRightAwayWhenBatchSizeIsReached() async throws {
    let transport = FakeAckTransport()
    var configuration = Configuration.testing
    configuration.maxBatchSize = 3

    try await withBatcher(transport: transport, configuration: configuration) { batcher in
      let tasks = (0..<4).map { index in
        Task { try await batcher.ack(AckID("id-\(index)")) }
      }
      try await waitUntil { transport.calls.count == 1 && batcher.queuedCount == 1 }
      #expect(transport.calls.map(\.ids.count) == [3])

      clock.advance(by: .milliseconds(100))
      for task in tasks {
        try await task.cancellableValue
      }

      #expect(transport.calls.map(\.ids.count) == [3, 1])
    }
  }

  @Test func shouldSplitRequestsToStayWithinMaxRequestBytes() async throws {
    let transport = FakeAckTransport()
    let ids = (0..<7).map { AckID("id-\($0)") }
    var configuration = Configuration.testing
    configuration.maxRequestBytes = 3 * ids[0].encodedSize

    try await withBatcher(transport: transport, configuration: configuration) { batcher in
      let tasks = ids.map { id in
        Task { try await batcher.ack(id) }
      }
      try await waitUntil { transport.calls.count == 2 && batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))
      for task in tasks {
        try await task.cancellableValue
      }

      #expect(transport.calls.map(\.ids.count) == [3, 3, 1])
      #expect(Set(transport.calls.flatMap(\.ids)) == Set(ids))
      for call in transport.calls {
        #expect(call.ids.reduce(0) { $0 + $1.encodedSize } <= configuration.maxRequestBytes)
      }
    }
  }

  @Test func shouldGroupDeadlineModificationsByDeadline() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      async let a: Void = batcher.ack(AckID("a"))
      async let b: Void = batcher.nack(AckID("b"))
      async let c: Void = batcher.nack(AckID("c"))
      async let d: Void = batcher.modifyDeadline(AckID("d"), to: .seconds(30))
      async let e: Void = batcher.modifyDeadline(AckID("e"), to: .seconds(30))
      async let f: Void = batcher.modifyDeadline(AckID("f"), to: .seconds(60))
      try await waitUntil { batcher.queuedCount == 6 }

      clock.advance(by: .milliseconds(100))
      _ = try await (a, b, c, d, e, f)

      #expect(transport.calls.count == 4)
      #expect(
        transport.normalizedCalls == [
          .init(.acknowledge, ["a"]),
          .init(.modify(.nack), ["b", "c"]),
          .init(.modify(.extend(.seconds(30))), ["d", "e"]),
          .init(.modify(.extend(.seconds(60))), ["f"]),
        ])
    }
  }

  @Test func shouldSendEachAckIDOnlyOnceWithItsStrongestRequestInABatch() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      async let a1: Void = batcher.modifyDeadline(AckID("a"), to: .seconds(30))
      async let a2: Void = batcher.ack(AckID("a"))
      async let a3: Void = batcher.nack(AckID("a"))
      async let b1: Void = batcher.modifyDeadline(AckID("b"), to: .seconds(30))
      async let b2: Void = batcher.nack(AckID("b"))
      async let c1: Void = batcher.modifyDeadline(AckID("c"), to: .seconds(60))
      async let c2: Void = batcher.modifyDeadline(AckID("c"), to: .seconds(30))
      async let c3: Void = batcher.modifyDeadline(AckID("c"), to: .seconds(60))
      try await waitUntil { batcher.queuedCount == 8 }

      clock.advance(by: .milliseconds(100))
      _ = try await (a1, a2, a3, b1, b2, c1, c2, c3)

      #expect(transport.calls.map(\.ids.count) == [1, 1, 1])
      #expect(
        transport.normalizedCalls == [
          .init(.acknowledge, ["a"]),
          .init(.modify(.nack), ["b"]),
          .init(.modify(.extend(.seconds(60))), ["c"]),
        ])
      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldNotDeduplicateAcrossBatches() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      async let extend: Void = batcher.modifyDeadline(AckID("a"), to: .seconds(30))
      try await waitUntil { batcher.queuedCount == 1 }
      clock.advance(by: .milliseconds(100))
      try await extend

      async let ack: Void = batcher.ack(AckID("a"))
      try await waitUntil { batcher.queuedCount == 1 }
      clock.advance(by: .milliseconds(100))
      try await ack

      #expect(
        transport.calls == [
          .init(.modify(.extend(.seconds(30))), [AckID("a")]),
          .init(.acknowledge, [AckID("a")]),
        ])
    }
  }

  // MARK: - Fire-and-forget and batched requests

  @Test func shouldReturnFromBatchedModifyOnceAllItsRPCsCompleted() async throws {
    let transport = FakeAckTransport()
    var configuration = Configuration.testing
    configuration.maxBatchSize = 2

    try await withBatcher(transport: transport, configuration: configuration) { batcher in
      let modify = Task {
        await batcher.modifyDeadlines(["a", "b", "c"].map(AckID.init(rawValue:)), to: .seconds(30))
      }
      try await waitUntil { transport.calls.count == 1 && batcher.queuedCount == 1 }
      #expect(batcher.waiterCount == 1)

      clock.advance(by: .milliseconds(100))
      await modify.cancellableValue

      #expect(transport.calls.map(\.ids.count) == [2, 1])
      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldReturnFromBatchedModifyWhenCancelledAndStillSendIt() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      let modify = Task {
        await batcher.modifyDeadlines([AckID("a"), AckID("b")], to: .seconds(30))
      }
      try await waitUntil { batcher.queuedCount == 2 }

      modify.cancel()
      await modify.cancellableValue
      #expect(batcher.waiterCount == 0)

      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 }
      #expect(transport.normalizedCalls == [.init(.modify(.extend(.seconds(30))), ["a", "b"])])
      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldReturnFromBatchedModifyWhenRPCFails() async throws {
    let transport = FakeAckTransport { _, _ in
      throw RPCError(code: .invalidArgument, message: "Invalid ack ID")
    }

    try await withBatcher(transport: transport) { batcher in
      let modify = Task { await batcher.modifyDeadlines([AckID("a")], to: .seconds(30)) }
      try await waitUntil { batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))
      await modify.cancellableValue

      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldSendEnqueuedRequestsWithoutWaiters() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      batcher.enqueueModifyDeadlines([AckID("a"), AckID("b")], to: .seconds(20))
      batcher.enqueueNacks([AckID("c")])
      batcher.enqueueAcks([AckID("d")])
      #expect(batcher.queuedCount == 4)
      #expect(batcher.waiterCount == 0)

      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 3 }

      #expect(
        transport.normalizedCalls == [
          .init(.modify(.extend(.seconds(20))), ["a", "b"]),
          .init(.modify(.nack), ["c"]),
          .init(.acknowledge, ["d"]),
        ])
    }
  }

  // MARK: - Errors and retries

  @Test func shouldDeliverErrorsOnlyToCallersOfFailedRPC() async throws {
    let transport = FakeAckTransport { call, _ in
      if call.kind == .acknowledge {
        throw RPCError(code: .invalidArgument, message: "Invalid ack ID")
      }
    }

    try await withBatcher(transport: transport) { batcher in
      let ack = Task { try await batcher.ack(AckID("a")) }
      let nack = Task { try await batcher.nack(AckID("b")) }
      try await waitUntil { batcher.queuedCount == 2 }

      clock.advance(by: .milliseconds(100))

      let error = await #expect(throws: RPCError.self) { try await ack.cancellableValue }
      #expect(error?.code == .invalidArgument)
      try await nack.cancellableValue
    }
  }

  @Test func shouldRetryTransientErrorsThenSucceed() async throws {
    let transport = FakeAckTransport { _, attempt in
      if attempt == 0 {
        throw RPCError(code: .unavailable, message: "Connection reset")
      }
    }

    try await withBatcher(transport: transport) { batcher in
      async let a: Void = batcher.ack(AckID("a"))
      try await waitUntil { batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 && clock.sleeperCount == 1 }

      clock.advance(by: .seconds(1))
      try await a

      #expect(transport.calls == Array(repeating: .init(.acknowledge, [AckID("a")]), count: 2))
    }
  }

  @Test func shouldFailWithoutRetryingOnPermanentError() async throws {
    let transport = FakeAckTransport { _, _ in
      throw RPCError(code: .permissionDenied, message: "Denied")
    }

    try await withBatcher(transport: transport) { batcher in
      let ack = Task { try await batcher.ack(AckID("a")) }
      try await waitUntil { batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))

      let error = await #expect(throws: RPCError.self) { try await ack.cancellableValue }
      #expect(error?.code == .permissionDenied)
      #expect(transport.calls.count == 1)
      #expect(clock.sleeperCount == 0)
    }
  }

  @Test func shouldGiveUpRetryingAfterMaxRetryDuration() async throws {
    let transport = FakeAckTransport { _, _ in
      throw RPCError(code: .unavailable, message: "Unavailable")
    }
    var configuration = Configuration.testing
    configuration.maxRetryDuration = .seconds(2)

    try await withBatcher(transport: transport, configuration: configuration) { batcher in
      let ack = Task { try await batcher.ack(AckID("a")) }
      try await waitUntil { batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 && clock.sleeperCount == 1 }

      // The next delay, 2 seconds, would end after the 2 second retry budget.
      clock.advance(by: .seconds(1))

      let error = await #expect(throws: RPCError.self) { try await ack.cancellableValue }
      #expect(error?.code == .unavailable)
      #expect(transport.calls.count == 2)
    }
  }

  @Test(arguments: [
    (RPCError.Code.unavailable, true),
    (.deadlineExceeded, true),
    (.resourceExhausted, true),
    (.aborted, true),
    (.internalError, true),
    (.invalidArgument, false),
    (.notFound, false),
    (.permissionDenied, false),
    (.failedPrecondition, false),
    (.cancelled, false),
  ])
  func shouldClassifyTransientErrors(code: RPCError.Code, isTransient: Bool) {
    #expect(AckBatcher<TestClock>.isTransient(RPCError(code: code, message: "")) == isTransient)
  }

  @Test func shouldNotTreatNonRPCErrorsAsTransient() {
    #expect(!AckBatcher<TestClock>.isTransient(CancellationError()))
  }

  // MARK: - Shutdown

  @Test func shouldSendQueuedRequestsOnFinishWithoutWaitingForTimer() async throws {
    let transport = FakeAckTransport()
    let batcher = makeBatcher(transport: transport)

    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { await batcher.run() }
      async let a: Void = batcher.ack(AckID("a"))
      async let b: Void = batcher.nack(AckID("b"))
      try await waitUntil { batcher.queuedCount == 2 }

      batcher.finish()
      _ = try await (a, b)
    }

    #expect(
      transport.normalizedCalls == [
        .init(.acknowledge, ["a"]),
        .init(.modify(.nack), ["b"]),
      ])
    await #expect(throws: AckBatcherError.shutDown) {
      try await batcher.ack(AckID("c"))
    }
    #expect(transport.calls.count == 2)
  }

  @Test func shouldSendRequestsSubmittedWhileFinishing() async throws {
    let gate = Gate()
    let transport = FakeAckTransport { call, _ in
      if call.ids == [AckID("a")] {
        try await gate.wait()
      }
    }
    let batcher = makeBatcher(transport: transport)

    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { await batcher.run() }
      // Sends aren't cancelled, so the batcher only returns once the gate is open.
      defer { gate.open() }
      async let a: Void = batcher.ack(AckID("a"))
      try await waitUntil { batcher.queuedCount == 1 }
      batcher.finish()
      try await waitUntil { transport.calls.count == 1 }

      async let b: Void = batcher.ack(AckID("b"))
      try await b
      gate.open()
      try await a
    }

    #expect(transport.normalizedCalls == [.init(.acknowledge, ["a"]), .init(.acknowledge, ["b"])])
  }

  @Test func shouldSendQueuedRequestsWithoutCancellationWhenRunIsCancelled() async throws {
    let transport = FakeAckTransport { _, _ in
      try Task.checkCancellation()
    }
    let batcher = makeBatcher(transport: transport)
    let run = Task { await batcher.run() }
    defer { run.cancel() }

    async let a: Void = batcher.ack(AckID("a"))
    try await waitUntil { clock.sleeperCount == 1 }
    run.cancel()
    await run.cancellableValue

    try await a
    #expect(transport.calls == [.init(.acknowledge, [AckID("a")])])
  }

  @Test func shouldStopRetryingAtFlushDeadline() async throws {
    let transport = FakeAckTransport { _, _ in
      throw RPCError(code: .unavailable, message: "Unavailable")
    }
    let batcher = makeBatcher(transport: transport)

    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { await batcher.run() }
      let ack = Task { try await batcher.ack(AckID("a")) }
      try await waitUntil { batcher.queuedCount == 1 }
      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 && clock.sleeperCount == 1 }

      // The 10 second retry budget would allow 3 more attempts.
      batcher.finish(flushBy: clock.now.advanced(by: .milliseconds(2_500)))
      clock.advance(by: .seconds(1))

      let error = await #expect(throws: RPCError.self) { try await ack.cancellableValue }
      #expect(error?.code == .unavailable)
      #expect(transport.calls.count == 2)
    }
  }

  @Test func shouldCancelInFlightRPCsAtFlushDeadline() async throws {
    let transport = FakeAckTransport { _, _ in
      try await Task.sleep(for: .seconds(3_600))
    }
    let batcher = makeBatcher(transport: transport)
    let run = Task { await batcher.run() }
    defer { run.cancel() }

    let ack = Task { try await batcher.ack(AckID("a")) }
    try await waitUntil { batcher.queuedCount == 1 }
    clock.advance(by: .milliseconds(100))
    try await waitUntil { transport.calls.count == 1 }

    batcher.finish(flushBy: clock.now.advanced(by: .seconds(1)))
    try await waitUntil { clock.sleeperCount == 1 }
    clock.advance(by: .seconds(1))

    await run.cancellableValue
    await #expect(throws: AckBatcherError.flushDeadlineExceeded) { try await ack.cancellableValue }
    #expect(batcher.waiterCount == 0)
  }

  @Test func shouldFailUnsentRequestsWhenFlushDeadlineHasPassed() async throws {
    let transport = FakeAckTransport()
    let batcher = makeBatcher(transport: transport)
    let run = Task { await batcher.run() }
    defer { run.cancel() }

    let ack = Task { try await batcher.ack(AckID("a")) }
    let group = Task { await batcher.modifyDeadlines([AckID("b")], to: .seconds(30)) }
    try await waitUntil { batcher.queuedCount == 2 }
    batcher.finish(flushBy: clock.now)

    await run.cancellableValue
    await #expect(throws: AckBatcherError.flushDeadlineExceeded) { try await ack.cancellableValue }
    await group.cancellableValue
    #expect(transport.calls.isEmpty)
    #expect(batcher.waiterCount == 0)
    await #expect(throws: AckBatcherError.shutDown) { try await batcher.ack(AckID("c")) }
  }

  @Test func shouldOnlyMoveFlushDeadlineEarlier() async throws {
    let transport = FakeAckTransport()
    let batcher = makeBatcher(transport: transport)
    let run = Task { await batcher.run() }
    defer { run.cancel() }

    let ack = Task { try await batcher.ack(AckID("a")) }
    try await waitUntil { batcher.queuedCount == 1 }
    batcher.finish(flushBy: clock.now)
    batcher.finish(flushBy: clock.now.advanced(by: .seconds(10)))
    batcher.finish()

    await run.cancellableValue
    await #expect(throws: AckBatcherError.flushDeadlineExceeded) { try await ack.cancellableValue }
  }

  // MARK: - Cancellation

  @Test func shouldLetQueuedCallerCancelWithoutAffectingOthers() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      let a = Task { try await batcher.ack(AckID("a")) }
      let b = Task { try await batcher.ack(AckID("b")) }
      try await waitUntil { batcher.queuedCount == 2 }

      a.cancel()
      await #expect(throws: CancellationError.self) { try await a.cancellableValue }

      clock.advance(by: .milliseconds(100))
      try await b.cancellableValue

      #expect(transport.normalizedCalls == [.init(.acknowledge, ["a", "b"])])
      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldLetInFlightCallerCancelWithoutAffectingOthers() async throws {
    let gate = Gate()
    let transport = FakeAckTransport { _, _ in try await gate.wait() }

    try await withBatcher(transport: transport) { batcher in
      // Sends aren't cancelled, so the batcher only returns once the gate is open.
      defer { gate.open() }
      let a = Task { try await batcher.ack(AckID("a")) }
      let b = Task { try await batcher.ack(AckID("b")) }
      try await waitUntil { batcher.queuedCount == 2 }
      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 }

      a.cancel()
      await #expect(throws: CancellationError.self) { try await a.cancellableValue }
      #expect(batcher.waiterCount == 1)

      gate.open()
      try await b.cancellableValue

      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldStillSendRequestOfAlreadyCancelledCaller() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      let a = Task {
        withUnsafeCurrentTask { $0?.cancel() }
        try await batcher.ack(AckID("a"))
      }
      await #expect(throws: CancellationError.self) { try await a.cancellableValue }
      try await waitUntil { batcher.queuedCount == 1 }

      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 }

      #expect(transport.calls == [.init(.acknowledge, [AckID("a")])])
      #expect(batcher.waiterCount == 0)
    }
  }

  // MARK: - Helpers

  private func makeBatcher(
    transport: FakeAckTransport, configuration: Configuration = .testing
  ) -> AckBatcher<TestClock> {
    AckBatcher(
      transport: transport, configuration: configuration, clock: clock,
      logger: Logger(label: "test"))
  }

  private func withBatcher(
    transport: FakeAckTransport,
    configuration: Configuration = .testing,
    _ body: (AckBatcher<TestClock>) async throws -> Void
  ) async throws {
    let batcher = makeBatcher(transport: transport, configuration: configuration)
    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { await batcher.run() }
      defer { batcher.finish() }
      try await body(batcher)
    }
  }
}

@Suite struct AckTransportTypesTests {

  @Test func shouldEncodeDeadlineModificationsAsSeconds() {
    #expect(AckDeadlineModification.nack.seconds == 0)
    #expect(AckDeadlineModification.extend(.seconds(30)).seconds == 30)
    #expect(AckDeadlineModification.extend(.seconds(1)).seconds == 10)
  }

  @Test func shouldIncludeFieldOverheadInEncodedSize() {
    #expect(AckID("").encodedSize == 2)
    #expect(AckID(String(repeating: "x", count: 10)).encodedSize == 12)
    #expect(AckID(String(repeating: "x", count: 127)).encodedSize == 129)
    #expect(AckID(String(repeating: "x", count: 128)).encodedSize == 131)
  }
}

extension AckBatcher<TestClock>.Configuration {

  fileprivate static var testing: Self {
    var configuration = Self()
    configuration.retryBackoff = ExponentialBackoff(
      initial: .seconds(1), maximum: .seconds(4), multiplier: 2, jitter: 0)
    configuration.maxRetryDuration = .seconds(10)
    return configuration
  }
}
