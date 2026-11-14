import GRPCCore
import Logging
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct AckBatcherTests {

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
        try await task.value
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
        try await task.value
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

      let error = await #expect(throws: RPCError.self) { try await ack.value }
      #expect(error?.code == .invalidArgument)
      try await nack.value
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

      let error = await #expect(throws: RPCError.self) { try await ack.value }
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

      let error = await #expect(throws: RPCError.self) { try await ack.value }
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
        await gate.wait()
      }
    }
    let batcher = makeBatcher(transport: transport)

    try await withThrowingDiscardingTaskGroup { group in
      group.addTask { await batcher.run() }
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

    async let a: Void = batcher.ack(AckID("a"))
    try await waitUntil { clock.sleeperCount == 1 }
    run.cancel()
    await run.value

    try await a
    #expect(transport.calls == [.init(.acknowledge, [AckID("a")])])
  }

  // MARK: - Cancellation

  @Test func shouldLetQueuedCallerCancelWithoutAffectingOthers() async throws {
    let transport = FakeAckTransport()

    try await withBatcher(transport: transport) { batcher in
      let a = Task { try await batcher.ack(AckID("a")) }
      let b = Task { try await batcher.ack(AckID("b")) }
      try await waitUntil { batcher.queuedCount == 2 }

      a.cancel()
      await #expect(throws: CancellationError.self) { try await a.value }

      clock.advance(by: .milliseconds(100))
      try await b.value

      #expect(transport.normalizedCalls == [.init(.acknowledge, ["a", "b"])])
      #expect(batcher.waiterCount == 0)
    }
  }

  @Test func shouldLetInFlightCallerCancelWithoutAffectingOthers() async throws {
    let gate = Gate()
    let transport = FakeAckTransport { _, _ in await gate.wait() }

    try await withBatcher(transport: transport) { batcher in
      let a = Task { try await batcher.ack(AckID("a")) }
      let b = Task { try await batcher.ack(AckID("b")) }
      try await waitUntil { batcher.queuedCount == 2 }
      clock.advance(by: .milliseconds(100))
      try await waitUntil { transport.calls.count == 1 }

      a.cancel()
      await #expect(throws: CancellationError.self) { try await a.value }
      #expect(batcher.waiterCount == 1)

      gate.open()
      try await b.value

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
      await #expect(throws: CancellationError.self) { try await a.value }
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

private final class FakeAckTransport: AckTransport {

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

private final class Gate: Sendable {

  private struct State {
    var isOpen = false
    var waiters: [CheckedContinuation<Void, Never>] = []
  }

  private let state = Mutex(State())

  func wait() async {
    await withCheckedContinuation { continuation in
      let isOpen = state.withLock { state in
        if !state.isOpen {
          state.waiters.append(continuation)
        }
        return state.isOpen
      }
      if isOpen {
        continuation.resume()
      }
    }
  }

  func open() {
    let waiters = state.withLock { state in
      state.isOpen = true
      defer { state.waiters = [] }
      return state.waiters
    }
    for waiter in waiters {
      waiter.resume()
    }
  }
}
