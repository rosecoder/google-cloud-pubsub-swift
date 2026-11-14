import Logging
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct LeaseManagerTests {

  typealias Manager = LeaseManager<TestClock, SplitMix64>

  let clock = TestClock()
  let recorder = ExtensionRecorder()

  // MARK: - Delay

  @Test(arguments: [(10, 0), (30, 2), (600, 100)])
  func shouldDelayUniformlyUpTo90PercentOfDeadlineMinusLatency(
    deadlineSeconds: Int, latencySeconds: Int
  ) {
    var generator = SplitMix64(seed: 42)
    let maximum = Duration.seconds(deadlineSeconds) * 0.9 - .seconds(latencySeconds)
    let tolerance = (maximum - Manager.minimumDelay) / 100

    let delays = (0..<10_000).map { _ in
      Manager.nextDelay(
        deadline: .seconds(deadlineSeconds), latency: .seconds(latencySeconds),
        using: &generator)
    }

    #expect(delays.allSatisfy { $0 >= Manager.minimumDelay && $0 <= maximum })
    #expect(delays.min()! < Manager.minimumDelay + tolerance)
    #expect(delays.max()! > maximum - tolerance)
  }

  @Test(arguments: [9, 20])
  func shouldUseMinimumDelayWhenLatencyLeavesNoTime(latencySeconds: Int) {
    var generator = SplitMix64(seed: 42)

    for _ in 0..<100 {
      let delay = Manager.nextDelay(
        deadline: .seconds(10), latency: .seconds(latencySeconds), using: &generator)
      #expect(delay == .milliseconds(10))
    }
  }

  // MARK: - Loop

  @Test(.timeLimit(.minutes(1)))
  func shouldExtendHeldAndDispatchedMessagesInOneCall() async throws {
    let inventory = MessageInventory(
      flowControl: .init(maxOutstandingMessages: 1), initialAckDeadline: .seconds(10))
    _ = inventory.add([message("a"), message("b")], now: clock.now)

    try await withLeaseManager(inventory) {
      try await tick()
      try await waitUntil { recorder.extensions.count == 1 }

      #expect(recorder.extensions == [Extension(["a", "b"], .seconds(10))])
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldStopExtendingExpiredMessagesAndDispatchHeldOnes() async throws {
    let inventory = MessageInventory(
      flowControl: .init(maxOutstandingMessages: 1),
      lease: .init(maxLeaseDuration: .seconds(15)),
      initialAckDeadline: .seconds(10))
    _ = inventory.add([message("a"), message("b")], now: clock.now)
    let dispatched = inventory.dispatched.makeAsyncIterator()
    #expect(await dispatched.next()?.ackID == AckID("ack-a"))

    try await withLeaseManager(inventory) {
      try await tick()
      try await tick()
      try await waitUntil { recorder.extensions.count == 2 }

      #expect(
        recorder.extensions == [
          Extension(["a", "b"], .seconds(10)),
          Extension(["b"], .seconds(10)),
        ])
      #expect(await dispatched.next()?.ackID == AckID("ack-b"))
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldNotTickWhileEmptyAndResumeWhenMessagesAreAdded() async throws {
    let inventory = MessageInventory(
      flowControl: .init(maxOutstandingMessages: 10), initialAckDeadline: .seconds(10))

    try await withLeaseManager(inventory) {
      await yieldRepeatedly()
      #expect(clock.sleeperCount == 0)

      _ = inventory.add([message("a")], now: clock.now)
      try await tick()
      try await waitUntil { recorder.extensions.count == 1 }

      try await waitUntil { clock.sleeperCount == 1 }
      inventory.release(AckID("ack-a"), now: clock.now)
      clock.advance(by: .seconds(9))
      await yieldRepeatedly()
      #expect(clock.sleeperCount == 0)
      #expect(recorder.extensions.count == 1)

      _ = inventory.add([message("b")], now: clock.now)
      try await tick()
      try await waitUntil { recorder.extensions.count == 2 }

      #expect(recorder.extensions.last == Extension(["b"], .seconds(10)))
    }
  }

  // MARK: - Helpers

  /// Fires the next renewal, which is at most 90% of the 10 second deadline away.
  private func tick() async throws {
    try await waitUntil { clock.sleeperCount == 1 }
    clock.advance(by: .seconds(9))
  }

  private func yieldRepeatedly() async {
    for _ in 0..<1_000 {
      await Task.yield()
    }
  }

  private func withLeaseManager(
    _ inventory: MessageInventory, _ body: () async throws -> Void
  ) async throws {
    let manager = Manager(
      inventory: inventory, clock: clock, generator: SplitMix64(seed: 1),
      logger: Logger(label: "test")
    ) { [recorder] ids, deadline in
      recorder.record(Extension(ids: Set(ids), deadline: deadline))
    }
    let run = Task { await manager.run() }
    defer { run.cancel() }
    try await body()
    run.cancel()
    await run.value
  }

  private func message(_ id: String) -> Google_Pubsub_V1_ReceivedMessage {
    FakePullTransport.message(id: id, size: 0)
  }
}

struct Extension: Equatable, Sendable {
  let ids: Set<AckID>
  let deadline: AckDeadline

  init(ids: Set<AckID>, deadline: AckDeadline) {
    self.ids = ids
    self.deadline = deadline
  }

  init(_ ids: Set<String>, _ deadline: AckDeadline) {
    self.init(ids: Set(ids.map { AckID("ack-\($0)") }), deadline: deadline)
  }
}

final class ExtensionRecorder: Sendable {

  private let recorded = Mutex<[Extension]>([])

  var extensions: [Extension] {
    recorded.withLock { $0 }
  }

  func record(_ extension: Extension) {
    recorded.withLock { $0.append(`extension`) }
  }
}
