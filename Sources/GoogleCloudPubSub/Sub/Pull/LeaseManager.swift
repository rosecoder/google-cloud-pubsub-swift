import Logging

/// Keeps extending the ack deadline of every message in an inventory, with one batched request
/// per tick for all of them, until cancelled.
///
/// Ticks are spread randomly between 10 ms and 90% of the ack deadline minus how long the last
/// extension took, as in the official clients, so many subscribers don't extend in lockstep.
/// While nothing is leased, the loop waits without ticking.
///
/// Each tick is planned from the deadline messages were last extended with. Messages accepted with
/// a shorter deadline than the one the pending tick was planned from, or after idling, make the
/// loop plan again, so no lease runs out before its renewal.
struct LeaseManager<C: Clock, Generator: RandomNumberGenerator & Sendable>: Sendable
where C.Duration == Duration, C.Instant == ContinuousClock.Instant {

  static var minimumDelay: Duration {
    .milliseconds(10)
  }

  let inventory: MessageInventory
  let clock: C
  let generator: Generator
  let logger: Logger
  let extendDeadlines: @Sendable (_ ids: [AckID], _ deadline: AckDeadline) async -> Void

  private struct Plan {
    let tick: C.Instant
    let deadline: AckDeadline
  }

  func run() async {
    var generator = self.generator
    var plan: Plan?
    while true {
      do {
        if try await inventory.waitUntilNonEmpty() {
          plan = nil
        }
        let current =
          plan ?? self.plan(deadline: inventory.ackDeadline, latency: .zero, using: &generator)
        if let shorter = try await sleep(
          until: current.tick, unlessAcceptedShorterThan: current.deadline)
        {
          let replanned = self.plan(deadline: shorter, latency: .zero, using: &generator)
          plan = Plan(tick: min(current.tick, replanned.tick), deadline: shorter)
          continue
        }
      } catch {
        return
      }

      let renewal = inventory.renewLeases(now: clock.now)
      if !renewal.expired.isEmpty {
        logger.warning(
          "Stopped extending the ack deadline of \(renewal.expired.count) messages handled for longer than maxLeaseDuration. They may be redelivered."
        )
      }

      let start = clock.now
      if !renewal.ids.isEmpty {
        await extendDeadlines(renewal.ids, renewal.ackDeadline)
      }
      plan = self.plan(
        deadline: renewal.ackDeadline, latency: start.duration(to: clock.now), using: &generator)
    }
  }

  private func plan(
    deadline: AckDeadline, latency: Duration, using generator: inout Generator
  ) -> Plan {
    let delay = Self.nextDelay(deadline: deadline, latency: latency, using: &generator)
    return Plan(tick: clock.now.advanced(by: delay), deadline: deadline)
  }

  /// - Returns: `nil` once `tick` is reached, or the shorter deadline if messages are accepted
  ///   with one first.
  private func sleep(
    until tick: C.Instant, unlessAcceptedShorterThan deadline: AckDeadline
  ) async throws -> AckDeadline? {
    try await withThrowingTaskGroup(of: AckDeadline?.self) { group in
      group.addTask {
        try await clock.sleep(until: tick, tolerance: nil)
        return nil
      }
      group.addTask {
        try await inventory.waitUntilAccepted(shorterThan: deadline)
      }
      defer { group.cancelAll() }
      return try await group.next()!
    }
  }

  static func nextDelay(
    deadline: AckDeadline, latency: Duration, using generator: inout some RandomNumberGenerator
  ) -> Duration {
    let maximum = max(minimumDelay, deadline.duration * 0.9 - latency)
    let range = maximum - minimumDelay
    guard range > .zero else {
      return minimumDelay
    }
    return minimumDelay + range * Double.random(in: 0...1, using: &generator)
  }
}
