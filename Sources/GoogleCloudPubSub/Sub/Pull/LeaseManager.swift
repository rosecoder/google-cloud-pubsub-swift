import Logging

/// Keeps extending the ack deadline of every message in an inventory, with one batched request
/// per tick for all of them, until cancelled.
///
/// Ticks are spread randomly between 10 ms and 90% of the ack deadline minus how long the last
/// extension took, as in the official clients, so many subscribers don't extend in lockstep.
/// While nothing is leased, the loop waits without ticking.
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

  func run() async {
    var generator = self.generator
    var delay: Duration?
    while true {
      do {
        try await inventory.waitUntilNonEmpty()
        try await clock.sleep(
          for: delay
            ?? Self.nextDelay(
              deadline: inventory.ackDeadline, latency: .zero, using: &generator))
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
      delay = Self.nextDelay(
        deadline: renewal.ackDeadline, latency: start.duration(to: clock.now), using: &generator)
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
