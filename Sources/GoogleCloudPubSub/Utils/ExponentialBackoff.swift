/// Capped ("truncated") exponential backoff with jitter.
///
/// Each call to ``next(using:)`` returns the current delay with jitter applied and grows the
/// delay by ``multiplier`` until it reaches ``maximum``. Call ``reset()`` after a successful
/// attempt to start over from ``initial``.
struct ExponentialBackoff: Sendable, Equatable {

  let initial: Duration
  let maximum: Duration
  let multiplier: Double

  /// Fraction of the delay to randomly add or subtract. For example `0.2` yields a delay within
  /// ±20% of the un-jittered delay.
  let jitter: Double

  private(set) var current: Duration

  init(
    initial: Duration = .milliseconds(100),
    maximum: Duration = .seconds(60),
    multiplier: Double = 2,
    jitter: Double = 0.2
  ) {
    precondition(initial > .zero, "initial must be positive")
    precondition(maximum >= initial, "maximum must be greater than or equal to initial")
    precondition(multiplier >= 1, "multiplier must be at least 1")
    precondition((0...1).contains(jitter), "jitter must be within 0...1")

    self.initial = initial
    self.maximum = maximum
    self.multiplier = multiplier
    self.jitter = jitter
    self.current = initial
  }

  mutating func next() -> Duration {
    // Not `next(using:)` with a local `SystemRandomNumberGenerator`: being zero-sized, all such
    // locals share one address, which ThreadSanitizer reports as a race between tasks.
    next { Double.random(in: $0) }
  }

  mutating func next(using generator: inout some RandomNumberGenerator) -> Duration {
    next { Double.random(in: $0, using: &generator) }
  }

  private mutating func next(randomFactor: (ClosedRange<Double>) -> Double) -> Duration {
    let base = current
    current = min(base * multiplier, maximum)

    guard jitter > 0 else {
      return base
    }
    return min(base * randomFactor((1 - jitter)...(1 + jitter)), maximum)
  }

  mutating func reset() {
    current = initial
  }
}
