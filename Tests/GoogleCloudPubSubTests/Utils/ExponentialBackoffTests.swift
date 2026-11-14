import Testing

@testable import GoogleCloudPubSub

@Suite struct ExponentialBackoffTests {

  @Test func shouldDefaultToNodeClientValues() {
    let backoff = ExponentialBackoff()

    #expect(backoff.initial == .milliseconds(100))
    #expect(backoff.maximum == .seconds(60))
    #expect(backoff.multiplier == 2)
    #expect(backoff.current == .milliseconds(100))
  }

  @Test func shouldGrowExponentiallyUntilCapped() {
    var backoff = ExponentialBackoff(
      initial: .milliseconds(100), maximum: .seconds(1), multiplier: 2, jitter: 0)

    let delays = (0..<7).map { _ in backoff.next() }

    #expect(
      delays == [
        .milliseconds(100), .milliseconds(200), .milliseconds(400), .milliseconds(800),
        .seconds(1), .seconds(1), .seconds(1),
      ])
  }

  @Test func shouldResetToInitial() {
    var backoff = ExponentialBackoff(
      initial: .milliseconds(100), maximum: .seconds(1), multiplier: 2, jitter: 0)
    _ = backoff.next()
    _ = backoff.next()
    _ = backoff.next()

    backoff.reset()

    #expect(backoff.current == .milliseconds(100))
    #expect(backoff.next() == .milliseconds(100))
    #expect(backoff.next() == .milliseconds(200))
  }

  @Test func shouldStayWithinJitterBoundsAndNeverExceedMaximum() {
    let initial = Duration.milliseconds(100)
    let maximum = Duration.seconds(2)
    var backoff = ExponentialBackoff(
      initial: initial, maximum: maximum, multiplier: 2, jitter: 0.2)
    var generator = SplitMix64(seed: 42)
    var base = initial
    var cappedBelowMaximumCount = 0

    for _ in 0..<1_000 {
      let delay = backoff.next(using: &generator)

      #expect(delay >= base * 0.8)
      #expect(delay <= min(base * 1.2, maximum))
      if base == maximum && delay < maximum {
        cappedBelowMaximumCount += 1
      }
      base = min(base * 2, maximum)
    }
    #expect(cappedBelowMaximumCount > 0)
  }

  @Test func shouldBeDeterministicForSameRandomSource() {
    var first = ExponentialBackoff()
    var second = ExponentialBackoff()
    var firstGenerator = SplitMix64(seed: 7)
    var secondGenerator = SplitMix64(seed: 7)

    let firstDelays = (0..<20).map { _ in first.next(using: &firstGenerator) }
    let secondDelays = (0..<20).map { _ in second.next(using: &secondGenerator) }

    #expect(firstDelays == secondDelays)
  }
}

private struct SplitMix64: RandomNumberGenerator {

  private var state: UInt64

  init(seed: UInt64) {
    self.state = seed
  }

  mutating func next() -> UInt64 {
    state &+= 0x9E37_79B9_7F4A_7C15
    var z = state
    z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
    z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
    return z ^ (z >> 31)
  }
}
