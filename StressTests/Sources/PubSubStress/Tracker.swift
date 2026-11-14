import Foundation
import Synchronization

/// Log-linear histogram of microsecond values with 16 sub-buckets per power of two (~6% error).
struct Histogram: Sendable {

  private var buckets = [UInt64](repeating: 0, count: 61 * 16)
  private(set) var count: UInt64 = 0
  private(set) var maximum: UInt64 = 0

  mutating func record(micros: UInt64) {
    count += 1
    maximum = max(maximum, micros)
    buckets[Self.index(of: micros)] += 1
  }

  mutating func merge(_ other: Histogram) {
    guard other.count > 0 else { return }
    for index in buckets.indices {
      buckets[index] += other.buckets[index]
    }
    count += other.count
    maximum = max(maximum, other.maximum)
  }

  /// Upper bound of the bucket holding the percentile, in microseconds.
  func percentile(_ percent: Double) -> UInt64 {
    guard count > 0 else { return 0 }
    let rank = UInt64((Double(count) * percent / 100).rounded(.up))
    var seen: UInt64 = 0
    for (index, bucketCount) in buckets.enumerated() where bucketCount > 0 {
      seen += bucketCount
      if seen >= rank {
        return min(Self.upperBound(of: index), maximum)
      }
    }
    return maximum
  }

  private static func index(of value: UInt64) -> Int {
    if value < 16 {
      return Int(value)
    }
    let exponent = 63 - value.leadingZeroBitCount
    let mantissa = Int(value >> (exponent - 4)) - 16
    return (exponent - 3) * 16 + mantissa
  }

  private static func upperBound(of index: Int) -> UInt64 {
    if index < 16 {
      return UInt64(index)
    }
    let exponent = index / 16 + 3
    let mantissa = UInt64(index % 16 + 16)
    return ((mantissa + 1) << (exponent - 4)) - 1
  }
}

func formatMicros(_ micros: UInt64) -> String {
  if micros < 1_000 { return "\(micros)us" }
  if micros < 1_000_000 { return String(format: "%.1fms", Double(micros) / 1_000) }
  return String(format: "%.2fs", Double(micros) / 1_000_000)
}

/// What one subscription received, keyed by the per-topic sequence number in each message body.
///
/// Success is kept as one bit per sequence number, so tracking millions of messages costs little
/// memory compared to the library. Everything else is sparse.
final class SubscriptionTracker: Sendable {

  enum Outcome {
    case success
    case failure
  }

  struct Counts: Sendable {
    var deliveries = 0
    var firstDeliveries = 0
    var succeededUnique = 0
    var handlerFailures = 0
    var redeliveriesAfterNack = 0
    /// Delivered again after its handler succeeded: a lost ack, or a duplicate from the server.
    var redeliveriesAfterSuccess = 0
    /// Delivered again while a handler for it was still running: an expired lease or a duplicate.
    var concurrentDuplicates = 0
    var inFlight = 0

    mutating func add(_ other: Counts) {
      deliveries += other.deliveries
      firstDeliveries += other.firstDeliveries
      succeededUnique += other.succeededUnique
      handlerFailures += other.handlerFailures
      redeliveriesAfterNack += other.redeliveriesAfterNack
      redeliveriesAfterSuccess += other.redeliveriesAfterSuccess
      concurrentDuplicates += other.concurrentDuplicates
      inFlight += other.inFlight
    }
  }

  private struct State {
    var succeeded: [UInt64] = []
    var inFlight: [UInt64: Int] = [:]
    var failed: Set<UInt64> = []
    var counts = Counts()
    var latency = Histogram()
    var intervalLatency = Histogram()
    var redeliveredAfterSuccessSamples: [UInt64] = []

    func isSucceeded(_ seq: UInt64) -> Bool {
      let word = Int(seq / 64)
      return word < succeeded.count && succeeded[word] & (1 << (seq % 64)) != 0
    }

    mutating func markSucceeded(_ seq: UInt64) -> Bool {
      let word = Int(seq / 64)
      if word >= succeeded.count {
        succeeded.append(contentsOf: repeatElement(0, count: max(word + 1 - succeeded.count, succeeded.count / 2)))
      }
      let bit: UInt64 = 1 << (seq % 64)
      guard succeeded[word] & bit == 0 else { return false }
      succeeded[word] |= bit
      return true
    }
  }

  let index: Int
  let topicIndex: Int
  private let state = Mutex(State())

  init(index: Int, topicIndex: Int) {
    self.index = index
    self.topicIndex = topicIndex
  }

  func begin(_ seq: UInt64, publishedAtNanos: UInt64) {
    let now = wallClockNanos()
    state.withLock { state in
      state.counts.deliveries += 1
      state.counts.inFlight += 1
      if let running = state.inFlight[seq] {
        state.counts.concurrentDuplicates += 1
        state.inFlight[seq] = running + 1
        return
      }
      state.inFlight[seq] = 1
      if state.isSucceeded(seq) {
        state.counts.redeliveriesAfterSuccess += 1
        if state.redeliveredAfterSuccessSamples.count < 20 {
          state.redeliveredAfterSuccessSamples.append(seq)
        }
      } else if state.failed.contains(seq) {
        state.counts.redeliveriesAfterNack += 1
      } else {
        state.counts.firstDeliveries += 1
        let micros = now > publishedAtNanos ? (now - publishedAtNanos) / 1_000 : 0
        state.latency.record(micros: micros)
        state.intervalLatency.record(micros: micros)
      }
    }
  }

  func end(_ seq: UInt64, _ outcome: Outcome) {
    state.withLock { state in
      state.counts.inFlight -= 1
      if let running = state.inFlight[seq], running > 1 {
        state.inFlight[seq] = running - 1
      } else {
        state.inFlight[seq] = nil
      }
      switch outcome {
      case .success:
        if state.markSucceeded(seq) {
          state.counts.succeededUnique += 1
        }
        state.failed.remove(seq)
      case .failure:
        state.counts.handlerFailures += 1
        if !state.isSucceeded(seq) {
          state.failed.insert(seq)
        }
      }
    }
  }

  var counts: Counts {
    state.withLock { $0.counts }
  }

  var succeededUnique: Int {
    state.withLock { $0.counts.succeededUnique }
  }

  /// Returns the latency histogram since the last call and resets it.
  func takeIntervalLatency() -> Histogram {
    state.withLock { state in
      defer { state.intervalLatency = Histogram() }
      return state.intervalLatency
    }
  }

  var latency: Histogram {
    state.withLock { $0.latency }
  }

  var redeliveredAfterSuccessSamples: [UInt64] {
    state.withLock { $0.redeliveredAfterSuccessSamples }
  }

  var trackingBytes: Int {
    state.withLock { $0.succeeded.capacity * 8 + $0.failed.capacity * 9 + $0.inFlight.capacity * 17 }
  }

  /// Sequence numbers below `published` that never succeeded, excluding `unconfirmed` ones.
  func missing(published: UInt64, unconfirmed: [Range<UInt64>]) -> (count: Int, samples: [UInt64])
  {
    state.withLock { state in
      var count = 0
      var samples: [UInt64] = []
      var seq: UInt64 = 0
      while seq < published {
        if !state.isSucceeded(seq), !unconfirmed.contains(where: { $0.contains(seq) }) {
          count += 1
          if samples.count < 10 {
            samples.append(seq)
          }
        }
        seq += 1
      }
      return (count, samples)
    }
  }
}
