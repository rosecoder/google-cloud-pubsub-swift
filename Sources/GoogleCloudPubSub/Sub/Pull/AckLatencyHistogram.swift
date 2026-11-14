/// Distribution of how long messages took from being received to being acknowledged, used to pick
/// ack deadlines for future messages.
///
/// Values are recorded with one second precision (rounded up) and clamped to the valid ack deadline
/// range, so storage is a fixed number of buckets and recording never allocates once the histogram
/// is uniquely referenced.
struct AckLatencyHistogram: Sendable {

  private static let minimumSeconds = Int(AckDeadline.minimum.seconds)
  private static let maximumSeconds = Int(AckDeadline.maximum.seconds)

  /// Bucket `i` counts latencies of `minimumSeconds + i` seconds.
  private var buckets: [Int]

  private(set) var count = 0

  init() {
    buckets = Array(repeating: 0, count: Self.maximumSeconds - Self.minimumSeconds + 1)
  }

  var isEmpty: Bool {
    count == 0
  }

  mutating func record(_ latency: Duration) {
    let seconds = Int(AckDeadline(latency).seconds)
    buckets[seconds - Self.minimumSeconds] += 1
    count += 1
  }

  /// The smallest recorded latency that at least `percent` percent of all recorded latencies are
  /// less than or equal to, or ``AckDeadline/minimum`` if nothing has been recorded.
  ///
  /// Matches the official Node client, which errs on the side of the longer deadline when the
  /// percentile falls exactly on a bucket boundary.
  func percentile(_ percent: Int) -> AckDeadline {
    guard count > 0 else {
      return .minimum
    }

    // Scaled by 100 to keep the arithmetic exact.
    var remaining = count * (100 - min(max(percent, 0), 100))
    for index in buckets.indices.reversed() where buckets[index] > 0 {
      remaining -= buckets[index] * 100
      if remaining <= 0 {
        return .seconds(Self.minimumSeconds + index)
      }
    }
    return .minimum
  }
}
