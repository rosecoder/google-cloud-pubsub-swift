import Testing

@testable import GoogleCloudPubSub

@Suite struct AckLatencyHistogramTests {

  @Test func emptyReturnsMinimum() {
    let histogram = AckLatencyHistogram()
    #expect(histogram.isEmpty)
    #expect(histogram.count == 0)
    #expect(histogram.percentile(99) == .minimum)
    #expect(histogram.percentile(0) == .minimum)
    #expect(histogram.percentile(100) == .minimum)
  }

  @Test func singleValue() {
    var histogram = AckLatencyHistogram()
    histogram.record(.seconds(42))
    #expect(!histogram.isEmpty)
    #expect(histogram.count == 1)
    #expect(histogram.percentile(0) == .seconds(42))
    #expect(histogram.percentile(50) == .seconds(42))
    #expect(histogram.percentile(99) == .seconds(42))
    #expect(histogram.percentile(100) == .seconds(42))
  }

  @Test func outOfRangeValuesAreClamped() {
    var histogram = AckLatencyHistogram()
    histogram.record(.zero)
    histogram.record(.seconds(-5))
    histogram.record(.milliseconds(500))
    #expect(histogram.percentile(100) == .minimum)

    histogram.record(.seconds(601))
    histogram.record(.seconds(60 * 60 * 24))
    #expect(histogram.count == 5)
    #expect(histogram.percentile(100) == .maximum)
    #expect(histogram.percentile(0) == .minimum)
  }

  @Test func fractionalSecondsRoundUp() {
    var histogram = AckLatencyHistogram()
    histogram.record(.milliseconds(12_001))
    #expect(histogram.percentile(99) == .seconds(13))
  }

  @Test func manyValues() {
    var histogram = AckLatencyHistogram()
    for seconds in 1...100 {
      histogram.record(.seconds(seconds))
    }
    #expect(histogram.count == 100)
    // Exactly 1% lies above 99 s; like the Node client, the boundary resolves to the larger value.
    #expect(histogram.percentile(99) == .seconds(100))
    #expect(histogram.percentile(98) == .seconds(99))
    #expect(histogram.percentile(50) == .seconds(51))
    // 1 through 10 all fall in the 10 s bucket.
    #expect(histogram.percentile(10) == .seconds(11))
    #expect(histogram.percentile(9) == .minimum)
    #expect(histogram.percentile(0) == .minimum)
    #expect(histogram.percentile(100) == .seconds(100))
  }

  @Test func percentileIgnoresSmallTail() {
    var histogram = AckLatencyHistogram()
    for _ in 0..<995 {
      histogram.record(.seconds(20))
    }
    for _ in 0..<5 {
      histogram.record(.seconds(300))
    }
    #expect(histogram.percentile(99) == .seconds(20))
    #expect(histogram.percentile(100) == .seconds(300))
  }

  @Test func percentileIncludesTailAboveOnePercent() {
    var histogram = AckLatencyHistogram()
    for _ in 0..<980 {
      histogram.record(.seconds(20))
    }
    for _ in 0..<20 {
      histogram.record(.seconds(300))
    }
    #expect(histogram.percentile(99) == .seconds(300))
  }

  @Test func percentOutsideRangeIsClamped() {
    var histogram = AckLatencyHistogram()
    histogram.record(.seconds(15))
    histogram.record(.seconds(30))
    #expect(histogram.percentile(150) == histogram.percentile(100))
    #expect(histogram.percentile(-10) == histogram.percentile(0))
  }

  @Test func copiesAreIndependent() {
    var original = AckLatencyHistogram()
    original.record(.seconds(20))
    var copy = original
    copy.record(.seconds(500))
    #expect(original.count == 1)
    #expect(original.percentile(100) == .seconds(20))
    #expect(copy.percentile(100) == .seconds(500))
  }
}
