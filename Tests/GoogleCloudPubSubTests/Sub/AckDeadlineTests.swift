import GoogleCloudPubSub
import Testing

@Suite struct AckDeadlineTests {

  @Test func bounds() {
    #expect(AckDeadline.minimum.duration == .seconds(10))
    #expect(AckDeadline.maximum.duration == .seconds(600))
    #expect(AckDeadline.minimum.seconds == 10)
    #expect(AckDeadline.maximum.seconds == 600)
  }

  @Test(arguments: [
    (Duration.seconds(-5), Int32(10)),
    (.zero, 10),
    (.milliseconds(9_999), 10),
    (.seconds(10), 10),
    (.seconds(42), 42),
    (.seconds(600), 600),
    (.seconds(601), 600),
    (.seconds(60 * 60), 600),
    (.seconds(Int64.max), 600),
  ])
  func clampsToValidRange(duration: Duration, expectedSeconds: Int32) {
    let deadline = AckDeadline(duration)
    #expect(deadline.seconds == expectedSeconds)
    #expect(deadline.duration == .seconds(expectedSeconds))
  }

  @Test(arguments: [
    (Duration.milliseconds(10_001), Int32(11)),
    (.seconds(59) + .nanoseconds(1), 60),
    (.milliseconds(599_500), 600),
  ])
  func roundsFractionalSecondsUp(duration: Duration, expectedSeconds: Int32) {
    #expect(AckDeadline(duration).seconds == expectedSeconds)
  }

  @Test func secondsFactory() {
    #expect(AckDeadline.seconds(60) == AckDeadline(.seconds(60)))
    #expect(AckDeadline.seconds(1) == .minimum)
    #expect(AckDeadline.seconds(1_000) == .maximum)
  }

  @Test func comparable() {
    #expect(AckDeadline.minimum < .seconds(11))
    #expect(AckDeadline.seconds(599) < .maximum)
    #expect([AckDeadline.seconds(30), .minimum, .maximum].sorted() == [.minimum, .seconds(30), .maximum])
  }

  @Test func description() {
    #expect(AckDeadline.seconds(42).description == "42s")
  }
}
