import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberOptionsTests {

  @Test func defaults() {
    let options = PullSubscriberOptions()
    #expect(options.flowControl.maxOutstandingMessages == 1_000)
    #expect(options.flowControl.maxOutstandingBytes == 100 * 1024 * 1024)
    #expect(options.lease.maxLeaseDuration == .seconds(60 * 60))
    #expect(options.lease.minAckDeadline == nil)
    #expect(options.lease.maxAckDeadline == .maximum)
    #expect(options.shutdownBehavior == .waitForProcessing(timeout: .seconds(30)))
  }

  @Test func customValues() {
    var options = PullSubscriberOptions(
      flowControl: .init(maxOutstandingMessages: 10, maxOutstandingBytes: 1024),
      lease: .init(
        maxLeaseDuration: .seconds(120), minAckDeadline: .seconds(20),
        maxAckDeadline: .seconds(40)),
      shutdownBehavior: .nackImmediately
    )
    #expect(options.flowControl.maxOutstandingMessages == 10)
    #expect(options.flowControl.maxOutstandingBytes == 1024)
    #expect(options.lease.maxLeaseDuration == .seconds(120))
    #expect(options.lease.minAckDeadline == .seconds(20))
    #expect(options.lease.maxAckDeadline == .seconds(40))
    #expect(options.shutdownBehavior == .nackImmediately)

    options.shutdownBehavior = .waitForProcessing(timeout: .seconds(5))
    #expect(options.shutdownBehavior != .waitForProcessing(timeout: .seconds(30)))
  }

  @Test func minAndMaxAckDeadlineMayBeEqual() {
    let lease = PullSubscriberOptions.Lease(
      minAckDeadline: .seconds(30), maxAckDeadline: .seconds(30))
    #expect(
      lease.ackDeadline(basedOn: AckLatencyHistogram(), exactlyOnceDelivery: false) == .seconds(30))
  }

  @Test func storedOnPullSubscriber() async throws {
    let options = PullSubscriberOptions(shutdownBehavior: .nackImmediately)
    let subscriber = try PullSubscriber(
      handler: NoopHandler(), projectID: "test-project", options: options)
    #expect(subscriber.options == options)

    let defaultSubscriber = try PullSubscriber(handler: NoopHandler(), projectID: "test-project")
    #expect(defaultSubscriber.options == PullSubscriberOptions())
  }

  @Test func supportedSpellings() {
    let primary = PullSubscriberOptions(shutdownBehavior: .nackImmediately)
    let annotated: PullSubscriber.Options = .init(shutdownBehavior: .nackImmediately)
    let nested: PullSubscriber.Options.Lease = .init()
    let specialized = PullSubscriber<NoopHandler>.Options(shutdownBehavior: .nackImmediately)
    #expect(annotated == primary)
    #expect(specialized == primary)
    #expect(nested == PullSubscriberOptions.Lease())
  }

  // MARK: - Validation

  @Test func rejectsNonPositiveMaxOutstandingMessages() async {
    await #expect(processExitsWith: .failure) {
      _ = PullSubscriberOptions.FlowControl(maxOutstandingMessages: 0)
    }
  }

  @Test func rejectsNonPositiveMaxOutstandingBytes() async {
    await #expect(processExitsWith: .failure) {
      _ = PullSubscriberOptions.FlowControl(maxOutstandingBytes: -1)
    }
  }

  @Test func rejectsNonPositiveMaxLeaseDuration() async {
    await #expect(processExitsWith: .failure) {
      _ = PullSubscriberOptions.Lease(maxLeaseDuration: .zero)
    }
  }

  @Test func rejectsMinAckDeadlineAboveMax() async {
    await #expect(processExitsWith: .failure) {
      _ = PullSubscriberOptions.Lease(minAckDeadline: .seconds(61), maxAckDeadline: .seconds(60))
    }
  }

  // MARK: - Effective ack deadline

  @Test func ackDeadlineWithoutDataIsMinimum() {
    let lease = PullSubscriberOptions.Lease()
    #expect(
      lease.ackDeadline(basedOn: AckLatencyHistogram(), exactlyOnceDelivery: false) == .minimum)
  }

  @Test func ackDeadlineFollowsPercentile() {
    let lease = PullSubscriberOptions.Lease()
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 42), exactlyOnceDelivery: false) == .seconds(42)
    )
  }

  @Test func ackDeadlineIsRaisedToMinimum() {
    let lease = PullSubscriberOptions.Lease(minAckDeadline: .seconds(30))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 12), exactlyOnceDelivery: false) == .seconds(30)
    )
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 45), exactlyOnceDelivery: false) == .seconds(45)
    )
  }

  @Test func ackDeadlineIsLoweredToMaximum() {
    let lease = PullSubscriberOptions.Lease(maxAckDeadline: .seconds(30))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 120), exactlyOnceDelivery: false)
        == .seconds(30))
  }

  @Test func ackDeadlineWithDefaultMaximumIsBoundedByPubSub() {
    let lease = PullSubscriberOptions.Lease()
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 900), exactlyOnceDelivery: false) == .maximum)
  }

  @Test func exactlyOnceDeliveryUsesHigherDefaultMinimum() {
    let lease = PullSubscriberOptions.Lease()
    #expect(
      lease.ackDeadline(basedOn: AckLatencyHistogram(), exactlyOnceDelivery: true) == .seconds(60))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 12), exactlyOnceDelivery: true) == .seconds(60))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 90), exactlyOnceDelivery: true) == .seconds(90))
  }

  @Test func exactlyOnceDeliveryRespectsExplicitMinimum() {
    let lease = PullSubscriberOptions.Lease(minAckDeadline: .seconds(20))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 12), exactlyOnceDelivery: true) == .seconds(20))
  }

  @Test func maximumTakesPrecedenceOverExactlyOnceDefaultMinimum() {
    let lease = PullSubscriberOptions.Lease(maxAckDeadline: .seconds(30))
    #expect(
      lease.ackDeadline(basedOn: histogram(seconds: 12), exactlyOnceDelivery: true) == .seconds(30))
  }

  private func histogram(seconds: Int) -> AckLatencyHistogram {
    var histogram = AckLatencyHistogram()
    histogram.record(.seconds(seconds))
    return histogram
  }
}

private struct NoopHandler: _Handler {

  let subscription = Subscription(
    name: "pull-subscriber-options-tests",
    topic: Topic<DataMessage>(name: "pull-subscriber-options-tests"))

  func handle(message: DataMessage.Incoming, context: HandlerContext) async throws {}
}
