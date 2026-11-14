extension PullSubscriber {

  /// Alias of ``PullSubscriberOptions``.
  ///
  /// Without a generic argument it can only be used where a type is expected, such as
  /// `let options: PullSubscriber.Options = .init()`. `PullSubscriber.Options()` does not compile
  /// because `Handler` cannot be inferred, so prefer ``PullSubscriberOptions`` in expressions.
  public typealias Options = PullSubscriberOptions
}

/// Configuration of how a ``PullSubscriber`` receives, leases and releases messages.
///
/// Defaults match Google's official Pub/Sub client libraries unless noted otherwise.
public struct PullSubscriberOptions: Sendable, Hashable {

  public var flowControl: FlowControl
  public var lease: Lease
  public var shutdownBehavior: ShutdownBehavior

  public init(
    flowControl: FlowControl = FlowControl(),
    lease: Lease = Lease(),
    shutdownBehavior: ShutdownBehavior = .waitForProcessing(timeout: .seconds(30))
  ) {
    self.flowControl = flowControl
    self.lease = lease
    self.shutdownBehavior = shutdownBehavior
  }

  /// Limits on messages that have been received but not yet acknowledged or nacked by this
  /// subscriber. Receiving pauses while either limit is reached and resumes once usage of both
  /// drops below 80%. Messages are never nacked because of these limits.
  public struct FlowControl: Sendable, Hashable {

    /// Also the maximum number of handlers running concurrently, since each outstanding message
    /// is handled in its own task. Lower it if handlers share a limited resource, such as a
    /// connection pool.
    public let maxOutstandingMessages: Int

    /// Total size of outstanding message data. A single message larger than this is still
    /// handled once no other message is outstanding.
    public let maxOutstandingBytes: Int

    /// - Precondition: Both limits are greater than zero.
    public init(
      maxOutstandingMessages: Int = 1_000,
      maxOutstandingBytes: Int = 100 * 1024 * 1024
    ) {
      precondition(maxOutstandingMessages > 0, "maxOutstandingMessages must be greater than zero")
      precondition(maxOutstandingBytes > 0, "maxOutstandingBytes must be greater than zero")
      self.maxOutstandingMessages = maxOutstandingMessages
      self.maxOutstandingBytes = maxOutstandingBytes
    }
  }

  /// How the ack deadline of received messages is extended while they are being processed.
  ///
  /// Each extension uses the 99th percentile of observed time from receiving a message until its
  /// handler succeeded, or the subscription's `acknowledgeDeadline` until that has been observed,
  /// bounded by ``minAckDeadline`` and ``maxAckDeadline``.
  public struct Lease: Sendable, Hashable {

    /// Minimum ack deadline used for subscriptions with exactly-once delivery when no
    /// ``minAckDeadline`` is set.
    public static let exactlyOnceDeliveryMinAckDeadline = AckDeadline.seconds(60)

    /// Total time a message is kept leased after its handler started before it is left to expire
    /// and be redelivered. Messages waiting for flow control capacity are leased without limit.
    public let maxLeaseDuration: Duration

    /// Lower bound of each lease extension. When `nil`, no bound is applied except for
    /// subscriptions with exactly-once delivery, which use
    /// ``exactlyOnceDeliveryMinAckDeadline``.
    public let minAckDeadline: AckDeadline?

    /// Upper bound of each lease extension, which also bounds how long it takes for a message to
    /// be redelivered if this subscriber stops extending its lease. Takes precedence over
    /// ``exactlyOnceDeliveryMinAckDeadline``. The default, ``AckDeadline/maximum``, applies no
    /// bound beyond what Pub/Sub accepts.
    public let maxAckDeadline: AckDeadline

    /// - Precondition: `maxLeaseDuration` is greater than zero and `minAckDeadline` is not
    ///   greater than `maxAckDeadline`.
    public init(
      maxLeaseDuration: Duration = .seconds(60 * 60),
      minAckDeadline: AckDeadline? = nil,
      maxAckDeadline: AckDeadline = .maximum
    ) {
      precondition(maxLeaseDuration > .zero, "maxLeaseDuration must be greater than zero")
      precondition(
        minAckDeadline.map { $0 <= maxAckDeadline } ?? true,
        "minAckDeadline must not be greater than maxAckDeadline")
      self.maxLeaseDuration = maxLeaseDuration
      self.minAckDeadline = minAckDeadline
      self.maxAckDeadline = maxAckDeadline
    }

    /// - Parameter initial: Used instead of the percentile until a latency has been recorded.
    func ackDeadline(
      basedOn histogram: AckLatencyHistogram, initial: AckDeadline, exactlyOnceDelivery: Bool
    ) -> AckDeadline {
      var deadline = histogram.isEmpty ? initial : histogram.percentile(99)
      if let minimum = minAckDeadline
        ?? (exactlyOnceDelivery ? Self.exactlyOnceDeliveryMinAckDeadline : nil)
      {
        deadline = max(deadline, minimum)
      }
      return min(deadline, maxAckDeadline)
    }
  }

  /// What happens to messages still outstanding when the subscriber shuts down gracefully.
  ///
  /// Either way, messages received but not yet handed to a handler are nacked right away, and
  /// shutdown finishes once pending acks and nacks have been sent. Cancelling
  /// ``PullSubscriber/run()`` instead skips all of this.
  public enum ShutdownBehavior: Sendable, Hashable {

    /// Nack all outstanding messages and cancel their handlers right away so Pub/Sub can
    /// redeliver them to another subscriber. Results of handlers finishing afterwards are
    /// dropped.
    case nackImmediately

    /// Let handlers finish without cancelling them while their messages are kept leased. One
    /// second before `timeout` elapses, remaining messages are nacked and their handlers
    /// cancelled, leaving that second to send pending acks and nacks.
    ///
    /// `timeout` is limited to ``Lease/maxLeaseDuration``. A `ServiceGroup` running the
    /// subscriber should allow at least `timeout` plus 1 second for graceful shutdown before
    /// cancelling it.
    case waitForProcessing(timeout: Duration)
  }
}
