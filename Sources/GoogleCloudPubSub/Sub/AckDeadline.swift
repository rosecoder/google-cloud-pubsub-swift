/// The time a subscriber has to acknowledge a message before Pub/Sub redelivers it.
///
/// Always within the range accepted by Pub/Sub (10 to 600 seconds) and always a whole number of
/// seconds, since that is the precision of the API. Values outside the range are clamped and
/// fractional seconds are rounded up.
public struct AckDeadline: Sendable, Hashable, Comparable, CustomStringConvertible {

  public static let minimum = AckDeadline(clampedSeconds: 10)
  public static let maximum = AckDeadline(clampedSeconds: 600)

  public let duration: Duration

  public init(_ duration: Duration) {
    if duration <= Self.minimum.duration {
      self = .minimum
    } else if duration >= Self.maximum.duration {
      self = .maximum
    } else {
      let (seconds, attoseconds) = duration.components
      self.init(clampedSeconds: seconds + (attoseconds > 0 ? 1 : 0))
    }
  }

  public static func seconds(_ seconds: Int) -> AckDeadline {
    AckDeadline(.seconds(seconds))
  }

  private init(clampedSeconds: Int64) {
    self.duration = .seconds(clampedSeconds)
  }

  /// Whole seconds, as expected by `ackDeadlineSeconds` in Pub/Sub requests.
  public var seconds: Int32 {
    Int32(duration.components.seconds)
  }

  public static func < (lhs: AckDeadline, rhs: AckDeadline) -> Bool {
    lhs.duration < rhs.duration
  }

  public var description: String {
    "\(seconds)s"
  }
}
