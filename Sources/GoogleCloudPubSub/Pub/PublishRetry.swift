import GRPCCore

extension Publisher {

  /// How failed publish RPCs are retried. Defaults to the publish settings of Google's client
  /// libraries: delays from 100 milliseconds growing fourfold up to 60 seconds, for 600 seconds.
  struct RetryPolicy: Sendable, Equatable {

    var backoff = ExponentialBackoff(
      initial: .milliseconds(100), maximum: .seconds(60), multiplier: 4)

    /// Time after the first attempt after which a failed publish is no longer retried.
    ///
    /// Must exceed the 60 second call timeout for timed out RPCs to be retried at all.
    var totalTimeout: Duration = .seconds(600)
  }
}

/// Why a publish RPC failed, deciding whether it is retried. Retries the codes Google's client
/// libraries retry for publishing.
enum PublishFailure: Equatable {

  /// The server or connection failed in a way that may not recur.
  case transient

  /// The RPC's own timeout fired, which grpc-swift reports as `deadlineExceeded`, or as a
  /// cancellation when it fires while the stream is opened.
  case timedOut

  /// The task publishing was cancelled.
  case cancelled

  case permanent

  /// - Parameter isSenderCancelled: Whether the task sending the RPC was cancelled, since its RPC
  ///   then fails with the same errors as one that timed out.
  init(_ error: any Error, isSenderCancelled: Bool) {
    if isSenderCancelled {
      self = .cancelled
    } else if error is CancellationError {
      self = .timedOut
    } else if let error = error as? RPCError {
      self =
        switch error.code {
        case .deadlineExceeded:
          .timedOut
        case .unknown where error.cause is CancellationError:
          .timedOut
        case .aborted, .cancelled, .internalError, .resourceExhausted, .unavailable, .unknown:
          .transient
        default:
          .permanent
        }
    } else {
      self = .permanent
    }
  }

  var isRetryable: Bool {
    self == .transient || self == .timedOut
  }
}
