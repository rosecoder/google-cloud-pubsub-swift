import GRPCCore
import Logging
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.timeLimit(.minutes(1)))
struct PublishRetryTests {

  @Test(arguments: [
    (RPCError(code: .unavailable, message: "Channel isn't ready."), PublishFailure.transient),
    (RPCError(code: .aborted, message: ""), .transient),
    (RPCError(code: .cancelled, message: ""), .transient),
    (RPCError(code: .deadlineExceeded, message: ""), .transient),
    (RPCError(code: .internalError, message: ""), .transient),
    (RPCError(code: .resourceExhausted, message: ""), .transient),
    (RPCError(code: .unknown, message: ""), .transient),
    (RPCError(code: .unknown, message: "", cause: CancellationError()), .timedOut),
    (RPCError(code: .notFound, message: ""), .permanent),
    (RPCError(code: .permissionDenied, message: ""), .permanent),
    (RPCError(code: .invalidArgument, message: ""), .permanent),
  ])
  func shouldClassifyRPCErrors(error: RPCError, failure: PublishFailure) {
    #expect(PublishFailure(error, isSenderCancelled: false) == failure)
  }

  @Test func shouldTreatCancellationAsTimeoutUnlessSenderWasCancelled() {
    #expect(PublishFailure(CancellationError(), isSenderCancelled: false) == .timedOut)
    #expect(PublishFailure(CancellationError(), isSenderCancelled: false).isRetryable)
    #expect(PublishFailure(CancellationError(), isSenderCancelled: true) == .cancelled)
    #expect(
      PublishFailure(
        RPCError(code: .unknown, message: "", cause: CancellationError()), isSenderCancelled: true)
        == .cancelled)
    #expect(!PublishFailure(CancellationError(), isSenderCancelled: true).isRetryable)
  }

  @Test func shouldNotRetryOtherErrors() {
    struct OtherError: Error {}
    #expect(PublishFailure(OtherError(), isSenderCancelled: false) == .permanent)
  }

  @Test func shouldDefaultToGoogleClientPublishSettings() {
    let policy = Publisher.RetryPolicy()
    #expect(policy.backoff.initial == .milliseconds(100))
    #expect(policy.backoff.multiplier == 4)
    #expect(policy.backoff.maximum == .seconds(60))
    #expect(policy.totalTimeout == .seconds(600))
  }

  @Test func shouldRetryWithExponentialBackoffUntilSuccess() async throws {
    let clock = TestClock()
    let attempts = Mutex(0)
    let recorder = LogRecorder()
    let task = Task {
      try await Publisher.withRetries(
        policy: Publisher.RetryPolicy(), clock: clock,
        logger: recorder.logger(label: "test")
      ) {
        let attempt = attempts.withLock {
          $0 += 1
          return $0
        }
        if attempt < 4 {
          throw RPCError(code: .unavailable, message: "Channel isn't ready.")
        }
        return attempt
      }
    }
    defer { task.cancel() }

    for (expected, completedAttempts) in [
      (Duration.milliseconds(100), 1), (.milliseconds(400), 2), (.milliseconds(1_600), 3),
    ] {
      try await waitUntil { clock.sleeperCount == 1 }
      #expect(attempts.withLock { $0 } == completedAttempts)
      let delay = try #require(clock.nextWakeUp)
      #expect(delay >= expected * 0.8 && delay <= expected * 1.2)
      clock.advance(by: delay)
    }

    #expect(try await task.value == 4)
    let levels = recorder.entries.filter { $0.message.contains("attempt") }.map(\.level)
    #expect(levels == [.debug, .debug, .debug])
  }

  @Test func shouldRetryTimedOutAttempts() async throws {
    let clock = TestClock()
    let attempts = Mutex(0)
    let task = Task {
      try await Publisher.withRetries(
        policy: Publisher.RetryPolicy(), clock: clock, logger: Logger(label: "test")
      ) {
        let attempt = attempts.withLock {
          $0 += 1
          return $0
        }
        if attempt == 1 {
          throw RPCError(code: .unknown, message: "", cause: CancellationError())
        }
        if attempt == 2 {
          throw CancellationError()
        }
      }
    }
    defer { task.cancel() }

    for _ in 0..<2 {
      try await waitUntil { clock.sleeperCount == 1 }
      clock.advance(by: try #require(clock.nextWakeUp))
    }
    try await task.value
    #expect(attempts.withLock { $0 } == 3)
  }

  @Test func shouldGiveUpWhenNextAttemptWouldExceedTotalTimeout() async throws {
    let clock = TestClock()
    let attempts = Mutex(0)
    let recorder = LogRecorder()
    var policy = Publisher.RetryPolicy()
    policy.backoff = ExponentialBackoff(
      initial: .milliseconds(100), maximum: .seconds(60), multiplier: 4, jitter: 0)
    policy.totalTimeout = .seconds(1)
    let task = Task {
      try await Publisher.withRetries(
        policy: policy, clock: clock, logger: recorder.logger(label: "test")
      ) {
        attempts.withLock { $0 += 1 }
        throw RPCError(code: .unavailable, message: "")
      }
    }
    defer { task.cancel() }

    // Retries after 100 and 400 milliseconds; the next 1.6 second delay would end past 1 second.
    for delay in [Duration.milliseconds(100), .milliseconds(400)] {
      try await waitUntil { clock.sleeperCount == 1 }
      #expect(clock.nextWakeUp == delay)
      clock.advance(by: delay)
    }

    let error = await #expect(throws: RPCError.self) { try await task.value }
    #expect(error?.code == .unavailable)
    #expect(attempts.withLock { $0 } == 3)
    #expect(recorder.entries.map(\.level) == [.debug, .debug, .warning])
  }

  @Test func shouldNotRetryPermanentFailures() async throws {
    let attempts = Mutex(0)
    let recorder = LogRecorder()
    await #expect(throws: RPCError.self) {
      try await Publisher.withRetries(
        policy: Publisher.RetryPolicy(), clock: TestClock(), logger: recorder.logger(label: "test")
      ) {
        attempts.withLock { $0 += 1 }
        throw RPCError(code: .permissionDenied, message: "")
      }
    }
    #expect(attempts.withLock { $0 } == 1)
    #expect(recorder.entries.map(\.level) == [.warning])
  }

  @Test func shouldNotRetryWhenCancelled() async throws {
    let attempts = Mutex(0)
    let recorder = LogRecorder()
    let task = Task {
      try await Publisher.withRetries(
        policy: Publisher.RetryPolicy(), clock: TestClock(), logger: recorder.logger(label: "test")
      ) {
        attempts.withLock { $0 += 1 }
        withUnsafeCurrentTask { $0?.cancel() }
        throw RPCError(code: .unknown, message: "", cause: CancellationError())
      }
    }
    await #expect(throws: RPCError.self) { try await task.value }
    #expect(attempts.withLock { $0 } == 1)
    #expect(recorder.entries.isEmpty)
  }

  @Test func shouldStopWaitingForRetryWhenCancelled() async throws {
    let clock = TestClock()
    let attempts = Mutex(0)
    let task = Task {
      try await Publisher.withRetries(
        policy: Publisher.RetryPolicy(), clock: clock, logger: Logger(label: "test")
      ) {
        attempts.withLock { $0 += 1 }
        throw RPCError(code: .unavailable, message: "")
      }
    }
    try await waitUntil { clock.sleeperCount == 1 }

    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(attempts.withLock { $0 } == 1)
  }
}
