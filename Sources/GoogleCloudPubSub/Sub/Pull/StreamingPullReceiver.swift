import GRPCCore
import Logging
import NIOCore
import Synchronization

/// The StreamingPull RPC of a subscriber client.
protocol StreamingPullConnection: Sendable {

  func streamingPull<Result: Sendable>(
    metadata: Metadata,
    options: CallOptions,
    requestProducer:
      @Sendable @escaping (RPCWriter<Google_Pubsub_V1_StreamingPullRequest>) async throws -> Void,
    onResponse:
      @Sendable @escaping (StreamingClientResponse<Google_Pubsub_V1_StreamingPullResponse>)
      async throws -> Result
  ) async throws -> Result
}

extension Google_Pubsub_V1_Subscriber.Client: StreamingPullConnection {}

/// Receives the messages of one subscription over a single StreamingPull stream, reconnecting
/// until cancelled or until the inventory stops receiving.
///
/// The stream only carries the initial request, keepalives and ack deadline updates. Acks and
/// deadline modifications are sent with unary RPCs by the ``AckBatcher``.
///
/// Responses are not read while the inventory pauses receiving, so HTTP/2 flow control holds back
/// the server. Messages the server sends beyond the limits anyway are held by the inventory.
struct StreamingPullReceiver: Sendable {

  /// Interval of empty requests that keep the stream from being closed as idle.
  var keepAliveInterval: Duration = .seconds(30)

  /// Time after which each stream is closed and a new one opened, as in the official clients.
  var streamTimeout: Duration = .seconds(900)

  /// Time a stream must stay open for the backoff to start over once it ends, even if no message
  /// arrived. Pub/Sub regularly closes idle streams, and the Go client also reconnects right away
  /// after a stream that lasted 30 seconds.
  var healthyStreamDuration: Duration = .seconds(30)

  /// Runs `body` with a connection to open one stream on, holding a slot of the connection
  /// until `body` returns.
  typealias WithConnection =
    @Sendable (_ body: @Sendable (any StreamingPullConnection) async throws -> Void) async throws
    -> Void

  /// Called for each stream, so a stream reopened after backing off may use another connection.
  let withConnection: WithConnection

  /// Fully qualified subscription name, `projects/{project}/subscriptions/{subscription}`.
  let subscriptionID: String

  /// Identifies this subscriber across reconnects, so Pub/Sub keeps its server-side state.
  let clientID: String
  let flowControl: PullSubscriberOptions.FlowControl
  let logger: Logger
  let now: @Sendable () -> ContinuousClock.Instant
  let sleep: @Sendable (_ deadline: ContinuousClock.Instant) async throws -> Void
  let createSubscription: @Sendable () async throws -> Void

  func run(into inventory: MessageInventory, ackBatcher: AckBatcher<ContinuousClock>) async {
    let session = Session()
    var backoff = ExponentialBackoff()
    var outage = Outage(since: now())
    while inventory.isReceiving {
      let attempt = Attempt()
      var failure: (any Error)?
      do {
        try await withConnection { connection in
          try await stream(
            on: connection, into: inventory, ackBatcher: ackBatcher, session: session,
            attempt: attempt)
        }
      } catch {
        failure = error
      }

      guard inventory.isReceiving,
        let retry = StreamingPullRetryPolicy.retry(after: failure, isCancelled: Task.isCancelled)
      else {
        return
      }
      let end = now()
      if attempt.wasHealthy(at: end, minimumDuration: healthyStreamDuration) {
        backoff.reset()
        outage = Outage(since: end)
      }
      if retry.escalates {
        outage.failures += 1
      }
      if retry.createsSubscription {
        do {
          try await createSubscription()
        } catch {
          logger.error("Failed to create subscription: \(error)")
        }
      }
      let delay: Duration
      switch retry.delay {
      case .initial:
        backoff.reset()
        delay = backoff.next()
      case .increasing:
        delay = backoff.next()
      case .maximum:
        delay = backoff.maximum
      }
      let logLevel =
        retry.escalates ? max(retry.logLevel, outage.logLevel(at: end)) : retry.logLevel
      let context = retry.escalates ? ", \(outage.description(at: end))" : ""
      logger.log(
        level: logLevel,
        failure.map {
          "StreamingPull failed (reconnecting in \(delay)\(context)): \($0)\(Self.hint(for: $0))"
        } ?? "StreamingPull ended (reconnecting in \(delay))")

      do {
        try await sleep(now().advanced(by: delay))
      } catch {
        return
      }
    }
  }

  /// The transport reports a response exceeding the message size limit only as a decoding failure.
  private static func hint(for error: any Error) -> String {
    guard let error = error as? RPCError, error.code == .internalError,
      error.message == "Failed to decode message"
    else {
      return ""
    }
    return " (possibly a response larger than the limit of \(PubSubService.maxMessageBytes) bytes)"
  }

  private func stream(
    on connection: any StreamingPullConnection, into inventory: MessageInventory, ackBatcher: AckBatcher<ContinuousClock>, session: Session,
    attempt: Attempt
  ) async throws {
    let (requests, control) = AsyncStream.makeStream(of: Google_Pubsub_V1_StreamingPullRequest.self)
    defer { control.finish() }
    let initialRequest = Google_Pubsub_V1_StreamingPullRequest.with {
      $0.subscription = subscriptionID
      $0.streamAckDeadlineSeconds = inventory.ackDeadline.seconds
      $0.clientID = clientID
      $0.maxOutstandingMessages = Int64(flowControl.maxOutstandingMessages)
      $0.maxOutstandingBytes = Int64(flowControl.maxOutstandingBytes)
    }
    var options = CallOptions.defaults
    // Set per call, since the client's service config gives every method a 60 second timeout.
    options.timeout = streamTimeout
    // Fails the stream while the channel can't connect instead of waiting up to the timeout, so
    // that outages are retried and reported like other failures.
    options.waitForReady = false

    try await connection.streamingPull(
      metadata: ["x-goog-request-params": "subscription=\(subscriptionID)"],
      options: options,
      requestProducer: { writer in
        attempt.markEstablished(at: now())
        try await writer.write(initialRequest)
        for await request in requests {
          try await writer.write(request)
        }
      },
      onResponse: { response in
        try await withThrowingDiscardingTaskGroup { group in
          group.addTask {
            await self.sendKeepAlives(to: control)
          }
          for try await response in response.messages {
            attempt.markDataReceived()
            self.apply(response, to: inventory, control: control, session: session)
            await self.receive(response.receivedMessages, into: inventory, ackBatcher: ackBatcher)
            try await inventory.waitUntilReceivable()
          }
          group.cancelAll()
        }
      })
  }

  private func sendKeepAlives(
    to control: AsyncStream<Google_Pubsub_V1_StreamingPullRequest>.Continuation
  ) async {
    while true {
      do {
        try await sleep(now().advanced(by: keepAliveInterval))
      } catch {
        return
      }
      control.yield(Google_Pubsub_V1_StreamingPullRequest())
    }
  }

  private func apply(
    _ response: Google_Pubsub_V1_StreamingPullResponse, to inventory: MessageInventory,
    control: AsyncStream<Google_Pubsub_V1_StreamingPullRequest>.Continuation, session: Session
  ) {
    guard response.hasSubscriptionProperties else {
      return
    }
    let properties = response.subscriptionProperties
    if inventory.setExactlyOnceDelivery(properties.exactlyOnceDeliveryEnabled) {
      logger.debug(
        "Exactly-once delivery is \(properties.exactlyOnceDeliveryEnabled ? "on" : "off")")
      let ackDeadline = inventory.ackDeadline
      control.yield(.with { $0.streamAckDeadlineSeconds = ackDeadline.seconds })
    }
    if properties.messageOrderingEnabled, session.takeOrderingWarning() {
      logger.warning(
        "Message ordering is enabled, but ordering keys are not supported yet: messages with the same key may be handled concurrently and out of order."
      )
    }
  }

  /// Leases the messages and tells Pub/Sub they arrived by extending their deadline to the one
  /// leased here.
  private func receive(
    _ messages: [Google_Pubsub_V1_ReceivedMessage], into inventory: MessageInventory,
    ackBatcher: AckBatcher<ContinuousClock>
  ) async {
    guard !messages.isEmpty else {
      return
    }
    guard inventory.exactlyOnceDelivery else {
      let received = inventory.add(messages, now: now())
      ackBatcher.enqueueModifyDeadlines(received.accepted, to: received.ackDeadline)
      ackBatcher.enqueueNacks(received.rejected)
      return
    }
    let confirmed = await confirmReceipt(
      of: messages, ackDeadline: inventory.ackDeadline, ackBatcher: ackBatcher)
    let received = inventory.add(confirmed, now: now())
    ackBatcher.enqueueNacks(received.rejected)
  }

  /// With exactly-once delivery, a message is only handled once extending its deadline succeeded,
  /// since its ack would fail otherwise. Messages whose extension failed are dropped without a
  /// nack.
  ///
  /// A failed RPC fails every message in it. Telling apart the IDs that failed from the error
  /// details is not supported yet.
  private func confirmReceipt(
    of messages: [Google_Pubsub_V1_ReceivedMessage], ackDeadline: AckDeadline,
    ackBatcher: AckBatcher<ContinuousClock>
  ) async -> [Google_Pubsub_V1_ReceivedMessage] {
    let failed = await withTaskGroup(of: (AckID, (any Error)?).self) { group in
      for message in messages {
        let id = AckID(message.ackID)
        group.addTask {
          do {
            try await ackBatcher.modifyDeadline(id, to: ackDeadline)
            return (id, nil)
          } catch {
            return (id, error)
          }
        }
      }
      var failed: [AckID: any Error] = [:]
      for await (id, error) in group {
        failed[id] = error
      }
      return failed
    }
    if let error = failed.values.first {
      logger.warning(
        "Dropped \(failed.count) messages whose receipt could not be confirmed: \(error)")
    }
    return messages.filter { failed[AckID($0.ackID)] == nil }
  }
}

extension StreamingPullReceiver {

  /// State shared by all streams of one run.
  private final class Session: Sendable {

    private let hasWarnedAboutOrdering = Mutex(false)

    /// - Returns: Whether the ordering warning should be logged, which is only the first time.
    func takeOrderingWarning() -> Bool {
      hasWarnedAboutOrdering.withLock { hasWarned in
        defer { hasWarned = true }
        return !hasWarned
      }
    }
  }

  private final class Attempt: Sendable {

    private struct State {
      var establishedAt: ContinuousClock.Instant?
      var hasReceivedData = false
    }

    private let state = Mutex(State())

    /// Called once the stream was opened on a connection. Response headers can't tell, since
    /// Pub/Sub may only send them with the first response.
    func markEstablished(at instant: ContinuousClock.Instant) {
      state.withLock { $0.establishedAt = instant }
    }

    func markDataReceived() {
      state.withLock { $0.hasReceivedData = true }
    }

    /// Whether the stream received data or stayed open for at least `minimumDuration` before
    /// ending at `end`.
    func wasHealthy(at end: ContinuousClock.Instant, minimumDuration: Duration) -> Bool {
      state.withLock { state in
        state.hasReceivedData
          || state.establishedAt.map { $0.duration(to: end) >= minimumDuration } == true
      }
    }
  }

  /// Streams failing since the last healthy one ended, or since receiving started.
  struct Outage {

    let since: ContinuousClock.Instant
    var failures = 0

    /// Lowest level failures are logged at: transient failures are routine, so they are only
    /// reported once they repeat or persist.
    func logLevel(at now: ContinuousClock.Instant) -> Logger.Level {
      let duration = since.duration(to: now)
      if duration >= .seconds(600) {
        return .error
      }
      if failures >= 5 || duration >= .seconds(60) {
        return .warning
      }
      return .trace
    }

    func description(at now: ContinuousClock.Instant) -> String {
      "failure \(failures) without a healthy stream for \(since.duration(to: now))"
    }
  }
}

// MARK: - Retry policy

/// How a StreamingPull stream is reopened after it ended.
struct StreamingPullRetry: Hashable, Sendable {

  enum Delay: Hashable, Sendable {

    /// The initial backoff delay, after which the backoff starts over.
    case initial

    /// The next backoff delay, which grows until a stream is healthy.
    case increasing

    /// The maximum backoff delay, for errors that are unlikely to resolve soon.
    case maximum
  }

  let delay: Delay
  var createsSubscription = false

  /// Level the retry is logged at, or the lowest one if it `escalates`.
  let logLevel: Logger.Level

  /// Whether the retry counts as a failure whose log level rises while streams keep failing.
  var escalates = false
}

enum StreamingPullRetryPolicy {

  /// - Parameter error: Why the stream ended, or `nil` if the server closed it.
  /// - Returns: `nil` if receiving should stop because the task was cancelled or the gRPC clients
  ///   have shut down.
  static func retry(after error: (any Error)?, isCancelled: Bool) -> StreamingPullRetry? {
    guard !isCancelled else {
      return nil
    }
    guard let error else {
      return StreamingPullRetry(delay: .initial, logLevel: .debug)
    }
    if let error = error as? RuntimeError, error.code == .clientIsStopped {
      return nil
    }
    if error is CancellationError || isWrappedCancellation(error) {
      // The stream's own timeout cancels it, which the transport reports as an unknown error
      // until response headers arrived.
      return StreamingPullRetry(delay: .initial, logLevel: .debug)
    }
    if case .ioOnClosedChannel? = error as? ChannelError {
      return StreamingPullRetry(delay: .increasing, logLevel: .debug, escalates: true)
    }
    guard let error = error as? RPCError else {
      return StreamingPullRetry(delay: .increasing, logLevel: .warning, escalates: true)
    }
    switch error.code {
    case .deadlineExceeded:
      return StreamingPullRetry(delay: .initial, logLevel: .debug)
    case .unavailable:
      #if DEBUG
        // The emulator commonly closes connections with unavailable, so avoid flooding the logs.
        return StreamingPullRetry(delay: .increasing, logLevel: .trace, escalates: true)
      #else
        return StreamingPullRetry(delay: .increasing, logLevel: .debug, escalates: true)
      #endif
    case .resourceExhausted, .aborted, .internalError, .unknown, .cancelled:
      // Including connections closed with GOAWAY or RST_STREAM.
      return StreamingPullRetry(delay: .increasing, logLevel: .debug, escalates: true)
    case .unauthenticated:
      // An access token can expire between being attached and the stream being accepted.
      return StreamingPullRetry(delay: .increasing, logLevel: .debug, escalates: true)
    case .notFound:
      return StreamingPullRetry(delay: .increasing, createsSubscription: true, logLevel: .warning)
    default:
      // Including permission and configuration errors. Retrying keeps one misconfigured
      // subscription from stopping every other service in its service group.
      return StreamingPullRetry(delay: .maximum, logLevel: .error)
    }
  }

  private static func isWrappedCancellation(_ error: any Error) -> Bool {
    guard let error = error as? RPCError, error.code == .unknown else {
      return false
    }
    return error.cause is CancellationError
  }
}
