import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import GoogleCloudAuth
import GoogleCloudAuthGRPC
import Logging
import RetryableTask
import Synchronization
import Tracing

actor PubSubService {

  private static let _shared = Mutex<PubSubService?>(nil)

  private static let logger = Logger(label: "pubsub.service")

  static var shared: PubSubService {
    get throws {
      try _shared.withLock {
        if let shared = $0 {
          return shared
        }
        let shared = try PubSubService()
        $0 = shared
        return shared
      }
    }
  }

  private nonisolated let authorization: Authorization?

  /// Clients each with their own HTTP/2 connection. Streaming pulls take a stream slot so that no
  /// connection carries more streams than the server allows.
  nonisolated let pool: GRPCClientPool<GRPCClient<HTTP2ClientTransport.Posix>>
  nonisolated let isUsingEmulator: Bool

  /// Client for unary RPCs.
  nonisolated var grpcClient: GRPCClient<HTTP2ClientTransport.Posix> {
    pool.primary
  }

  /// Largest gRPC message sent or received, as in the Go client. Pub/Sub accepts publish requests
  /// of up to 10 MB and StreamingPull responses batch several messages, so the transport's 4 MiB
  /// default would reject them.
  ///
  /// The HTTP/2 transport limits received messages by `maxRequestMessageBytes`, so both limits
  /// are set.
  static let maxMessageBytes = 20 * 1024 * 1024

  /// - Parameter maxStreamsPerConnection: Streaming pulls per HTTP/2 connection, below the
  ///   ~100 concurrent streams Google front ends allow to leave room for unary RPCs.
  /// - Parameter emulatorHost: `host:port` of a Pub/Sub emulator to use instead of Google Cloud.
  init(
    maxStreamsPerConnection: Int = 80,
    channelDebuggingCallbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks = .defaults,
    emulatorHost: String? = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"]
  ) throws {
    let target: ResolvableTargets.DNS
    let transportSecurity: HTTP2ClientTransport.Posix.TransportSecurity
    let interceptors: [any ClientInterceptor]
    if let host = emulatorHost {
      let components = host.components(separatedBy: ":")
      let port = Int(components[1])!

      target = .dns(host: components[0], port: port)
      transportSecurity = .plaintext
      interceptors = []
      self.authorization = nil
      self.isUsingEmulator = true
    } else {
      let authorization = Authorization(
        scopes: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/pubsub",
        ], eventLoopGroup: .singletonMultiThreadedEventLoopGroup)

      target = .dns(host: "pubsub.googleapis.com")
      transportSecurity = .tls
      interceptors = [AuthorizationClientInterceptor(authorization: authorization)]
      self.authorization = authorization
      self.isUsingEmulator = false
    }
    self.pool = try GRPCClientPool(maxStreamsPerClient: maxStreamsPerConnection) {
      try Self.makeClient(
        target: target,
        transportSecurity: transportSecurity,
        interceptors: interceptors,
        channelDebuggingCallbacks: channelDebuggingCallbacks)
    }
  }

  private static func makeClient(
    target: ResolvableTargets.DNS,
    transportSecurity: HTTP2ClientTransport.Posix.TransportSecurity,
    interceptors: [any ClientInterceptor],
    channelDebuggingCallbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks
  ) throws -> GRPCClient<HTTP2ClientTransport.Posix> {
    GRPCClient(
      transport: try .http2NIOPosix(
        target: target,
        transportSecurity: transportSecurity,
        config: .defaults { config in
          config.channelDebuggingCallbacks = channelDebuggingCallbacks
          config.backoff = .init(
            initial: .milliseconds(100),
            max: .seconds(1),
            multiplier: 1.6,
            jitter: 0.2
          )
          config.connection = .init(
            maxIdleTime: .seconds(30 * 60),
            keepalive: .init(
              time: .seconds(30),
              timeout: .seconds(5),
              allowWithoutCalls: true
            )
          )
        },
        serviceConfig: .init(
          methodConfig: [
            .init(
              names: [.init(service: "")],  // Empty service means all methods
              waitForReady: true,
              timeout: .seconds(60),
              maxRequestMessageBytes: maxMessageBytes,
              maxResponseMessageBytes: maxMessageBytes
            )
          ]
        )
      ),
      interceptors: interceptors
    )
  }

  struct ClientUsage: Sendable {
    private(set) var users = 0
    private(set) var hasBegunShutdown = false

    mutating func retain() throws {
      if hasBegunShutdown {
        throw RuntimeError(
          code: .clientIsStopped,
          message: "The Pub/Sub clients shut down when their last user stopped and can't be reused."
        )
      }
      users += 1
    }

    /// - Returns: Whether the clients should begin shutting down now.
    mutating func release() -> Bool {
      users -= 1
      hasBegunShutdown = users == 0
      return hasBegunShutdown
    }
  }

  private struct State {
    var usage = ClientUsage()
    var run: Task<Void, any Error>?
    var rpcsInFlight = 0
    var rpcsFinished: CheckedContinuation<Void, Never>?
  }

  private nonisolated let state = Mutex(State())

  /// Runs the clients' connections while `body` runs, sharing them with concurrent callers.
  ///
  /// Returns as soon as `body` has, unless this was the last caller: it then lets RPCs in flight
  /// finish, for at most ``shutdownTimeout``, and stops the clients before returning. Cancelling
  /// the last caller stops them right away.
  ///
  /// - Throws: `RuntimeError` with code `clientIsStopped` without running `body` if the clients
  ///   have already shut down.
  nonisolated func withClientInUse<T: Sendable>(
    _ body: @Sendable () async throws -> T
  ) async throws -> T {
    let run = try state.withLock { state in
      try state.usage.retain()
      if state.run == nil {
        state.run = Task { [pool, authorization] in
          // Also returns when clients were shut down before their connections started running.
          try await pool.run()
          try await authorization?.shutdown()
        }
      }
      return state.run!
    }

    let result: Result<T, any Error>
    do {
      result = .success(try await body())
    } catch {
      result = .failure(error)
    }

    if state.withLock({ $0.usage.release() }) {
      do {
        try await stopClients(run)
      } catch {
        _ = try result.get()
        throw error
      }
    }
    return try result.get()
  }

  static var shutdownTimeout: Duration {
    .seconds(5)
  }

  /// Rejects new RPCs, waits for the ones in flight and then cancels `run` rather than waiting for
  /// the transport's graceful shutdown, which also waits for connection attempts in progress: up to
  /// the connect timeout, or indefinitely if the server never completes the HTTP/2 handshake.
  private nonisolated func stopClients(_ run: Task<Void, any Error>) async throws {
    pool.beginGracefulShutdown()
    let hasTimedOut = await withTaskGroup(of: Bool.self) { group in
      group.addTask {
        await self.waitUntilRPCsFinished()
        return false
      }
      group.addTask {
        try? await Task.sleep(for: Self.shutdownTimeout)
        return !Task.isCancelled
      }
      defer { group.cancelAll() }
      return await group.next()!
    }
    if hasTimedOut {
      Self.logger.warning(
        "Cancelled \(state.withLock { $0.rpcsInFlight }) RPCs still in flight \(Self.shutdownTimeout) after the last user of the gRPC clients stopped."
      )
    }
    run.cancel()
    if case .failure(let error) = await run.result, !(error is CancellationError) {
      throw error
    }
  }

  private nonisolated func waitUntilRPCsFinished() async {
    await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        let hasFinished = state.withLock { state in
          if state.rpcsInFlight == 0 || Task.isCancelled {
            return true
          }
          state.rpcsFinished = continuation
          return false
        }
        if hasFinished {
          continuation.resume()
        }
      }
    } onCancel: {
      state.withLock { state in
        defer { state.rpcsFinished = nil }
        return state.rpcsFinished
      }?.resume()
    }
  }

  /// Keeps the clients from being stopped while `body` runs an RPC, which may be started without
  /// holding them through ``withClientInUse(_:)``, such as a publish from outside a service.
  nonisolated func withRPC<T: Sendable>(_ body: @Sendable () async throws -> T) async rethrows -> T
  {
    state.withLock { $0.rpcsInFlight += 1 }
    defer {
      state.withLock { state -> CheckedContinuation<Void, Never>? in
        state.rpcsInFlight -= 1
        guard state.rpcsInFlight == 0 else {
          return nil
        }
        defer { state.rpcsFinished = nil }
        return state.rpcsFinished
      }?.resume()
    }
    return try await body()
  }

  nonisolated var rpcsInFlight: Int {
    state.withLock { $0.rpcsInFlight }
  }

  nonisolated var clientUsage: ClientUsage {
    state.withLock { $0.usage }
  }

  func create(
    topic: Topic<some _Message>,
    publisherClient: Google_Pubsub_V1_Publisher.ClientProtocol,
    projectID: String
  ) async throws {
    do {
      try await withSpan(
        "pubsub-create-topic",
        ofKind: .producer
      ) { span in
        span.attributes["pubsub/topic"] = topic.name
        let request = Google_Pubsub_V1_Topic.with {
          $0.name = topic.id(projectID: projectID)
          $0.labels = topic.labels
        }
        _ = try await withRPC { try await publisherClient.createTopic(request) }
      }
    } catch let error as RPCError where error.code == .alreadyExists {
      // pass
    }
  }

  func create<Message: _Message>(
    subscription: Subscription<Message>,
    createTopicIfNeeded: Bool = true,
    subscriberClient: Google_Pubsub_V1_Subscriber.ClientProtocol,
    publisherClient: Google_Pubsub_V1_Publisher.ClientProtocol,
    projectID: String
  ) async throws {
    do {
      try await withSpan(
        "pubsub-create-subscription",
        ofKind: .producer
      ) { span in
        span.attributes["pubsub/subscription"] = subscription.name
        let request = Google_Pubsub_V1_Subscription.with {
          $0.name = subscription.id(projectID: projectID)
          $0.labels = subscription.labels
          $0.topic = subscription.topic.id(projectID: projectID)
          $0.ackDeadlineSeconds = Int32(subscription.acknowledgeDeadline)
          $0.retainAckedMessages = subscription.retainAcknowledgedMessages
          $0.messageRetentionDuration = .with {
            $0.seconds = Int64(subscription.messageRetentionDuration)
          }
          $0.expirationPolicy = .with {
            $0.ttl = .with {
              $0.seconds = Int64(subscription.expirationPolicyDuration)
            }
          }
          if let deadLetterPolicy = subscription.deadLetterPolicy {
            $0.deadLetterPolicy = .with {
              $0.deadLetterTopic = deadLetterPolicy.topic.id(projectID: projectID)
              $0.maxDeliveryAttempts = deadLetterPolicy.maxDeliveryAttempts
            }
          }
        }
        _ = try await withRPC { try await subscriberClient.createSubscription(request) }
      }
    } catch let error as RPCError {
      switch error.code {
      case .alreadyExists:
        break  // pass
      case .notFound:
        if !createTopicIfNeeded {
          throw error
        }
        try await create(
          topic: subscription.topic,
          publisherClient: publisherClient,
          projectID: projectID
        )
        try await create(
          subscription: subscription,
          createTopicIfNeeded: false,
          subscriberClient: subscriberClient,
          publisherClient: publisherClient,
          projectID: projectID
        )
      default:
        throw error
      }
    }
  }

  func delete<Message: _Message>(
    subscription: Subscription<Message>,
    subscriberClient: Google_Pubsub_V1_Subscriber.ClientProtocol,
    projectID: String
  ) async throws {
    let request = Google_Pubsub_V1_DeleteSubscriptionRequest.with {
      $0.subscription = subscription.id(projectID: projectID)
    }
    try await withRetryableTask { [service = self] in
      do {
        try await withSpan(
          "pubsub-delete-subscription",
          ofKind: .producer
        ) { span in
          span.attributes["pubsub/subscription"] = subscription.name
          _ = try await service.withRPC {
            try await subscriberClient.deleteSubscription(request)
          }
        }
      } catch let error as RPCError {
        switch error.code {
        case .notFound:
          break  // pass
        default:
          throw error
        }
      }
    }
  }
}
