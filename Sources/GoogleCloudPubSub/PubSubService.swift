import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import GoogleCloudAuth
import GoogleCloudAuthGRPC
import RetryableTask
import ServiceLifecycle
import Synchronization
import Tracing

actor PubSubService {

  private static let _shared = Mutex<PubSubService?>(nil)

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
  init(
    maxStreamsPerConnection: Int = 80,
    channelDebuggingCallbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks = .defaults
  ) throws {
    let target: ResolvableTargets.DNS
    let transportSecurity: HTTP2ClientTransport.Posix.TransportSecurity
    let interceptors: [any ClientInterceptor]
    if let host = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"] {
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
    private(set) var isShutdownRequested = false
    private(set) var hasBegunShutdown = false

    mutating func retain() {
      users += 1
    }

    /// - Returns: Whether the client should begin shutting down now.
    mutating func release() -> Bool {
      users -= 1
      return shouldShutDown()
    }

    /// - Returns: Whether the client should begin shutting down now.
    mutating func requestShutdown() -> Bool {
      isShutdownRequested = true
      return shouldShutDown()
    }

    private mutating func shouldShutDown() -> Bool {
      guard isShutdownRequested, users == 0, !hasBegunShutdown else {
        return false
      }
      hasBegunShutdown = true
      return true
    }
  }

  private nonisolated let usage = Mutex(ClientUsage())

  var runTask: Task<Void, Error>?

  /// Runs the connections of every pooled client until a caller of `run()` is cancelled or shut
  /// down gracefully and nothing uses the clients through ``withClientInUse(_:)`` anymore.
  func run() async throws {
    if runTask == nil {
      let task = Task {
        // Also returns when clients were shut down before their connections started running.
        try await pool.run()
        try await authorization?.shutdown()
      }
      runTask = task
    }
    let task = runTask!

    try await withTaskCancellationOrGracefulShutdownHandler {
      try await task.value
    } onCancelOrGracefulShutdown: {
      self.requestShutdown()
    }
  }

  /// Keeps the gRPC clients from shutting down while `body` runs.
  nonisolated func withClientInUse<T: Sendable>(
    _ body: @Sendable () async throws -> T
  ) async rethrows -> T {
    usage.withLock { $0.retain() }
    defer {
      if usage.withLock({ $0.release() }) {
        pool.beginGracefulShutdown()
      }
    }
    return try await body()
  }

  nonisolated var clientUsage: ClientUsage {
    usage.withLock { $0 }
  }

  private nonisolated func requestShutdown() {
    if usage.withLock({ $0.requestShutdown() }) {
      pool.beginGracefulShutdown()
    }
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
        _ = try await publisherClient.createTopic(
          .with {
            $0.name = topic.id(projectID: projectID)
            $0.labels = topic.labels
          })
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
        _ = try await subscriberClient.createSubscription(
          .with {
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

          })
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
    try await withRetryableTask {
      do {
        try await withSpan(
          "pubsub-delete-subscription",
          ofKind: .producer
        ) { span in
          span.attributes["pubsub/subscription"] = subscription.name
          _ = try await subscriberClient.deleteSubscription(
            .with {
              $0.subscription = subscription.id(projectID: projectID)
            }
          )
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
