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

  private nonisolated let makePool:
    @Sendable () throws -> GRPCClientPool<GRPCClient<HTTP2ClientTransport.Posix>>
  nonisolated let isUsingEmulator: Bool

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
      self.isUsingEmulator = true
    } else {
      // Shared by every generation of clients and never shut down, since up to
      // google-cloud-auth-swift 1.2.3 that also shuts down the process-wide default provider,
      // even one set with `AuthorizationSystem.bootstrap(_:)`, which then can't be used again.
      let authorization = Authorization(
        scopes: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/pubsub",
        ], eventLoopGroup: .singletonMultiThreadedEventLoopGroup)

      target = .dns(host: "pubsub.googleapis.com")
      transportSecurity = .tls
      interceptors = [AuthorizationClientInterceptor(authorization: authorization)]
      self.isUsingEmulator = false
    }
    self.makePool = {
      try GRPCClientPool(maxStreamsPerClient: maxStreamsPerConnection) {
        try Self.makeClient(
          target: target,
          transportSecurity: transportSecurity,
          interceptors: interceptors,
          channelDebuggingCallbacks: channelDebuggingCallbacks)
      }
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

  /// One generation of clients, started for the first user and shut down after the last one, since
  /// a `GRPCClient` can't run again once stopped.
  struct Clients: Sendable {

    /// Clients each with their own HTTP/2 connection. Streaming pulls take a stream slot so that
    /// no connection carries more streams than the server allows.
    let pool: GRPCClientPool<GRPCClient<HTTP2ClientTransport.Posix>>

    fileprivate let run: Task<Void, any Error>

    fileprivate init(pool: GRPCClientPool<GRPCClient<HTTP2ClientTransport.Posix>>) {
      self.pool = pool
      self.run = Task {
        // Also returns when clients were shut down before their connections started running.
        try await pool.run()
      }
    }

    /// Client for unary RPCs.
    var primary: GRPCClient<HTTP2ClientTransport.Posix> {
      pool.primary
    }

    var publisher: Google_Pubsub_V1_Publisher.Client<HTTP2ClientTransport.Posix> {
      .init(wrapping: pool.primary)
    }

    var subscriber: Google_Pubsub_V1_Subscriber.Client<HTTP2ClientTransport.Posix> {
      .init(wrapping: pool.primary)
    }
  }

  /// Counts the users of the current generation of clients. The first user after the previous
  /// generation began shutting down starts a new one without waiting for it to stop.
  struct ClientUsage<Generation: Sendable>: Sendable {
    private(set) var users = 0
    private(set) var current: Generation?
    private(set) var startedGenerations = 0

    /// Whether the last generation has begun shutting down and no new one has been started.
    var hasBegunShutdown: Bool {
      startedGenerations > 0 && current == nil
    }

    /// - Returns: The current generation, created with `start` if there is none.
    mutating func retain(startingWith start: () throws -> Generation) rethrows -> Generation {
      if let current {
        users += 1
        return current
      }
      let generation = try start()
      current = generation
      startedGenerations += 1
      users += 1
      return generation
    }

    /// - Returns: The generation to shut down if this was its last user.
    mutating func release() -> Generation? {
      users -= 1
      guard users == 0 else {
        return nil
      }
      defer { current = nil }
      return current
    }
  }

  private nonisolated let usage = Mutex(ClientUsage<Clients>())

  /// Runs the current generation of clients while `body` runs, sharing it with concurrent callers,
  /// or starts a new one if there is none. Every RPC runs in such a use, so the clients stop only
  /// once none is in flight.
  ///
  /// Returns as soon as `body` has, unless this was the last caller: it then stops the clients
  /// before returning.
  ///
  /// - Parameter body: Must not use the clients after returning.
  /// - Throws: Without running `body` if new clients couldn't be created.
  nonisolated func withClientInUse<T: Sendable>(
    _ body: (Clients) async throws -> T
  ) async throws -> T {
    let clients = try usage.withLock { usage in
      try usage.retain { Clients(pool: try makePool()) }
    }

    let result: Result<T, any Error>
    do {
      result = .success(try await body(clients))
    } catch {
      result = .failure(error)
    }

    if let stopping = usage.withLock({ $0.release() }) {
      do {
        try await stop(stopping)
      } catch {
        _ = try result.get()
        throw error
      }
    }
    return try result.get()
  }

  /// Rejects new RPCs and cancels the clients' connections rather than waiting for the transport's
  /// graceful shutdown, which also waits for connection attempts in progress: up to the connect
  /// timeout, or indefinitely if the server never completes the HTTP/2 handshake.
  private nonisolated func stop(_ clients: Clients) async throws {
    clients.pool.beginGracefulShutdown()
    clients.run.cancel()
    if case .failure(let error) = await clients.run.result, !(error is CancellationError) {
      throw error
    }
  }

  nonisolated var clientUsage: ClientUsage<Clients> {
    usage.withLock { $0 }
  }

  /// Number of active streams per client of the current generation, in creation order.
  nonisolated var activeStreamCounts: [Int] {
    clientUsage.current?.pool.activeStreamCounts ?? []
  }

  /// Creates `subscription`, and its topic if needed, unless it exists.
  nonisolated func create<Message: _Message>(subscription: Subscription<Message>, projectID: String)
    async throws
  {
    try await withClientInUse { clients in
      try await self.create(
        subscription: subscription,
        subscriberClient: clients.subscriber,
        publisherClient: clients.publisher,
        projectID: projectID)
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
        let request = Google_Pubsub_V1_Topic.with {
          $0.name = topic.id(projectID: projectID)
          $0.labels = topic.labels
        }
        _ = try await publisherClient.createTopic(request)
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
        _ = try await subscriberClient.createSubscription(request)
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

  nonisolated func delete<Message: _Message>(subscription: Subscription<Message>, projectID: String)
    async throws
  {
    try await withClientInUse { clients in
      try await self.delete(
        subscription: subscription, subscriberClient: clients.subscriber, projectID: projectID)
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
    try await withRetryableTask {
      do {
        try await withSpan(
          "pubsub-delete-subscription",
          ofKind: .producer
        ) { span in
          span.attributes["pubsub/subscription"] = subscription.name
          _ = try await subscriberClient.deleteSubscription(request)
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
