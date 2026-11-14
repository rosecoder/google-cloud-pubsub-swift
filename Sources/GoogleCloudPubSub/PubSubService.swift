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

  /// Time the clients keep running after their last user was a ``Use/call``.
  nonisolated let lingerDuration: Duration

  /// Largest gRPC message sent or received, as in the Go client. Pub/Sub accepts publish requests
  /// of up to 10 MB and StreamingPull responses batch several messages, so the transport's 4 MiB
  /// default would reject them.
  ///
  /// The HTTP/2 transport limits received messages by `maxRequestMessageBytes`, so both limits
  /// are set.
  static let maxMessageBytes = 20 * 1024 * 1024

  /// - Parameter maxStreamsPerConnection: Streaming pulls per HTTP/2 connection, below the 100
  ///   concurrent streams Google front ends allow. Unary RPCs use a connection of their own.
  /// - Parameter emulatorHost: `host:port` of a Pub/Sub emulator to use instead of Google Cloud.
  /// - Parameter callTimeout: Time after which an RPC is cancelled.
  init(
    maxStreamsPerConnection: Int = 80,
    lingerDuration: Duration = .seconds(30),
    channelDebuggingCallbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks = .defaults,
    emulatorHost: String? = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"],
    callTimeout: Duration = .seconds(60)
  ) throws {
    self.lingerDuration = lingerDuration
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
          channelDebuggingCallbacks: channelDebuggingCallbacks,
          callTimeout: callTimeout)
      }
    }
  }

  private static func makeClient(
    target: ResolvableTargets.DNS,
    transportSecurity: HTTP2ClientTransport.Posix.TransportSecurity,
    interceptors: [any ClientInterceptor],
    channelDebuggingCallbacks: HTTP2ClientTransport.Config.ChannelDebuggingCallbacks,
    callTimeout: Duration
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
              timeout: callTimeout,
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
    /// no connection carries more streams than the server allows, and none on the unary client so
    /// that acknowledgements never wait for a stream slot.
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
    var unary: GRPCClient<HTTP2ClientTransport.Posix> {
      pool.unary
    }

    /// For unary RPCs.
    var publisher: Google_Pubsub_V1_Publisher.Client<HTTP2ClientTransport.Posix> {
      .init(wrapping: pool.unary)
    }

    /// For unary RPCs. Streaming pulls should use a client of ``GRPCClientPool/withStreamSlot(_:)``.
    var subscriber: Google_Pubsub_V1_Subscriber.Client<HTTP2ClientTransport.Posix> {
      .init(wrapping: pool.unary)
    }
  }

  /// How long a use of the clients lasts, which decides what happens when it is their last user.
  enum Use: Sendable {

    /// Runs until its service shuts down. The clients shut down right away after the last one, so
    /// that the process can exit.
    case service

    /// A few RPCs. The clients keep running for ``PubSubService/lingerDuration`` after the last
    /// one, so that consecutive calls reuse their connections.
    case call
  }

  /// Counts the users of the current generation of clients. The first user after the previous
  /// generation began shutting down starts a new one without waiting for it to stop.
  struct ClientUsage<Generation: Sendable>: Sendable {
    private(set) var users = 0
    private(set) var current: Generation?
    private(set) var startedGenerations = 0

    /// Identifies the linger of the current generation while no user holds it.
    private(set) var linger: Int?
    private var lingerCount = 0

    /// Whether the last generation has begun shutting down and no new one has been started.
    var hasBegunShutdown: Bool {
      startedGenerations > 0 && current == nil
    }

    var isLingering: Bool {
      linger != nil
    }

    /// Ends a linger, so that a pending ``expireLinger(_:)`` does nothing.
    ///
    /// - Returns: The current generation, created with `start` if there is none.
    mutating func retain(startingWith start: () throws -> Generation) rethrows -> Generation {
      if let current {
        users += 1
        linger = nil
        return current
      }
      let generation = try start()
      current = generation
      startedGenerations += 1
      users += 1
      return generation
    }

    enum Release {

      /// Other users still hold the generation.
      case keep

      case shutDown(Generation)

      /// The generation keeps running until ``ClientUsage/expireLinger(_:)`` is called with the
      /// identifier, unless a user arrives before.
      case linger(Int)
    }

    mutating func release(_ use: Use) -> Release {
      users -= 1
      guard users == 0, let current else {
        return .keep
      }
      switch use {
      case .service:
        self.current = nil
        return .shutDown(current)
      case .call:
        lingerCount += 1
        linger = lingerCount
        return .linger(lingerCount)
      }
    }

    /// - Returns: The generation to shut down, unless a user arrived since the linger began.
    mutating func expireLinger(_ id: Int) -> Generation? {
      guard linger == id, let current else {
        return nil
      }
      linger = nil
      self.current = nil
      return current
    }
  }

  private struct State {
    var usage = ClientUsage<Clients>()

    /// Lingers before stopping a generation last used by a call, then stops it. Awaits the one
    /// before, so awaiting the latest awaits every generation stopped in the background.
    var background: Task<Void, Never>?
  }

  private nonisolated let state = Mutex(State())

  /// Runs the current generation of clients while `body` runs, sharing it with concurrent callers,
  /// or starts a new one if there is none. Every RPC runs in such a use, so the clients stop only
  /// once none is in flight.
  ///
  /// Returns as soon as `body` has. If this was the last caller and a ``Use/service``, it then stops
  /// the clients, and those stopping in the background, before returning. After the last
  /// ``Use/call``, the clients keep running for ``lingerDuration`` and then stop in the background.
  ///
  /// - Parameter body: Must not use the clients after returning.
  /// - Throws: Without running `body` if new clients couldn't be created.
  nonisolated func withClientInUse<T: Sendable>(
    _ use: Use = .call,
    _ body: (Clients) async throws -> T
  ) async throws -> T {
    let (clients, endedLinger) = try state.withLock { state in
      let wasLingering = state.usage.isLingering
      let clients = try state.usage.retain { Clients(pool: try makePool()) }
      return (clients, wasLingering ? state.background : nil)
    }
    endedLinger?.cancel()

    let result: Result<T, any Error>
    do {
      result = .success(try await body(clients))
    } catch {
      result = .failure(error)
    }

    let (stopping, background) = state.withLock { state -> (Clients?, Task<Void, Never>?) in
      switch state.usage.release(use) {
      case .keep:
        return (nil, nil)
      case .shutDown(let stopping):
        return (stopping, state.background)
      case .linger(let id):
        state.background = lingerThenStop(id, after: state.background)
        return (nil, nil)
      }
    }
    if let stopping {
      do {
        try await stop(stopping)
      } catch {
        _ = try result.get()
        throw error
      }
      await background?.value
    }
    return try result.get()
  }

  /// Stops the current generation after ``lingerDuration`` unless a user arrives before, which
  /// cancels the returned task.
  private nonisolated func lingerThenStop(
    _ id: Int, after previous: Task<Void, Never>?
  ) -> Task<Void, Never> {
    Task {
      try? await Task.sleep(for: lingerDuration)
      if let expired = state.withLock({ $0.usage.expireLinger(id) }) {
        do {
          try await stop(expired)
        } catch {
          Self.logger.error("The gRPC clients failed after lingering: \(error)")
        }
      }
      await previous?.value
    }
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
    state.withLock { $0.usage }
  }

  /// Number of active streams per streaming client of the current generation, in creation order.
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
