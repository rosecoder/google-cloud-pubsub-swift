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
  nonisolated let grpcClient: GRPCClient<HTTP2ClientTransport.Posix>
  nonisolated let isUsingEmulator: Bool

  init() throws {
    if let host = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"] {
      let components = host.components(separatedBy: ":")
      let port = Int(components[1])!

      self.authorization = nil
      self.grpcClient = GRPCClient(
        transport: try .http2NIOPosix(
          target: .dns(host: components[0], port: port),
          transportSecurity: .plaintext,
          config: .defaults { config in
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
                timeout: .seconds(60)
              )
            ]
          )
        )
      )
      self.isUsingEmulator = true
    } else {
      let authorization = Authorization(
        scopes: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/pubsub",
        ], eventLoopGroup: .singletonMultiThreadedEventLoopGroup)

      self.authorization = authorization
      self.grpcClient = GRPCClient(
        transport: try .http2NIOPosix(
          target: .dns(host: "pubsub.googleapis.com"),
          transportSecurity: .tls,
          config: .defaults { config in
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
                timeout: .seconds(60)
              )
            ]
          )
        ),
        interceptors: [
          AuthorizationClientInterceptor(authorization: authorization)
        ]
      )
      self.isUsingEmulator = false
    }
  }

  var runTask: Task<Void, Error>?

  func run() async throws {
    if let runTask {
      try await runTask.value
      return
    }
    let task = Task {
      try await withTaskCancellationOrGracefulShutdownHandler {
        try await grpcClient.runConnections()
      } onCancelOrGracefulShutdown: {
        Task {
          await self.waitForBlockingTasks()
          self.grpcClient.beginGracefulShutdown()
        }
      }
      try await authorization?.shutdown()
    }
    runTask = task
    try await withTaskCancellationHandler {
      try await task.value
    } onCancel: {
      task.cancel()
    }
  }

  private var grpcBlockerTasks = [Task<Void, Never>]()

  func registerBlockerForGRPCShutdown(task: Task<Void, Never>) {
    grpcBlockerTasks.append(task)
  }

  func waitForBlockingTasks() async {
    for task in grpcBlockerTasks {
      await task.value
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
