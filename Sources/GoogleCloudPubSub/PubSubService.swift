import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix
import GoogleCloudAuth
import GoogleCloudAuthGRPC
import ServiceLifecycle
import Synchronization

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
          transportSecurity: .plaintext
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
          transportSecurity: .tls
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
      try await withGracefulShutdownHandler {
        try await grpcClient.runConnections()
      } onGracefulShutdown: {
        Task {
          await self.waitForBlockingTasks()
          self.grpcClient.beginGracefulShutdown()
        }
      }
      try await authorization?.shutdown()
    }
    runTask = task
    try await task.value
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
}
