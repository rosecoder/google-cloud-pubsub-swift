import Foundation
import NIOCore
import ServiceLifecycle
import Synchronization

@testable import GoogleCloudPubSub

struct InjectedFailure: Error, CustomStringConvertible {
  var description: String { "Injected handler failure" }
}

struct HandlerBehavior: Sendable {
  let sleepMs: Double
  let sleepJitter: Double
  let cpuMs: Double
  let failRate: Double
  let slowRate: Double
  let slowMs: Double

  init(_ config: Config) {
    sleepMs = config.sleepMs
    sleepJitter = config.sleepJitter
    cpuMs = config.cpuMs
    failRate = config.failRate
    slowRate = config.slowRate
    slowMs = config.slowMs
  }

  func perform() async throws {
    if slowRate > 0, Double.random(in: 0..<1) < slowRate {
      try await Task.sleep(for: .milliseconds(slowMs))
    } else if sleepMs > 0 {
      let jitter = sleepMs * sleepJitter
      let duration = jitter > 0 ? Double.random(in: (sleepMs - jitter)...(sleepMs + jitter)) : sleepMs
      try await Task.sleep(for: .milliseconds(max(0, duration)))
    }
    if cpuMs > 0 {
      let end = monotonicNanos() + UInt64(cpuMs * 1e6)
      var value: UInt64 = 0
      while monotonicNanos() < end {
        for index in 0..<1_000 { value = value &* 6364136223846793005 &+ UInt64(index) }
      }
      if value == 42 { print("") }
    }
    if failRate > 0, Double.random(in: 0..<1) < failRate {
      throw InjectedFailure()
    }
  }
}

struct StressHandler: Handler {
  typealias Message = DataMessage

  let subscription: Subscription<DataMessage>
  let tracker: SubscriptionTracker
  let behavior: HandlerBehavior

  func handle(message: IncomingDataMessage, context: HandlerContext) async throws {
    guard let header = MessageHeader(message.body) else {
      throw InjectedFailure()
    }
    tracker.begin(header.seq, publishedAtNanos: header.publishedAtNanos)
    do {
      try await behavior.perform()
    } catch {
      tracker.end(header.seq, .failure)
      throw error
    }
    tracker.end(header.seq, .success)
  }
}

/// Runs every pull subscriber in one service, working around graceful shutdown hanging when
/// several subscribers are separate services of one ServiceGroup.
final class SubscriberGroup: Service {

  let subscribers: [PullSubscriber<StressHandler>]
  private let runningIndices = Mutex<Set<Int>>([])

  init(subscribers: [PullSubscriber<StressHandler>]) {
    self.subscribers = subscribers
  }

  /// Indices of subscribers whose `run()` has not returned yet.
  var running: [Int] {
    runningIndices.withLock { $0.sorted() }
  }

  func run() async throws {
    runningIndices.withLock { $0 = Set(subscribers.indices) }
    try await withThrowingDiscardingTaskGroup { group in
      for (index, subscriber) in subscribers.enumerated() {
        group.addTask {
          defer { self.runningIndices.withLock { _ = $0.remove(index) } }
          try await subscriber.run()
        }
      }
    }
  }
}

func makePubSubService(maxStreamsPerConnection: Int = 80) throws -> PubSubService {
  try PubSubService(
    maxStreamsPerConnection: maxStreamsPerConnection,
    channelDebuggingCallbacks: .init(
      onCreateTCPConnection: { channel in
        TransportStats.shared.recordTCPConnection()
        return channel.eventLoop.makeSucceededVoidFuture()
      },
      onCreateHTTP2Stream: { channel in
        TransportStats.shared.recordHTTP2Stream()
        return channel.eventLoop.makeSucceededVoidFuture()
      }))
}

func makeSubscribers(
  config: Config, resources: Resources, trackers: [SubscriptionTracker], service: PubSubService
) -> [PullSubscriber<StressHandler>] {
  let behavior = HandlerBehavior(config)
  let options = PullSubscriberOptions(
    flowControl: .init(
      maxOutstandingMessages: config.maxOutstandingMessages,
      maxOutstandingBytes: config.maxOutstandingBytes),
    shutdownBehavior: config.nackImmediately
      ? .nackImmediately : .waitForProcessing(timeout: .seconds(config.shutdownTimeout)))
  return zip(resources.subscriptions, trackers).map { subscription, tracker in
    let handler = StressHandler(subscription: subscription, tracker: tracker, behavior: behavior)
    let ackClient = Google_Pubsub_V1_Subscriber.Client(wrapping: service.grpcClient)
    return PullSubscriber(
      handler: handler,
      projectID: config.projectID,
      options: options,
      pubSubService: service,
      ackTransport: CountingAckTransport(
        inner: GRPCAckTransport(
          client: ackClient, subscriptionID: subscription.id(projectID: config.projectID))))
  }
}
