import Foundation
import GoogleCloudPubSub

struct InjectedFailure: Error, CustomStringConvertible {
  var description: String { "Injected handler failure" }
}

struct MissingHeader: Error, CustomStringConvertible {
  var description: String { "Message without stress attributes" }
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
      // Busy work on the cooperative pool without suspending, like a CPU-bound handler.
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
    guard let header = MessageHeader(message.attributes) else {
      throw MissingHeader()
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

func makeSubscribers(config: Config, resources: Resources, trackers: [SubscriptionTracker]) throws
  -> [PullSubscriber<StressHandler>]
{
  let behavior = HandlerBehavior(config)
  let options = PullSubscriberOptions(
    flowControl: .init(
      maxOutstandingMessages: config.maxOutstandingMessages,
      maxOutstandingBytes: config.maxOutstandingBytes),
    shutdownBehavior: config.nackImmediately
      ? .nackImmediately : .waitForProcessing(timeout: .seconds(config.shutdownTimeout)))
  return try zip(resources.subscriptions, trackers).map { subscription, tracker in
    try PullSubscriber(
      handler: StressHandler(subscription: subscription, tracker: tracker, behavior: behavior),
      projectID: config.projectID,
      options: options)
  }
}
