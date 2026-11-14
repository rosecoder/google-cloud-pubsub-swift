import Foundation
import GRPCCore
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite(.tags(.integration))
struct StreamingPullIntegrationTests {

  let projectID = "streaming-pull-integration-tests"

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveThousandMessages() async throws {
    let messageCount = 1_000
    let (handled, handledContinuation) = AsyncStream.makeStream(of: String.self)

    try await withSubscriber(handle: { handledContinuation.yield($0) }) { publish, _ in
      var published = Set<String>()
      for batch in stride(from: 0, to: messageCount, by: 100) {
        published.formUnion(try await publish((batch..<batch + 100).map { "Message \($0)" }))
      }

      var deliveries: [String: Int] = [:]
      for await id in handled {
        deliveries[id, default: 0] += 1
        if deliveries.count == messageCount {
          break
        }
      }
      #expect(Set(deliveries.keys) == published)
    }
  }

  @Test(.tags(.slow), .timeLimit(.minutes(1)))
  func shouldRedeliverNackedMessageRightAwayButNotAckedMessage() async throws {
    let deliveries = Mutex<[ContinuousClock.Instant]>([])
    let (handled, handledContinuation) = AsyncStream.makeStream(of: Void.self)

    try await withSubscriber(handle: { _ in
      let delivery = deliveries.withLock {
        $0.append(.now)
        return $0.count
      }
      if delivery == 1 {
        throw NackError()
      }
      handledContinuation.yield()
    }) { publish, _ in
      _ = try await publish(["Hello"])
      _ = await handled.first { _ in true }

      // An unacked message would be redelivered once its 10 second ack deadline has expired.
      try await Task.sleep(for: .seconds(12))
    }

    let times = deliveries.withLock { $0 }
    #expect(times.count == 2)
    if times.count >= 2 {
      #expect(times[0].duration(to: times[1]) < .seconds(5))
    }
  }

  @Test(
    .tags(.slow), .timeLimit(.minutes(2)),
    .enabled(
      if: ProcessInfo.processInfo.environment["PUBSUB_RUN_LONG_TESTS"] != nil,
      "Takes over a minute, so only runs when PUBSUB_RUN_LONG_TESTS is set"))
  func shouldKeepStreamOpenPastDefaultCallTimeout() async throws {
    let (handled, handledContinuation) = AsyncStream.makeStream(of: String.self)

    try await withSubscriber(handle: { handledContinuation.yield($0) }) { publish, streamCount in
      try await waitUntil { streamCount.value == 1 }
      // The client's service config times out calls after 60 seconds.
      try await Task.sleep(for: .seconds(65))
      let published = try await publish(["Hello"])

      #expect(await handled.first { _ in true } == published.first)
      #expect(streamCount.value == 1)
    }
  }

  // MARK: - Helpers

  private struct NackError: Error {}

  /// Runs a subscriber of a new subscription until `body` returns.
  ///
  /// `body` gets a function publishing messages and returning their IDs, and the number of
  /// streams opened so far.
  private func withSubscriber(
    handle: @escaping @Sendable (_ id: String) async throws -> Void,
    _ body: (
      _ publish: ([String]) async throws -> [String], _ streamCount: Counter
    ) async throws -> Void
  ) async throws {
    let pubSubService = try PubSubService()
    let topic = Topic<PlainTextMessage>(name: uniqueName("StreamingPullIntegrationTests"))
    let handler = CallbackHandler(topic: topic) { message in
      try await handle(message.id)
    }
    let streamCount = Counter()
    let subscriber = PullSubscriber(
      handler: handler,
      projectID: projectID,
      pubSubService: pubSubService,
      streamingPullConnection: CountingConnection(
        wrapped: Google_Pubsub_V1_Subscriber.Client(wrapping: pubSubService.grpcClient),
        count: streamCount)
    )
    let publisher = Publisher(projectID: projectID, pubSubService: pubSubService)

    try await testGracefulShutdown { shutdownTrigger in
      let publisherRun = Task { try await publisher.run() }
      try await pubSubService.create(
        subscription: handler.subscription,
        subscriberClient: subscriber.client,
        publisherClient: subscriber.publisherClient,
        projectID: projectID
      )
      let subscriberRun = Task { try await subscriber.run() }

      let result = await Result {
        try await body(
          { bodies in try await publisher.publish(to: topic, bodies: bodies).map(\.id) },
          streamCount)
      }

      shutdownTrigger.triggerGracefulShutdown()
      try await subscriberRun.value
      publisherRun.cancel()
      try await publisherRun.value
      try result.get()
    }
  }
}

private struct CountingConnection: StreamingPullConnection {

  let wrapped: any StreamingPullConnection
  let count: Counter

  func streamingPull<Result: Sendable>(
    metadata: Metadata,
    options: CallOptions,
    requestProducer:
      @Sendable @escaping (RPCWriter<Google_Pubsub_V1_StreamingPullRequest>) async throws -> Void,
    onResponse:
      @Sendable @escaping (StreamingClientResponse<Google_Pubsub_V1_StreamingPullResponse>)
      async throws -> Result
  ) async throws -> Result {
    count.increment()
    return try await wrapped.streamingPull(
      metadata: metadata, options: options, requestProducer: requestProducer,
      onResponse: onResponse)
  }
}
