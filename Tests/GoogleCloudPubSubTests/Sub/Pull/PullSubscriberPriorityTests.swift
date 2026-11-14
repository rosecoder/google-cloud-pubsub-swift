import GRPCCore
import ServiceLifecycle
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PullSubscriberPriorityTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldReceiveLeaseAndAckAtHighPriorityAndHandleAtCallersPriority() async throws {
    let priorities = PriorityLog()
    let gate = Gate()
    let fakeConnection = FakeStreamingPullConnection()
    let connection = PriorityRecordingConnection(wrapping: fakeConnection, log: priorities)
    let clock = PriorityRecordingClock(log: priorities)
    let ackTransport = FakeAckTransport { call, _ in
      priorities.record(call.kind == .acknowledge ? "ack" : "modack")
    }
    let handler = CallbackHandler(
      topic: Topic<DataMessage>(name: uniqueName("PullSubscriberPriorityTests"))
    ) { _ in
      priorities.record("handler")
      try await gate.wait()
    }
    let pubSubService = try PubSubService()
    let subscriber = PullSubscriber(
      handler: handler,
      projectID: "pull-subscriber-priority-tests",
      pubSubService: pubSubService,
      streamingPullConnection: connection,
      ackTransport: ackTransport,
      clock: clock,
      leaseGenerator: SplitMix64(seed: 1))
    fakeConnection.deliver([FakeStreamingPullConnection.message(id: "a")])

    let recorded = try await testGracefulShutdown { shutdownTrigger in
      let run = Task(priority: .low) { try await subscriber.run() }
      defer { run.cancel() }
      try await waitUntil {
        priorities["handler"].count == 1 && priorities["modack"].count == 1
          && priorities["lease sleep"].count == 1
      }
      #expect(pubSubService.clientUsage.users == 1)
      clock.advance(by: .seconds(9))
      try await waitUntil { priorities["modack"].count == 2 }
      gate.open()
      try await waitUntil { priorities["ack"].count == 1 }
      // The shutdown timer runs at the caller's priority.
      let recorded = priorities.entries

      shutdownTrigger.triggerGracefulShutdown()
      try await run.cancellableValue
      return recorded
    }
    #expect(pubSubService.clientUsage.current == nil)

    for path in ["stream", "response", "keepalive sleep", "lease sleep", "modack", "ack"] {
      let pathPriorities = recorded[path] ?? []
      #expect(!pathPriorities.isEmpty, "\(path)")
      #expect(pathPriorities.allSatisfy { $0 == .high }, "\(path): \(pathPriorities)")
    }
    #expect(recorded["handler"] == [.low])
  }
}

/// Records the priority of the current task under a name.
private final class PriorityLog: Sendable {

  private let state = Mutex<[String: [TaskPriority]]>([:])

  var entries: [String: [TaskPriority]] {
    state.withLock { $0 }
  }

  subscript(name: String) -> [TaskPriority] {
    state.withLock { $0[name] ?? [] }
  }

  func record(_ name: String) {
    let priority = Task.currentPriority
    state.withLock { $0[name, default: []].append(priority) }
  }
}

private struct PriorityRecordingConnection: StreamingPullConnection {

  let connection: FakeStreamingPullConnection
  let log: PriorityLog

  init(wrapping connection: FakeStreamingPullConnection, log: PriorityLog) {
    self.connection = connection
    self.log = log
  }

  func streamingPull<Result: Sendable>(
    metadata: Metadata,
    options: CallOptions,
    requestProducer:
      @Sendable @escaping (RPCWriter<Google_Pubsub_V1_StreamingPullRequest>) async throws -> Void,
    onResponse:
      @Sendable @escaping (StreamingClientResponse<Google_Pubsub_V1_StreamingPullResponse>)
      async throws -> Result
  ) async throws -> Result {
    log.record("stream")
    return try await connection.streamingPull(
      metadata: metadata, options: options, requestProducer: requestProducer
    ) { [log] response in
      log.record("response")
      return try await onResponse(response)
    }
  }
}

/// A ``TestClock`` that records the priority of each sleep, as a lease sleep if it is shorter
/// than the 30 second keepalive interval.
private final class PriorityRecordingClock: Clock, Sendable {

  typealias Instant = ContinuousClock.Instant

  private let clock = TestClock()
  private let log: PriorityLog

  init(log: PriorityLog) {
    self.log = log
  }

  var now: Instant {
    clock.now
  }

  var minimumResolution: Duration {
    clock.minimumResolution
  }

  func sleep(until deadline: Instant, tolerance: Duration?) async throws {
    log.record(now.duration(to: deadline) < .seconds(30) ? "lease sleep" : "keepalive sleep")
    try await clock.sleep(until: deadline, tolerance: tolerance)
  }

  func advance(by duration: Duration) {
    clock.advance(by: duration)
  }
}
