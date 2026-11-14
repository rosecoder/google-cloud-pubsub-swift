import GRPCCore
import ServiceLifecycleTestKit
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PubSubServiceTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldBeginShutdownOnlyAfterLastClientUseEnds() async throws {
    let service = try PubSubService()
    let firstGate = Gate()
    let secondGate = Gate()
    let first = Task { await service.withClientInUse { await firstGate.wait() } }
    let second = Task { await service.withClientInUse { await secondGate.wait() } }
    try await waitUntil { service.clientUsage.users == 2 }

    let run = Task { try await service.run() }
    run.cancel()
    try await waitUntil { service.clientUsage.isShutdownRequested }
    #expect(!service.clientUsage.hasBegunShutdown)

    firstGate.open()
    await first.value
    #expect(!service.clientUsage.hasBegunShutdown)

    secondGate.open()
    await second.value
    #expect(service.clientUsage.hasBegunShutdown)
    try await run.value
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldBeginShutdownRightAwayWhenClientIsNotInUse() async throws {
    let service = try PubSubService()

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { try await service.run() }
      shutdownTrigger.triggerGracefulShutdown()

      try await run.value
      #expect(service.clientUsage.hasBegunShutdown)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldEndClientUseWhenBodyThrows() async throws {
    let service = try PubSubService()
    let gate = Gate()
    let holder = Task { await service.withClientInUse { await gate.wait() } }
    try await waitUntil { service.clientUsage.users == 1 }
    let run = Task { try await service.run() }
    run.cancel()
    try await waitUntil { service.clientUsage.isShutdownRequested }

    await #expect(throws: CancellationError.self) {
      try await service.withClientInUse {
        #expect(service.clientUsage.users == 2)
        throw CancellationError()
      }
    }
    #expect(service.clientUsage.users == 1)
    #expect(!service.clientUsage.hasBegunShutdown)

    gate.open()
    await holder.value
    #expect(service.clientUsage.hasBegunShutdown)
    try await run.value
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldOpenAnotherConnectionForStreamsBeyondMaxStreamsPerConnection() async throws {
    let service = try PubSubService(maxStreamsPerConnection: 2)
    let gate = Gate()
    let clients = Mutex<[ObjectIdentifier]>([])
    let streams = (0..<3).map { _ in
      Task {
        try await service.withClientInUse {
          try await service.pool.withStreamSlot { client in
            clients.withLock { $0.append(ObjectIdentifier(client)) }
            await gate.wait()
          }
        }
      }
    }
    let run = Task { try await service.run() }
    try await waitUntil { service.pool.activeStreamCounts == [2, 1] }

    let primary = ObjectIdentifier(service.grpcClient)
    #expect(clients.withLock { $0 }.count { $0 == primary } == 2)
    #expect(Set(clients.withLock { $0 }).count == 2)

    run.cancel()
    gate.open()
    for stream in streams {
      try await stream.value
    }
    try await run.value
    await #expect(throws: RuntimeError.self) {
      try await service.pool.withStreamSlot { _ in }
    }
  }
}
