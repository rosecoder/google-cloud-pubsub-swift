import ServiceLifecycleTestKit
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
}
