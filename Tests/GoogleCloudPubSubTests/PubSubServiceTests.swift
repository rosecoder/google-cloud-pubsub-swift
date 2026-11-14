import GRPCCore
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PubSubServiceTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldBeginShutdownOnlyAfterLastClientUseEnds() async throws {
    let service = try PubSubService()
    let firstGate = Gate()
    let secondGate = Gate()
    let first = Task { try await service.withClientInUse { await firstGate.wait() } }
    let second = Task { try await service.withClientInUse { await secondGate.wait() } }
    try await waitUntil { service.clientUsage.users == 2 }

    firstGate.open()
    try await first.value
    #expect(!service.clientUsage.hasBegunShutdown)
    try await service.pool.withStreamSlot { _ in }

    secondGate.open()
    try await second.value
    #expect(service.clientUsage.hasBegunShutdown)
    await #expect(throws: RuntimeError.self) {
      try await service.pool.withStreamSlot { _ in }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldThrowWithoutRunningBodyWhenUsedAfterShutdown() async throws {
    let service = try PubSubService()
    try await service.withClientInUse {}

    let error = await #expect(throws: RuntimeError.self) {
      try await service.withClientInUse {
        Issue.record("Body ran after the clients shut down.")
      }
    }
    #expect(error?.code == .clientIsStopped)
    #expect(service.clientUsage.users == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldEndClientUseWhenBodyThrows() async throws {
    let service = try PubSubService()
    let gate = Gate()
    let holder = Task { try await service.withClientInUse { await gate.wait() } }
    try await waitUntil { service.clientUsage.users == 1 }

    await #expect(throws: CancellationError.self) {
      try await service.withClientInUse {
        #expect(service.clientUsage.users == 2)
        throw CancellationError()
      }
    }
    #expect(service.clientUsage.users == 1)
    #expect(!service.clientUsage.hasBegunShutdown)

    gate.open()
    try await holder.value
    #expect(service.clientUsage.hasBegunShutdown)
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldRethrowBodyErrorOfLastClientUse() async throws {
    let service = try PubSubService()
    struct BodyError: Error {}

    await #expect(throws: BodyError.self) {
      try await service.withClientInUse { throw BodyError() }
    }
    #expect(service.clientUsage.hasBegunShutdown)
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
    try await waitUntil { service.pool.activeStreamCounts == [2, 1] }

    let primary = ObjectIdentifier(service.grpcClient)
    #expect(clients.withLock { $0 }.count { $0 == primary } == 2)
    #expect(Set(clients.withLock { $0 }).count == 2)

    gate.open()
    for stream in streams {
      try await stream.value
    }
    await #expect(throws: RuntimeError.self) {
      try await service.pool.withStreamSlot { _ in }
    }
  }
}
