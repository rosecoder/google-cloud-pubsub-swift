import ServiceLifecycleTestKit
import Testing

@testable import GoogleCloudPubSub

@Suite struct PublisherTests {

  @Test(.timeLimit(.minutes(1)))
  func shouldKeepSharedClientRunningUntilGracefulShutdown() async throws {
    let service = try PubSubService()
    let publisher = Publisher(projectID: "publisher-tests", pubSubService: service)

    try await testGracefulShutdown { shutdownTrigger in
      let run = Task { try await publisher.run() }
      defer { run.cancel() }
      try await waitUntil { service.clientUsage.users == 1 }

      // Stands in for a subscriber sharing the service that stops before the publisher.
      try await service.withClientInUse {}
      #expect(!service.clientUsage.hasBegunShutdown)

      let start = ContinuousClock.now
      shutdownTrigger.triggerGracefulShutdown()
      try await run.cancellableValue
      #expect(start.duration(to: .now) < .seconds(2))
      #expect(service.clientUsage.hasBegunShutdown)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func shouldReturnWhenCancelled() async throws {
    let service = try PubSubService()
    let publisher = Publisher(projectID: "publisher-tests", pubSubService: service)

    let run = Task { try await publisher.run() }
    defer { run.cancel() }
    try await waitUntil { service.clientUsage.users == 1 }
    run.cancel()
    try await run.cancellableValue
    #expect(service.clientUsage.hasBegunShutdown)
  }
}
