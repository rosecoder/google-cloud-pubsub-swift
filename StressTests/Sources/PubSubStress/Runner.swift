import Foundation
import Logging
import ServiceLifecycle
import Synchronization

@testable import GoogleCloudPubSub

enum StressFailure: Error {
  case shutdownHung(cycle: Int, seconds: Double)
  case aborted
}

/// Set by SIGINT/SIGTERM: stops the load and moves on to shutdown, verification and cleanup.
let abortRequested = Atomic<Bool>(false)

final class Runner: Sendable {

  struct CycleResult: Sendable {
    let cycle: Int
    let ranSeconds: Double
    let startupSeconds: Double?
    let shutdownSeconds: Double
    let hung: Bool
  }

  let config: Config
  let resources: Resources
  let admin: Admin
  let trackers: [SubscriptionTracker]
  let load: LoadGenerator
  let sampler: Sampler
  let loadService: PubSubService
  let resultDirectory: String
  private let cycles = Mutex<[CycleResult]>([])
  private let notes = Mutex<[String]>([])
  private let phaseTimes = Mutex<[String: (start: Double, end: Double)]>([:])

  init(config: Config, admin: Admin, resultDirectory: String) throws {
    self.config = config
    self.admin = admin
    self.resultDirectory = resultDirectory
    let resources = Resources(config: config)
    self.resources = resources
    let trackers = resources.subscriptions.indices.map {
      SubscriptionTracker(index: $0, topicIndex: resources.topicIndex(ofSubscription: $0))
    }
    self.trackers = trackers
    let loadService = try makePubSubService()
    self.loadService = loadService
    let pending = Mutex<Sampler?>(nil)
    let load = LoadGenerator(
      publisher: Publisher(projectID: config.projectID, pubSubService: loadService),
      topics: resources.topics, config: config,
      pendingDeliveries: { pending.withLock { $0?.pendingDeliveries() } ?? 0 })
    self.load = load
    let sampler = Sampler(
      config: config, trackers: trackers, load: load,
      csvPath: resultDirectory + "/samples.csv")
    pending.withLock { $0 = sampler }
    self.sampler = sampler
  }

  func note(_ text: String) {
    print("[note] \(text)")
    notes.withLock { $0.append(text) }
  }

  private func beginPhase(_ name: String) {
    let now = sampler.elapsed
    sampler.phase = name
    phaseTimes.withLock { $0[name] = (now, now) }
    print("== \(name) at \(String(format: "%.1f", now))s")
  }

  private func endPhase(_ name: String) {
    let now = sampler.elapsed
    phaseTimes.withLock { $0[name]?.end = now }
  }

  // MARK: - Run

  func run() async throws {
    print("Config: \(config)")
    print("Results: \(resultDirectory)")
    let loadServiceRun = Task { try await loadService.run() }
    let loadPublisherRun = Task { try await load.publisher.run() }
    let samplerRun = Task { await sampler.run() }
    let peakRun = Task { await trackPeaks() }
    defer {
      samplerRun.cancel()
      peakRun.cancel()
      loadPublisherRun.cancel()
      loadServiceRun.cancel()
    }

    beginPhase("create")
    let createStart = monotonicNanos()
    try await admin.create(resources)
    note(
      "Created \(resources.topics.count) topics and \(resources.subscriptions.count) subscriptions in \(String(format: "%.1f", Double(monotonicNanos() - createStart) / 1e9))s"
    )
    endPhase("create")

    if config.backlog > 0 {
      beginPhase("backlog")
      let start = monotonicNanos()
      await load.publishBacklog(perTopic: config.backlog)
      let seconds = Double(monotonicNanos() - start) / 1e9
      note(
        "Published backlog of \(config.backlog) x \(resources.topics.count) topics = \(load.snapshot.publishedMessages) messages in \(String(format: "%.1f", seconds))s (\(String(format: "%.0f", Double(load.snapshot.publishedMessages) / seconds))/s)"
      )
      endPhase("backlog")
      sampler.sample()
    }

    switch config.mode {
    case .run:
      try await runOnce()
    case .churn:
      try await runChurn()
    case .cleanup:
      fatalError("Handled in main")
    }
  }

  /// Starts the subscribers, publishes for the configured duration, drains and shuts down.
  private func runOnce() async throws {
    let group = try SubscriberServiceGroup(runner: self, cycle: 0)
    await group.start()

    await publishLoad()
    await drain()

    let result = await group.shutDown()
    if result.hung {
      throw StressFailure.shutdownHung(cycle: 0, seconds: result.shutdownSeconds)
    }
  }

  /// Restarts the subscriber service group under load, then drains with a final group.
  private func runChurn() async throws {
    beginPhase("churn")
    let loadRun = Task { await load.run() }
    load.rate = config.rate
    for cycle in 1...config.cycles {
      guard !abortRequested.load(ordering: .relaxed) else { break }
      let group = try SubscriberServiceGroup(runner: self, cycle: cycle)
      await group.start()
      let runFor = Double.random(in: config.cycleMin...max(config.cycleMin, config.cycleMax))
      await sleepUnlessAborted(seconds: runFor)
      let result = await group.shutDown()
      if result.hung {
        await stop(loadRun)
        throw StressFailure.shutdownHung(cycle: cycle, seconds: result.shutdownSeconds)
      }
    }
    await stop(loadRun)
    endPhase("churn")

    let group = try SubscriberServiceGroup(runner: self, cycle: config.cycles + 1)
    await group.start()
    await drain()
    let result = await group.shutDown()
    if result.hung {
      throw StressFailure.shutdownHung(cycle: config.cycles + 1, seconds: result.shutdownSeconds)
    }
  }

  private func publishLoad() async {
    let steps = config.rateSteps.isEmpty ? [(config.rate, config.duration)] : config.rateSteps
    guard steps.contains(where: { $0.1 > 0 }) else { return }
    let loadRun = Task { await load.run() }
    for (index, step) in steps.enumerated() {
      load.rate = step.0
      let name = steps.count == 1 ? "load" : "load\(index + 1)"
      beginPhase(name)
      note("Publishing at \(step.0 == 0 ? "max rate" : "\(Int(step.0))/s") for \(Int(step.1))s")
      await sleepUnlessAborted(seconds: step.1)
      endPhase(name)
    }
    await stop(loadRun)
  }

  /// Stops publishing, cancelling publishes still retrying after a few seconds.
  private func stop(_ loadRun: Task<Void, Never>) async {
    load.stop()
    let stopped = Atomic<Bool>(false)
    let waiter = Task {
      await loadRun.value
      stopped.store(true, ordering: .relaxed)
    }
    let start = monotonicNanos()
    while !stopped.load(ordering: .relaxed) {
      if monotonicNanos() - start > 10_000_000_000 {
        note("Publishing did not stop within 10s, cancelling publishes")
        loadRun.cancel()
        break
      }
      try? await Task.sleep(for: .milliseconds(20))
    }
    await waiter.value
  }

  private func drain() async {
    beginPhase("drain")
    let start = monotonicNanos()
    while sampler.pendingDeliveries() > 0, !abortRequested.load(ordering: .relaxed) {
      if Double(monotonicNanos() - start) / 1e9 > config.drainTimeout {
        note("Drain timed out after \(Int(config.drainTimeout))s with \(sampler.pendingDeliveries()) deliveries pending")
        break
      }
      try? await Task.sleep(for: .milliseconds(100))
    }
    // Lets the acks of the last handled messages go out before shutting down.
    try? await Task.sleep(for: .seconds(1))
    note(
      "Drained in \(String(format: "%.1f", Double(monotonicNanos() - start) / 1e9))s, \(sampler.pendingDeliveries()) pending"
    )
    endPhase("drain")
  }

  func sleepUnlessAborted(seconds: Double) async {
    let end = monotonicNanos() + UInt64(seconds * 1e9)
    while monotonicNanos() < end, !abortRequested.load(ordering: .relaxed) {
      try? await Task.sleep(for: .milliseconds(min(200, Double(end - monotonicNanos()) / 1e6)))
    }
  }

  private func trackPeaks() async {
    // Sampled more often than the regular samples to catch short memory spikes.
    while !Task.isCancelled {
      try? await Task.sleep(for: .milliseconds(250))
      sampler.recordPeaks(ProcessMetrics.snapshot())
    }
  }

  func record(_ result: CycleResult) {
    cycles.withLock { $0.append(result) }
  }

  // MARK: - Summary

  func summary(error: (any Error)?) -> String {
    var lines: [String] = []
    lines.append("Config: \(config)")
    if let error {
      lines.append("FAILED: \(error)")
    }

    let phases = phaseTimes.withLock { $0 }.sorted { $0.value.start < $1.value.start }
    lines.append(
      "Phases: "
        + phases.map { "\($0.key) \(String(format: "%.0f-%.0fs", $0.value.start, $0.value.end))" }
        .joined(separator: ", "))
    lines += notes.withLock { $0 }.map { "Note: \($0)" }

    // Correctness
    var counts = SubscriptionTracker.Counts()
    var latency = Histogram()
    var missing = 0
    var missingSamples: [String] = []
    var expected = 0
    var unconfirmed = 0
    var redeliveredAfterSuccessSamples: [String] = []
    for (index, tracker) in trackers.enumerated() {
      counts.add(tracker.counts)
      latency.merge(tracker.latency)
      let published = load.allocatedCount(topic: tracker.topicIndex)
      let ranges = load.unconfirmedRanges(topic: tracker.topicIndex)
      let unconfirmedCount = ranges.reduce(0) { $0 + $1.count }
      unconfirmed += unconfirmedCount
      expected += Int(published) - unconfirmedCount
      let result = tracker.missing(published: published, unconfirmed: ranges)
      missing += result.count
      if result.count > 0, missingSamples.count < 10 {
        missingSamples.append("s\(index): \(result.count) e.g. seq \(result.samples.prefix(5))")
      }
      let samples = tracker.redeliveredAfterSuccessSamples
      if !samples.isEmpty, redeliveredAfterSuccessSamples.count < 10 {
        redeliveredAfterSuccessSamples.append("s\(index): seq \(samples.prefix(5))")
      }
    }
    let publish = load.snapshot
    lines.append(
      "Published: \(publish.publishedMessages) msgs in \(publish.publishRPCs) RPCs, failed RPCs \(publish.failedRPCs), publish RPC p50 \(formatMicros(publish.latency.percentile(50))) p99 \(formatMicros(publish.latency.percentile(99)))"
    )
    lines.append(
      "Correctness: expected deliveries \(expected) (+\(unconfirmed) unconfirmed), succeeded unique \(counts.succeededUnique), LOST \(missing), deliveries \(counts.deliveries), first \(counts.firstDeliveries), handler failures \(counts.handlerFailures), redeliveries after nack \(counts.redeliveriesAfterNack), REDELIVERED AFTER SUCCESS \(counts.redeliveriesAfterSuccess), concurrent duplicates \(counts.concurrentDuplicates), still in flight \(counts.inFlight)"
    )
    if !missingSamples.isEmpty {
      lines.append("Missing samples: \(missingSamples.joined(separator: "; "))")
    }
    if !redeliveredAfterSuccessSamples.isEmpty {
      lines.append(
        "Redelivered after success samples: \(redeliveredAfterSuccessSamples.joined(separator: "; "))")
    }
    lines.append(
      "Latency publish->handler (first delivery): p50 \(formatMicros(latency.percentile(50))) p90 \(formatMicros(latency.percentile(90))) p99 \(formatMicros(latency.percentile(99))) p99.9 \(formatMicros(latency.percentile(99.9))) max \(formatMicros(latency.maximum))"
    )

    // Throughput and resources during load phases
    let samples = sampler.samples
    let loadPhases = Set(phases.map(\.key).filter { $0.hasPrefix("load") || $0 == "churn" || $0 == "drain" })
    for phase in phases.map(\.key) where loadPhases.contains(phase) {
      let phaseSamples = samples.filter { $0.phase == phase }
      guard !phaseSamples.isEmpty else { continue }
      let steady = phaseSamples.count > 4 ? Array(phaseSamples.dropFirst(phaseSamples.count / 4)) : phaseSamples
      func average(_ values: [Double]) -> Double { values.reduce(0, +) / Double(max(values.count, 1)) }
      let recv = average(steady.map(\.firstDeliveryRate))
      let all = average(steady.map(\.deliveryRate))
      let cpu = average(steady.map(\.cpuCores))
      let trend = slope(
        times: steady.map { $0.time / 60 }, values: steady.map { $0.footprintMB - $0.trackingMB })
      var line = String(
        format:
          "Phase %@: recv avg %.0f/s (all deliveries %.0f/s, peak %.0f/s), publish avg %.0f/s, cpu %.2f cores (%.3f cores per 1k msgs/s), footprint %.0f-%.0f MB (trend %+.1f MB/min excl. tracking), threads max %d, in-flight max %d, ack RPC %.1f/s (%.0f ids each), extend RPC %.1f/s, nack RPC %.1f/s, interval p99 max %@",
        phase as NSString, recv, all, phaseSamples.map(\.firstDeliveryRate).max() ?? 0,
        average(steady.map(\.publishRate)), cpu, all > 0 ? cpu / (all / 1000) : 0,
        phaseSamples.map(\.footprintMB).min() ?? 0, phaseSamples.map(\.footprintMB).max() ?? 0, trend,
        phaseSamples.map(\.threads).max() ?? 0, phaseSamples.map(\.inFlight).max() ?? 0,
        average(steady.map(\.ackRPCRate)), average(steady.map(\.idsPerAckRPC)),
        average(steady.map(\.extendRPCRate)), average(steady.map(\.nackRPCRate)),
        formatMicros(phaseSamples.map(\.p99).max() ?? 0) as NSString)
      if let serverCPU = steady.compactMap(\.serverCPU).max() {
        line += String(format: ", server cpu max %.2f cores, server mem max %.0f MB", serverCPU, steady.compactMap(\.serverMB).max() ?? 0)
      }
      lines.append(line)
    }
    let peaks = sampler.peaks
    lines.append(
      String(
        format: "Peaks: footprint %.0f MB, resident %.0f MB, threads %d, handlers running %d, handler tasks waiting for ack RPCs %d",
        megabytes(peaks.footprint), megabytes(peaks.resident), peaks.threads, peaks.inFlight,
        peaks.ackWaiters))

    let transport = TransportStats.shared.snapshot
    lines.append(
      "Transport: ack RPCs \(transport.ackRPCs) (\(transport.ackIDs) ids), extend RPCs \(transport.extendRPCs) (\(transport.extendIDs) ids), nack RPCs \(transport.nackRPCs) (\(transport.nackIDs) ids), failed \(transport.failedRPCs), RPC p50 \(formatMicros(transport.rpcLatency.percentile(50))) p99 \(formatMicros(transport.rpcLatency.percentile(99))) max \(formatMicros(transport.rpcLatency.maximum)), TCP connections opened \(transport.tcpConnections), HTTP/2 streams opened \(transport.http2Streams)"
    )

    let cycleResults = cycles.withLock { $0 }
    if !cycleResults.isEmpty {
      let shutdowns = cycleResults.map(\.shutdownSeconds)
      let startups = cycleResults.compactMap(\.startupSeconds)
      lines.append(
        String(
          format: "Service groups: %d, startup (all streams) avg %.2fs max %.2fs, graceful shutdown avg %.2fs max %.2fs, hung %d",
          cycleResults.count, startups.reduce(0, +) / Double(max(startups.count, 1)),
          startups.max() ?? 0, shutdowns.reduce(0, +) / Double(shutdowns.count), shutdowns.max() ?? 0,
          cycleResults.filter(\.hung).count))
    }

    let logs = LogCounter.shared.snapshot.sorted { $0.value > $1.value }
    lines.append("Log messages (\(logs.reduce(0) { $0 + $1.value }) total):")
    lines += logs.prefix(40).map { "  \($0.value)x \($0.key)" }
    return lines.joined(separator: "\n")
  }
}

/// One ServiceGroup of a publisher (listed first) and every subscriber, on a fresh PubSubService.
final class SubscriberServiceGroup: Sendable {

  let runner: Runner
  let cycle: Int
  let service: PubSubService
  let subscribers: [PullSubscriber<StressHandler>]
  let subscriberGroup: SubscriberGroup
  let group: ServiceGroup
  let startNanos = Mutex<UInt64>(0)
  let task = Mutex<Task<Void, any Error>?>(nil)
  let startupSeconds = Mutex<Double?>(nil)

  init(runner: Runner, cycle: Int) throws {
    self.runner = runner
    self.cycle = cycle
    let service = try makePubSubService(maxStreamsPerConnection: runner.config.maxStreamsPerConnection)
    self.service = service
    let subscribers = makeSubscribers(
      config: runner.config, resources: runner.resources, trackers: runner.trackers, service: service)
    self.subscribers = subscribers
    let subscriberGroup = SubscriberGroup(subscribers: subscribers)
    self.subscriberGroup = subscriberGroup
    let publisher = Publisher(projectID: runner.config.projectID, pubSubService: service)
    var logger = Logger(label: "stress.servicegroup")
    logger.logLevel = .notice
    group = ServiceGroup(
      configuration: .init(
        services: [
          .init(service: publisher),
          .init(service: subscriberGroup),
        ],
        logger: logger))
  }

  func start() async {
    runner.sampler.service.withLock { $0 = service }
    runner.sampler.subscribers.withLock { $0 = subscribers }
    let start = monotonicNanos()
    startNanos.withLock { $0 = start }
    let group = self.group
    task.withLock { $0 = Task { try await group.run() } }
    let expected = runner.config.subscriptions
    while service.pool.activeStreamCounts.reduce(0, +) < expected {
      if Double(monotonicNanos() - start) / 1e9 > 60 {
        runner.note("Cycle \(cycle): only \(service.pool.activeStreamCounts) streams after 60s")
        return
      }
      try? await Task.sleep(for: .milliseconds(5))
    }
    let seconds = Double(monotonicNanos() - start) / 1e9
    startupSeconds.withLock { $0 = seconds }
    if cycle <= 1 {
      runner.note(
        "Cycle \(cycle): \(expected) streams active after \(String(format: "%.3f", seconds))s on \(service.pool.activeStreamCounts.count) connections \(service.pool.activeStreamCounts)"
      )
    }
  }

  /// Triggers graceful shutdown and waits for the group to return, sampling stacks if it hangs.
  func shutDown() async -> Runner.CycleResult {
    let ran = Double(monotonicNanos() - startNanos.withLock { $0 }) / 1e9
    let start = monotonicNanos()
    await group.triggerGracefulShutdown()
    let task = self.task.withLock { $0 }!
    let hangTimeout = runner.config.hangTimeout
    let done = Atomic<Bool>(false)
    let waiter = Task {
      _ = try? await task.value
      done.store(true, ordering: .relaxed)
    }
    var lastReport = start
    while !done.load(ordering: .relaxed), Double(monotonicNanos() - start) / 1e9 < hangTimeout {
      try? await Task.sleep(for: .milliseconds(20))
      if monotonicNanos() - lastReport > 10_000_000_000 {
        lastReport = monotonicNanos()
        print("-- cycle \(cycle): still shutting down after \(Int(Double(monotonicNanos() - start) / 1e9))s: \(shutdownState())")
      }
    }
    let finished = done.load(ordering: .relaxed)
    let seconds = Double(monotonicNanos() - start) / 1e9
    if !finished {
      let path = runner.resultDirectory + "/hang-cycle\(cycle).sample.txt"
      runner.note(
        "Cycle \(cycle): graceful shutdown hung for \(Int(seconds))s: \(shutdownState()). Sampling stacks to \(path)")
      let process = Process()
      process.executableURL = URL(fileURLWithPath: "/usr/bin/sample")
      process.arguments = ["\(getpid())", "3", "-file", path]
      try? process.run()
      process.waitUntilExit()
      task.cancel()
      let cancelStart = monotonicNanos()
      while !done.load(ordering: .relaxed), monotonicNanos() - cancelStart < 30_000_000_000 {
        try? await Task.sleep(for: .milliseconds(20))
      }
      runner.note(
        done.load(ordering: .relaxed)
          ? "Cycle \(cycle): returned \(String(format: "%.2f", Double(monotonicNanos() - cancelStart) / 1e9))s after cancellation"
          : "Cycle \(cycle): still running 30s after cancellation: \(shutdownState())")
    } else if case .failure(let error) = await task.result {
      runner.note("Cycle \(cycle): service group threw \(error)")
    }
    _ = waiter
    let result = Runner.CycleResult(
      cycle: cycle, ranSeconds: ran, startupSeconds: startupSeconds.withLock { $0 },
      shutdownSeconds: seconds, hung: !finished)
    runner.record(result)
    if seconds > 5 {
      runner.note(String(format: "Cycle %d: graceful shutdown took %.1fs", cycle, seconds))
    }
    print(
      String(
        format: "-- cycle %d: ran %.1fs, graceful shutdown %.2fs%@", cycle, ran, seconds,
        (finished ? "" : " HUNG") as NSString))
    return result
  }

  private func shutdownState() -> String {
    let usage = service.clientUsage
    let queued = subscribers.reduce(0) { $0 + $1.ackBatcher.queuedCount }
    let waiters = subscribers.reduce(0) { $0 + $1.ackBatcher.waiterCount }
    let running = subscriberGroup.running
    return
      "subscribers still running \(running.count) \(running.prefix(5).map { "s\($0)" }), client users \(usage.users), shutdown requested \(usage.isShutdownRequested), begun \(usage.hasBegunShutdown), stream slots \(service.pool.activeStreamCounts), acks queued \(queued), ack waiters \(waiters)"
  }
}
