import Foundation
import GoogleCloudPubSub
import Logging
import ServiceLifecycle
import Synchronization

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
    /// Time until every subscription handled a message, if they all did within 60 seconds.
    let startupSeconds: Double?
    let shutdownSeconds: Double
    let hung: Bool
    let inFlightAtShutdown: Int
  }

  let config: Config
  let resources: Resources
  let admin: Admin
  let trackers: [SubscriptionTracker]
  let load: LoadGenerator
  let sampler: Sampler
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
    let pending = Mutex<Sampler?>(nil)
    let load = LoadGenerator(
      publisher: try Publisher(projectID: config.projectID, createTopicsIfNeeded: false),
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
    let samplerRun = Task { await sampler.run() }
    let peakRun = Task { await trackPeaks() }
    defer {
      samplerRun.cancel()
      peakRun.cancel()
    }

    beginPhase("create")
    let createStart = monotonicNanos()
    try await admin.create(resources)
    note(
      "Created \(resources.topics.count) topics and \(resources.subscriptions.count) subscriptions in \(String(format: "%.1f", Double(monotonicNanos() - createStart) / 1e9))s"
    )
    endPhase("create")

    // Production runs a Publisher service alongside the subscribers. It keeps the shared clients
    // running, so churn can leave it out to restart them.
    var publisherGroup: ServiceGroup?
    var publisherRun: Task<Void, any Error>?
    if config.publisherService {
      let group = ServiceGroup(
        configuration: .init(services: [load.publisher], logger: Logger(label: "stress.publisher")))
      publisherGroup = group
      publisherRun = Task { try await group.run() }
    }

    do {
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
      case .publish:
        await publishLoad()
      case .cleanup:
        fatalError("Handled in main")
      }
    } catch {
      await publisherGroup?.triggerGracefulShutdown()
      _ = await publisherRun?.result
      throw error
    }
    await publisherGroup?.triggerGracefulShutdown()
    if case .failure(let error) = await publisherRun?.result {
      note("Publisher service threw \(error)")
    }
  }

  /// Starts the subscribers, publishes for the configured duration, drains and shuts down.
  private func runOnce() async throws {
    let group = try SubscriberServiceGroup(runner: self, cycle: 0)
    group.start()

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
      group.start()
      let runFor = Double.random(in: config.cycleMin...max(config.cycleMin, config.cycleMax))
      await sleepUnlessAborted(seconds: runFor)
      let result = await group.shutDown()
      if result.hung {
        await stop(loadRun)
        throw StressFailure.shutdownHung(cycle: cycle, seconds: result.shutdownSeconds)
      }
      if config.gapMax > 0 {
        load.isPausedNow = true
        let gap = Double.random(in: 0...config.gapMax)
        await sleepUnlessAborted(seconds: gap / 2)
        await load.publishOne()
        await sleepUnlessAborted(seconds: gap / 2)
        load.isPausedNow = false
      }
    }
    await stop(loadRun)
    endPhase("churn")

    let group = try SubscriberServiceGroup(runner: self, cycle: config.cycles + 1)
    group.start()
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
      "Published: \(publish.publishedMessages) msgs in \(publish.publishRPCs) RPCs, failed RPCs \(publish.failedRPCs), publish RPC p50 \(formatMicros(publish.latency.percentile(50))) p95 \(formatMicros(publish.latency.percentile(95))) p99 \(formatMicros(publish.latency.percentile(99))) max \(formatMicros(publish.latency.maximum))"
    )
    if !trackers.isEmpty {
      lines.append(
        "Correctness: expected deliveries \(expected) (+\(unconfirmed) unconfirmed), succeeded unique \(counts.succeededUnique), LOST \(missing), deliveries \(counts.deliveries), first \(counts.firstDeliveries), handler failures \(counts.handlerFailures), redeliveries after nack \(counts.redeliveriesAfterNack), REDELIVERED AFTER SUCCESS \(counts.redeliveriesAfterSuccess), concurrent duplicates \(counts.concurrentDuplicates), still in flight \(counts.inFlight)"
      )
    }
    if !missingSamples.isEmpty {
      lines.append("Missing samples: \(missingSamples.joined(separator: "; "))")
    }
    if !redeliveredAfterSuccessSamples.isEmpty {
      lines.append(
        "Redelivered after success samples: \(redeliveredAfterSuccessSamples.joined(separator: "; "))")
    }
    if latency.count > 0 {
      lines.append(
        "Latency publish->handler (first delivery): p50 \(formatMicros(latency.percentile(50))) p95 \(formatMicros(latency.percentile(95))) p99 \(formatMicros(latency.percentile(99))) p99.9 \(formatMicros(latency.percentile(99.9))) max \(formatMicros(latency.maximum))"
      )
    }

    // Throughput and resources per phase
    let samples = sampler.samples
    let reported = Set(
      phases.map(\.key).filter { $0.hasPrefix("load") || $0 == "churn" || $0 == "drain" || $0 == "backlog" })
    for phase in phases.map(\.key) where reported.contains(phase) {
      let phaseSamples = samples.filter { $0.phase == phase }
      guard !phaseSamples.isEmpty else { continue }
      let steady = phaseSamples.count > 4 ? Array(phaseSamples.dropFirst(phaseSamples.count / 4)) : phaseSamples
      func average(_ values: [Double]) -> Double { values.reduce(0, +) / Double(max(values.count, 1)) }
      let recv = average(steady.map(\.firstDeliveryRate))
      let all = average(steady.map(\.deliveryRate))
      let cpu = average(steady.map(\.cpuCores))
      let trend = slope(
        times: steady.map { $0.time / 60 }, values: steady.map { $0.residentMB - $0.trackingMB })
      let cpuPerK = all > 0 ? cpu / (all / 1000) : 0
      lines.append(
        "Phase \(phase): "
          + String(
            format:
              "recv avg %.0f/s (all deliveries %.0f/s, peak %.0f/s), publish avg %.0f/s (peak %.0f/s), cpu %.2f cores (%.3f cores per 1k deliveries/s), rss %.0f-%.0f MB (trend %+.1f MB/min excl. tracking), threads max %d, fds %d-%d, tls conns max %d, in-flight max %d",
            recv, all, phaseSamples.map(\.firstDeliveryRate).max() ?? 0,
            average(steady.map(\.publishRate)), phaseSamples.map(\.publishRate).max() ?? 0, cpu,
            cpuPerK, phaseSamples.map(\.residentMB).min() ?? 0,
            phaseSamples.map(\.residentMB).max() ?? 0, trend, phaseSamples.map(\.threads).max() ?? 0,
            phaseSamples.map(\.fileDescriptors).min() ?? 0,
            phaseSamples.map(\.fileDescriptors).max() ?? 0,
            phaseSamples.map(\.tlsConnections).max() ?? 0, phaseSamples.map(\.inFlight).max() ?? 0)
          + ", interval p99 max \(formatMicros(phaseSamples.map(\.p99).max() ?? 0))")
    }
    let peaks = sampler.peaks
    lines.append(
      String(
        format: "Peaks: rss %.0f MB, threads %d, fds %d, tls conns %d, handlers running %d",
        megabytes(peaks.resident), peaks.threads, peaks.fileDescriptors, peaks.tlsConnections,
        peaks.inFlight))

    let cycleResults = cycles.withLock { $0 }
    if !cycleResults.isEmpty {
      let shutdowns = cycleResults.map(\.shutdownSeconds)
      let startups = cycleResults.compactMap(\.startupSeconds)
      lines.append(
        String(
          format:
            "Service groups: %d, startup (every subscription handled a message) avg %.2fs max %.2fs (%d measured), graceful shutdown avg %.2fs max %.2fs, hung %d, handlers in flight at shutdown max %d",
          cycleResults.count, startups.reduce(0, +) / Double(max(startups.count, 1)),
          startups.max() ?? 0, startups.count, shutdowns.reduce(0, +) / Double(shutdowns.count),
          shutdowns.max() ?? 0, cycleResults.filter(\.hung).count,
          cycleResults.map(\.inFlightAtShutdown).max() ?? 0))
    }

    let logs = LogCounter.shared.snapshot.sorted { $0.value > $1.value }
    lines.append("Log messages (\(logs.reduce(0) { $0 + $1.value }) total):")
    lines += logs.prefix(40).map { "  \($0.value)x \($0.key)" }
    return lines.joined(separator: "\n")
  }
}

/// One ServiceGroup running every subscriber in a `PullSubscriberGroup`, as in production.
final class SubscriberServiceGroup: Sendable {

  let runner: Runner
  let cycle: Int
  let group: ServiceGroup
  let startNanos = Mutex<UInt64>(0)
  let task = Mutex<Task<Void, any Error>?>(nil)
  let startupSeconds = Mutex<Double?>(nil)
  let startupWatch = Mutex<Task<Void, Never>?>(nil)

  init(runner: Runner, cycle: Int) throws {
    self.runner = runner
    self.cycle = cycle
    let subscribers = try makeSubscribers(
      config: runner.config, resources: runner.resources, trackers: runner.trackers)
    var logger = Logger(label: "stress.servicegroup")
    logger.logLevel = .notice
    var configuration = ServiceGroupConfiguration(
      services: [.init(service: PullSubscriberGroup(subscribers))], logger: logger)
    configuration.maximumGracefulShutdownDuration = .seconds(runner.config.shutdownTimeout + 2)
    group = ServiceGroup(configuration: configuration)
  }

  func start() {
    let start = monotonicNanos()
    startNanos.withLock { $0 = start }
    let group = self.group
    task.withLock { $0 = Task { try await group.run() } }

    let trackers = runner.trackers
    let baseline = trackers.map(\.counts.deliveries)
    let runner = self.runner
    let cycle = self.cycle
    startupWatch.withLock {
      $0 = Task { [self] in
        while !Task.isCancelled {
          if zip(trackers, baseline).allSatisfy({ $0.counts.deliveries > $1 }) {
            let seconds = Double(monotonicNanos() - start) / 1e9
            startupSeconds.withLock { $0 = seconds }
            if cycle <= 1 {
              runner.note(
                "Cycle \(cycle): every subscription handled a message \(String(format: "%.2f", seconds))s after start")
            }
            return
          }
          if Double(monotonicNanos() - start) / 1e9 > 60 { return }
          try? await Task.sleep(for: .milliseconds(10))
        }
      }
    }
  }

  private var inFlight: Int {
    runner.trackers.reduce(0) { $0 + $1.counts.inFlight }
  }

  /// Triggers graceful shutdown and waits for the group to return.
  func shutDown() async -> Runner.CycleResult {
    startupWatch.withLock { $0?.cancel() }
    let ran = Double(monotonicNanos() - startNanos.withLock { $0 }) / 1e9
    let inFlightAtShutdown = inFlight
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
        print("-- cycle \(cycle): still shutting down after \(Int(Double(monotonicNanos() - start) / 1e9))s, handlers in flight \(inFlight)")
      }
    }
    let finished = done.load(ordering: .relaxed)
    let seconds = Double(monotonicNanos() - start) / 1e9
    if !finished {
      runner.note(
        "Cycle \(cycle): graceful shutdown hung for \(Int(seconds))s with \(inFlight) handlers in flight, cancelling")
      task.cancel()
      let cancelStart = monotonicNanos()
      while !done.load(ordering: .relaxed), monotonicNanos() - cancelStart < 30_000_000_000 {
        try? await Task.sleep(for: .milliseconds(20))
      }
      runner.note(
        done.load(ordering: .relaxed)
          ? "Cycle \(cycle): returned \(String(format: "%.2f", Double(monotonicNanos() - cancelStart) / 1e9))s after cancellation"
          : "Cycle \(cycle): still running 30s after cancellation")
    } else if case .failure(let error) = await task.result {
      runner.note("Cycle \(cycle): service group threw \(error)")
    }
    _ = waiter
    let result = Runner.CycleResult(
      cycle: cycle, ranSeconds: ran, startupSeconds: startupSeconds.withLock { $0 },
      shutdownSeconds: seconds, hung: !finished, inFlightAtShutdown: inFlightAtShutdown)
    runner.record(result)
    if seconds > 5 {
      runner.note(String(format: "Cycle %d: graceful shutdown took %.1fs", cycle, seconds))
    }
    print(
      String(format: "-- cycle %d: ran %.1fs, graceful shutdown %.2fs, %d handlers in flight at shutdown", cycle, ran, seconds, inFlightAtShutdown)
        + (finished ? "" : " HUNG"))
    return result
  }
}
