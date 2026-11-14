import Foundation
import Synchronization

struct Sample: Sendable {
  var time: Double
  var phase: String
  var published: Int
  var publishRate: Double
  var firstDeliveries: Int
  var firstDeliveryRate: Double
  var deliveryRate: Double
  var inFlight: Int
  var backlog: Int
  var residentMB: Double
  var trackingMB: Double
  var threads: Int
  var fileDescriptors: Int
  var tlsConnections: Int
  var cpuCores: Double
  var p50: UInt64
  var p99: UInt64
  var max: UInt64
  var warnings: Int

  static let csvHeader =
    "time,phase,published,publish_rate,first_deliveries,first_delivery_rate,delivery_rate,in_flight,backlog,resident_mb,tracking_mb,threads,fds,tls_connections,cpu_cores,p50_us,p99_us,max_us,warnings"

  var csvLine: String {
    [
      String(format: "%.1f", time), phase, "\(published)", String(format: "%.0f", publishRate),
      "\(firstDeliveries)", String(format: "%.0f", firstDeliveryRate),
      String(format: "%.0f", deliveryRate), "\(inFlight)", "\(backlog)",
      String(format: "%.1f", residentMB), String(format: "%.1f", trackingMB), "\(threads)",
      "\(fileDescriptors)", "\(tlsConnections)", String(format: "%.2f", cpuCores),
      "\(p50)", "\(p99)", "\(max)", "\(warnings)",
    ].joined(separator: ",")
  }

  var line: String {
    String(format: "%6.0fs ", time) + phase.padding(toLength: 8, withPad: " ", startingAt: 0)
      + String(
        format:
          " pub %7.0f/s  recv %7.0f/s (all %7.0f/s)  inflight %6d backlog %8d  rss %6.0fMB (track %4.0f)  thr %3d fd %4d tls %3d  cpu %5.2f",
        publishRate, firstDeliveryRate, deliveryRate, inFlight, backlog, residentMB, trackingMB,
        threads, fileDescriptors, tlsConnections, cpuCores)
      + "  lat p50 \(formatMicros(p50)) p99 \(formatMicros(p99)) max \(formatMicros(max))  warn \(warnings)"
  }
}

/// Periodically records process and correctness metrics.
final class Sampler: Sendable {

  private struct State {
    var samples: [Sample] = []
    var phase = "setup"
    var lastTime = 0.0
    var lastPublished = 0
    var lastFirst = 0
    var lastDeliveries = 0
    var lastCPU = 0.0
    var peakResident: UInt64 = 0
    var peakThreads = 0
    var peakFileDescriptors = 0
    var peakTLSConnections = 0
    var peakInFlight = 0
  }

  let config: Config
  let trackers: [SubscriptionTracker]
  let load: LoadGenerator
  let startNanos = monotonicNanos()
  private let state = Mutex(State())
  private let csv: FileHandle?

  init(config: Config, trackers: [SubscriptionTracker], load: LoadGenerator, csvPath: String) {
    self.config = config
    self.trackers = trackers
    self.load = load
    FileManager.default.createFile(atPath: csvPath, contents: Data((Sample.csvHeader + "\n").utf8))
    csv = FileHandle(forWritingAtPath: csvPath)
    csv?.seekToEndOfFile()
  }

  var elapsed: Double {
    Double(monotonicNanos() - startNanos) / 1e9
  }

  var phase: String {
    get { state.withLock { $0.phase } }
    set { state.withLock { $0.phase = newValue } }
  }

  var samples: [Sample] {
    state.withLock { $0.samples }
  }

  var peaks: (resident: UInt64, threads: Int, fileDescriptors: Int, tlsConnections: Int, inFlight: Int) {
    state.withLock {
      ($0.peakResident, $0.peakThreads, $0.peakFileDescriptors, $0.peakTLSConnections, $0.peakInFlight)
    }
  }

  func recordPeaks(_ snapshot: ProcessSnapshot) {
    state.withLock { state in
      state.peakResident = max(state.peakResident, snapshot.residentBytes)
      state.peakThreads = max(state.peakThreads, snapshot.threads)
      state.peakFileDescriptors = max(state.peakFileDescriptors, snapshot.fileDescriptors)
      state.peakTLSConnections = max(state.peakTLSConnections, snapshot.tlsConnections)
    }
  }

  func run() async {
    while !Task.isCancelled {
      try? await Task.sleep(for: .seconds(config.sampleInterval))
      if Task.isCancelled { return }
      sample()
    }
  }

  /// Pending deliveries: published messages each subscription has not yet handled successfully.
  func pendingDeliveries() -> Int {
    let allocated = load.allocatedCounts
    var pending = 0
    for tracker in trackers {
      pending += Int(allocated[tracker.topicIndex]) - tracker.succeededUnique
    }
    return pending
  }

  @discardableResult
  func sample() -> Sample {
    let time = elapsed
    let process = ProcessMetrics.snapshot()
    var counts = SubscriptionTracker.Counts()
    var latency = Histogram()
    var tracking = 0
    for tracker in trackers {
      counts.add(tracker.counts)
      latency.merge(tracker.takeIntervalLatency())
      tracking += tracker.trackingBytes
    }
    let published = load.snapshot.publishedMessages
    let warnings = LogCounter.shared.total(atLeast: .warning)
    let backlog = pendingDeliveries()

    let sample = state.withLock { state in
      let interval = max(time - state.lastTime, 0.001)
      let sample = Sample(
        time: time, phase: state.phase, published: published,
        publishRate: Double(published - state.lastPublished) / interval,
        firstDeliveries: counts.firstDeliveries,
        firstDeliveryRate: Double(counts.firstDeliveries - state.lastFirst) / interval,
        deliveryRate: Double(counts.deliveries - state.lastDeliveries) / interval,
        inFlight: counts.inFlight, backlog: backlog,
        residentMB: megabytes(process.residentBytes),
        trackingMB: Double(tracking) / 1_048_576, threads: process.threads,
        fileDescriptors: process.fileDescriptors, tlsConnections: process.tlsConnections,
        cpuCores: (process.cpuSeconds - state.lastCPU) / interval,
        p50: latency.percentile(50), p99: latency.percentile(99), max: latency.maximum,
        warnings: warnings)
      state.lastTime = time
      state.lastPublished = published
      state.lastFirst = counts.firstDeliveries
      state.lastDeliveries = counts.deliveries
      state.lastCPU = process.cpuSeconds
      state.peakResident = max(state.peakResident, process.residentBytes)
      state.peakThreads = max(state.peakThreads, process.threads)
      state.peakFileDescriptors = max(state.peakFileDescriptors, process.fileDescriptors)
      state.peakTLSConnections = max(state.peakTLSConnections, process.tlsConnections)
      state.peakInFlight = max(state.peakInFlight, counts.inFlight)
      state.samples.append(sample)
      return sample
    }
    print(sample.line)
    csv?.write(Data((sample.csvLine + "\n").utf8))
    return sample
  }
}
