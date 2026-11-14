import Foundation
import Logging
import Synchronization

@testable import GoogleCloudPubSub

struct Sample: Sendable {
  var time: Double
  var phase: String
  var published: Int
  var publishRate: Double
  var firstDeliveries: Int
  var firstDeliveryRate: Double
  var deliveryRate: Double
  var inFlight: Int
  /// Handler tasks of the library waiting for the outcome of their ack or nack RPC.
  var ackWaiters: Int
  var backlog: Int
  var footprintMB: Double
  var residentMB: Double
  var trackingMB: Double
  var threads: Int
  var cpuCores: Double
  var ackRPCRate: Double
  var extendRPCRate: Double
  var nackRPCRate: Double
  var idsPerAckRPC: Double
  var p50: UInt64
  var p99: UInt64
  var max: UInt64
  var warnings: Int
  var tcpConnections: Int
  var streams: String
  var serverCPU: Double?
  var serverMB: Double?

  static let csvHeader =
    "time,phase,published,publish_rate,first_deliveries,first_delivery_rate,delivery_rate,in_flight,ack_waiters,backlog,footprint_mb,resident_mb,tracking_mb,threads,cpu_cores,ack_rpc_rate,extend_rpc_rate,nack_rpc_rate,ids_per_ack_rpc,p50_us,p99_us,max_us,warnings,tcp_connections,streams,server_cpu,server_mb"

  var csvLine: String {
    [
      String(format: "%.1f", time), phase, "\(published)", String(format: "%.0f", publishRate),
      "\(firstDeliveries)", String(format: "%.0f", firstDeliveryRate),
      String(format: "%.0f", deliveryRate), "\(inFlight)", "\(ackWaiters)", "\(backlog)",
      String(format: "%.1f", footprintMB), String(format: "%.1f", residentMB),
      String(format: "%.1f", trackingMB), "\(threads)", String(format: "%.2f", cpuCores),
      String(format: "%.1f", ackRPCRate), String(format: "%.1f", extendRPCRate),
      String(format: "%.1f", nackRPCRate), String(format: "%.0f", idsPerAckRPC), "\(p50)", "\(p99)",
      "\(max)", "\(warnings)", "\(tcpConnections)", streams,
      serverCPU.map { String(format: "%.2f", $0) } ?? "", serverMB.map { String(format: "%.0f", $0) } ?? "",
    ].joined(separator: ",")
  }

  var line: String {
    var text = String(
      format: "%6.0fs %-8@ pub %7.0f/s  recv %7.0f/s (all %7.0f/s)  inflight %6d ackwait %6d backlog %8d  mem %6.0fMB (rss %6.0f, track %4.0f)  thr %3d  cpu %5.2f  ack %5.1f/s ext %5.1f/s nack %5.1f/s (%4.0f ids/ack)  lat p50 %@ p99 %@ max %@  warn %d tcp %d streams %@",
      time, phase as NSString, publishRate, firstDeliveryRate, deliveryRate, inFlight, ackWaiters, backlog,
      footprintMB, residentMB, trackingMB, threads, cpuCores, ackRPCRate, extendRPCRate,
      nackRPCRate, idsPerAckRPC, formatMicros(p50) as NSString, formatMicros(p99) as NSString,
      formatMicros(max) as NSString, warnings, tcpConnections, streams as NSString)
    if let serverCPU, let serverMB {
      text += String(format: "  server cpu %.2f mem %.0fMB", serverCPU, serverMB)
    }
    return text
  }
}

/// Periodically records process, library and correctness metrics.
final class Sampler: Sendable {

  private struct State {
    var samples: [Sample] = []
    var phase = "setup"
    var lastTime = 0.0
    var lastPublished = 0
    var lastFirst = 0
    var lastDeliveries = 0
    var lastCPU = 0.0
    var lastServerCPU: Double?
    var lastTransport = TransportStats.Counts()
    var peakFootprint: UInt64 = 0
    var peakResident: UInt64 = 0
    var peakThreads = 0
    var peakInFlight = 0
    var peakAckWaiters = 0
  }

  let config: Config
  let trackers: [SubscriptionTracker]
  let load: LoadGenerator
  let startNanos = monotonicNanos()
  let service = Mutex<PubSubService?>(nil)
  let subscribers = Mutex<[PullSubscriber<StressHandler>]>([])
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

  var peaks: (footprint: UInt64, resident: UInt64, threads: Int, inFlight: Int, ackWaiters: Int) {
    state.withLock {
      ($0.peakFootprint, $0.peakResident, $0.peakThreads, $0.peakInFlight, $0.peakAckWaiters)
    }
  }

  func recordPeaks(_ snapshot: ProcessSnapshot) {
    state.withLock { state in
      state.peakFootprint = max(state.peakFootprint, snapshot.footprintBytes)
      state.peakResident = max(state.peakResident, snapshot.residentBytes)
      state.peakThreads = max(state.peakThreads, snapshot.threads)
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
    let transport = TransportStats.shared.snapshot
    let warnings = LogCounter.shared.total(atLeast: .warning)
    let streams = service.withLock { $0?.pool.activeStreamCounts.map(String.init).joined(separator: "+") } ?? "-"
    let server = config.serverPID.flatMap(ProcessMetrics.otherProcess)
    let backlog = pendingDeliveries()
    let ackWaiters = subscribers.withLock { $0 }.reduce(0) { $0 + $1.ackBatcher.waiterCount }

    let sample = state.withLock { state in
      let interval = max(time - state.lastTime, 0.001)
      let ackRPCs = transport.ackRPCs - state.lastTransport.ackRPCs
      let ackIDs = transport.ackIDs - state.lastTransport.ackIDs
      let sample = Sample(
        time: time, phase: state.phase, published: published,
        publishRate: Double(published - state.lastPublished) / interval,
        firstDeliveries: counts.firstDeliveries,
        firstDeliveryRate: Double(counts.firstDeliveries - state.lastFirst) / interval,
        deliveryRate: Double(counts.deliveries - state.lastDeliveries) / interval,
        inFlight: counts.inFlight, ackWaiters: ackWaiters, backlog: backlog,
        footprintMB: megabytes(process.footprintBytes), residentMB: megabytes(process.residentBytes),
        trackingMB: Double(tracking) / 1_048_576, threads: process.threads,
        cpuCores: (process.cpuSeconds - state.lastCPU) / interval,
        ackRPCRate: Double(ackRPCs) / interval,
        extendRPCRate: Double(transport.extendRPCs - state.lastTransport.extendRPCs) / interval,
        nackRPCRate: Double(transport.nackRPCs - state.lastTransport.nackRPCs) / interval,
        idsPerAckRPC: ackRPCs > 0 ? Double(ackIDs) / Double(ackRPCs) : 0,
        p50: latency.percentile(50), p99: latency.percentile(99), max: latency.maximum,
        warnings: warnings, tcpConnections: transport.tcpConnections, streams: streams,
        serverCPU: server.flatMap { server in
          state.lastServerCPU.map { (server.cpuSeconds - $0) / interval }
        },
        serverMB: server.map { megabytes($0.residentBytes) })
      state.lastTime = time
      state.lastPublished = published
      state.lastFirst = counts.firstDeliveries
      state.lastDeliveries = counts.deliveries
      state.lastCPU = process.cpuSeconds
      state.lastServerCPU = server?.cpuSeconds
      state.lastTransport = transport
      state.peakFootprint = max(state.peakFootprint, process.footprintBytes)
      state.peakResident = max(state.peakResident, process.residentBytes)
      state.peakThreads = max(state.peakThreads, process.threads)
      state.peakInFlight = max(state.peakInFlight, counts.inFlight)
      state.peakAckWaiters = max(state.peakAckWaiters, ackWaiters)
      state.samples.append(sample)
      return sample
    }
    print(sample.line)
    csv?.write(Data((sample.csvLine + "\n").utf8))
    return sample
  }
}
