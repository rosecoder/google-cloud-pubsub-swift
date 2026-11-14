import Foundation
import GRPCCore
import Synchronization

@testable import GoogleCloudPubSub

/// Layout of the first 20 bytes of every message body; the rest is filler.
struct MessageHeader {
  static let size = 20

  let seq: UInt64
  let publishedAtNanos: UInt64
  let topicIndex: UInt32

  init(seq: UInt64, publishedAtNanos: UInt64, topicIndex: UInt32) {
    self.seq = seq
    self.publishedAtNanos = publishedAtNanos
    self.topicIndex = topicIndex
  }

  init?(_ body: Data) {
    guard body.count >= Self.size else { return nil }
    (seq, publishedAtNanos, topicIndex) = body.withUnsafeBytes { bytes in
      (
        UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: 0, as: UInt64.self)),
        UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: 8, as: UInt64.self)),
        UInt32(littleEndian: bytes.loadUnaligned(fromByteOffset: 16, as: UInt32.self))
      )
    }
  }

  func write(into body: inout Data) {
    body.withUnsafeMutableBytes { bytes in
      bytes.storeBytes(of: seq.littleEndian, toByteOffset: 0, as: UInt64.self)
      bytes.storeBytes(of: publishedAtNanos.littleEndian, toByteOffset: 8, as: UInt64.self)
      bytes.storeBytes(of: topicIndex.littleEndian, toByteOffset: 16, as: UInt32.self)
    }
  }
}

/// Publishes numbered messages round robin over the topics, paced to a rate and paused while too
/// many deliveries are outstanding.
final class LoadGenerator: Sendable {

  struct Stats: Sendable {
    var publishedMessages = 0
    var publishRPCs = 0
    var failedRPCs: [String: Int] = [:]
    var latency = Histogram()
  }

  private struct Pacer {
    var rate: Double
    var nextAtNanos: UInt64 = 0
  }

  let publisher: Publisher
  let topics: [Topic<DataMessage>]
  let batchSize: Int
  let concurrency: Int
  let maxBacklog: Int
  private let template: Data
  private let nextTopic = Atomic<Int>(0)
  private let allocated: Mutex<[UInt64]>
  private let unconfirmed: Mutex<[[Range<UInt64>]]>
  private let stats = Mutex(Stats())
  private let pacer: Mutex<Pacer>
  private let isStopped = Atomic<Bool>(false)
  private let pendingDeliveries: @Sendable () -> Int

  init(
    publisher: Publisher, topics: [Topic<DataMessage>], config: Config,
    pendingDeliveries: @escaping @Sendable () -> Int
  ) {
    self.publisher = publisher
    self.topics = topics
    self.batchSize = config.batchSize
    self.concurrency = config.publishers
    self.maxBacklog = config.maxBacklog
    self.template = Data((0..<max(config.messageSize, MessageHeader.size)).map { UInt8(truncatingIfNeeded: $0 &* 31) })
    self.allocated = Mutex(Array(repeating: 0, count: topics.count))
    self.unconfirmed = Mutex(Array(repeating: [], count: topics.count))
    self.pacer = Mutex(Pacer(rate: config.rate))
    self.pendingDeliveries = pendingDeliveries
  }

  var rate: Double {
    get { pacer.withLock { $0.rate } }
    set { pacer.withLock { $0.rate = newValue } }
  }

  func allocatedCount(topic: Int) -> UInt64 {
    allocated.withLock { $0[topic] }
  }

  var allocatedCounts: [UInt64] {
    allocated.withLock { $0 }
  }

  func unconfirmedRanges(topic: Int) -> [Range<UInt64>] {
    unconfirmed.withLock { $0[topic] }
  }

  var snapshot: Stats {
    stats.withLock { $0 }
  }

  func stop() {
    isStopped.store(true, ordering: .relaxed)
  }

  /// Publishes until ``stop()`` is called or the task is cancelled.
  func run() async {
    isStopped.store(false, ordering: .relaxed)
    await withDiscardingTaskGroup { group in
      for _ in 0..<concurrency {
        group.addTask {
          while !self.isStopped.load(ordering: .relaxed), !Task.isCancelled {
            await self.waitForCapacity()
            if self.isStopped.load(ordering: .relaxed) || Task.isCancelled { break }
            let topic = self.nextTopic.wrappingAdd(1, ordering: .relaxed).oldValue % self.topics.count
            await self.publishBatch(to: topic, count: self.batchSize)
          }
        }
      }
    }
  }

  /// Publishes `count` messages to every topic as fast as possible.
  func publishBacklog(perTopic count: Int) async {
    let batches = (0..<topics.count).flatMap { topic in
      stride(from: 0, to: count, by: batchSize).map { (topic, min(batchSize, count - $0)) }
    }
    let next = Atomic<Int>(0)
    await withDiscardingTaskGroup { group in
      for _ in 0..<concurrency {
        group.addTask {
          while !Task.isCancelled {
            let index = next.wrappingAdd(1, ordering: .relaxed).oldValue
            guard index < batches.count else { return }
            await self.publishBatch(to: batches[index].0, count: batches[index].1)
          }
        }
      }
    }
  }

  private func waitForCapacity() async {
    while pendingDeliveries() > maxBacklog, !isStopped.load(ordering: .relaxed) {
      try? await Task.sleep(for: .milliseconds(20))
    }
    let wakeAt = pacer.withLock { pacer -> UInt64? in
      guard pacer.rate > 0 else { return nil }
      let now = monotonicNanos()
      // Don't burst to catch up after a pause.
      let slot = max(pacer.nextAtNanos, now &- 100_000_000)
      pacer.nextAtNanos = max(slot, now) + UInt64(Double(batchSize) / pacer.rate * 1e9)
      return slot > now ? slot : nil
    }
    if let wakeAt {
      try? await Task.sleep(for: .nanoseconds(Int64(wakeAt - monotonicNanos())))
    }
  }

  private func publishBatch(to topic: Int, count: Int) async {
    let start = allocated.withLock { allocated in
      defer { allocated[topic] += UInt64(count) }
      return allocated[topic]
    }
    let now = monotonicNanos()
    let messages = (0..<count).map { offset in
      var body = template
      MessageHeader(seq: start + UInt64(offset), publishedAtNanos: now, topicIndex: UInt32(topic))
        .write(into: &body)
      return try! OutgoingDataMessage(body: body)
    }
    do {
      try await publisher.publish(to: topics[topic], messages: messages)
      let micros = (monotonicNanos() - now) / 1_000
      stats.withLock { stats in
        stats.publishedMessages += count
        stats.publishRPCs += 1
        stats.latency.record(micros: micros)
      }
    } catch {
      unconfirmed.withLock { $0[topic].append(start..<(start + UInt64(count))) }
      let code = (error as? RPCError).map { "\($0.code)" } ?? String(describing: type(of: error))
      stats.withLock { $0.failedRPCs[code, default: 0] += 1 }
    }
  }
}
