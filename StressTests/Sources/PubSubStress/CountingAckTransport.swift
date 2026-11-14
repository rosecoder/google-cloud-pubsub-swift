import GRPCCore
import Synchronization

@testable import GoogleCloudPubSub

/// Process wide counters of transport level activity.
final class TransportStats: Sendable {

  static let shared = TransportStats()

  struct Counts: Sendable {
    var ackRPCs = 0
    var ackIDs = 0
    var extendRPCs = 0
    var extendIDs = 0
    var nackRPCs = 0
    var nackIDs = 0
    var failedRPCs: [String: Int] = [:]
    var rpcLatency = Histogram()
    var tcpConnections = 0
    var http2Streams = 0
  }

  private let counts = Mutex(Counts())

  func recordRPC(
    _ operation: AckDeadlineModification?, ids: Int, micros: UInt64, error: (any Error)?
  ) {
    counts.withLock { counts in
      switch operation {
      case nil:
        counts.ackRPCs += 1
        counts.ackIDs += ids
      case .nack:
        counts.nackRPCs += 1
        counts.nackIDs += ids
      case .extend:
        counts.extendRPCs += 1
        counts.extendIDs += ids
      }
      counts.rpcLatency.record(micros: micros)
      if let error {
        let kind = operation.map { $0 == .nack ? "nack" : "modack" } ?? "ack"
        let code = (error as? RPCError).map { "\($0.code)" } ?? String(describing: type(of: error))
        counts.failedRPCs["\(kind):\(code)", default: 0] += 1
      }
    }
  }

  func recordTCPConnection() {
    counts.withLock { $0.tcpConnections += 1 }
  }

  func recordHTTP2Stream() {
    counts.withLock { $0.http2Streams += 1 }
  }

  var snapshot: Counts {
    counts.withLock { $0 }
  }
}

/// Wraps the library's gRPC ack transport to count RPCs, IDs, latency and failures.
struct CountingAckTransport: AckTransport {

  let inner: any AckTransport

  func acknowledge(_ ids: [AckID]) async throws {
    let start = monotonicNanos()
    do {
      try await inner.acknowledge(ids)
      TransportStats.shared.recordRPC(nil, ids: ids.count, micros: (monotonicNanos() - start) / 1_000, error: nil)
    } catch {
      TransportStats.shared.recordRPC(nil, ids: ids.count, micros: (monotonicNanos() - start) / 1_000, error: error)
      throw error
    }
  }

  func modifyAckDeadline(_ ids: [AckID], to modification: AckDeadlineModification) async throws {
    let start = monotonicNanos()
    do {
      try await inner.modifyAckDeadline(ids, to: modification)
      TransportStats.shared.recordRPC(
        modification, ids: ids.count, micros: (monotonicNanos() - start) / 1_000, error: nil)
    } catch {
      TransportStats.shared.recordRPC(
        modification, ids: ids.count, micros: (monotonicNanos() - start) / 1_000, error: error)
      throw error
    }
  }
}
