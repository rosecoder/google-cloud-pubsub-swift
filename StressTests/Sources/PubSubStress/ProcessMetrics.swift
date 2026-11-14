import Foundation

#if canImport(Darwin)
  import Darwin
#elseif canImport(Glibc)
  import Glibc
#endif

struct ProcessSnapshot: Sendable {
  var residentBytes: UInt64 = 0
  var cpuSeconds: Double = 0
  var threads: Int = 0
  var fileDescriptors: Int = 0
  /// Established TCP connections to port 443 in the process's network namespace (Linux only).
  var tlsConnections: Int = 0
}

enum ProcessMetrics {

  static func snapshot() -> ProcessSnapshot {
    var snapshot = ProcessSnapshot()
    var usage = rusage()
    if getrusage(RUSAGE_SELF, &usage) == 0 {
      snapshot.cpuSeconds = seconds(usage.ru_utime) + seconds(usage.ru_stime)
    }
    #if os(Linux)
      if let status = try? String(contentsOfFile: "/proc/self/status", encoding: .utf8) {
        for line in status.split(separator: "\n") {
          if line.hasPrefix("VmRSS:") {
            snapshot.residentBytes = (UInt64(field(line, 1)) ?? 0) * 1024
          } else if line.hasPrefix("Threads:") {
            snapshot.threads = Int(field(line, 1)) ?? 0
          }
        }
      }
      snapshot.fileDescriptors =
        ((try? FileManager.default.contentsOfDirectory(atPath: "/proc/self/fd"))?.count ?? 1) - 1
      snapshot.tlsConnections = ["/proc/self/net/tcp", "/proc/self/net/tcp6"].reduce(0) {
        $0 + establishedConnections(toPort: 443, table: $1)
      }
    #else
      var info = mach_task_basic_info()
      var count = mach_msg_type_number_t(
        MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
      let result = withUnsafeMutablePointer(to: &info) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
          task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
        }
      }
      if result == KERN_SUCCESS {
        snapshot.residentBytes = UInt64(info.resident_size)
      }
      var threads: thread_act_array_t?
      var threadCount: mach_msg_type_number_t = 0
      if task_threads(mach_task_self_, &threads, &threadCount) == KERN_SUCCESS, let threads {
        snapshot.threads = Int(threadCount)
        for index in 0..<Int(threadCount) {
          mach_port_deallocate(mach_task_self_, threads[index])
        }
        vm_deallocate(
          mach_task_self_, vm_address_t(UInt(bitPattern: threads)),
          vm_size_t(Int(threadCount) * MemoryLayout<thread_t>.stride))
      }
      snapshot.fileDescriptors =
        ((try? FileManager.default.contentsOfDirectory(atPath: "/dev/fd"))?.count ?? 1) - 1
    #endif
    return snapshot
  }

  private static func field(_ line: Substring, _ index: Int) -> Substring {
    let fields = line.split(whereSeparator: { $0 == " " || $0 == "\t" })
    return index < fields.count ? fields[index] : ""
  }

  private static func establishedConnections(toPort port: Int, table path: String) -> Int {
    guard let table = try? String(contentsOfFile: path, encoding: .utf8) else { return 0 }
    let portHex = String(format: ":%04X", port)
    return table.split(separator: "\n").dropFirst().reduce(0) { count, line in
      let fields = line.split(separator: " ")
      // fields: sl local_address rem_address st ...
      guard fields.count > 3, fields[2].hasSuffix(portHex), fields[3] == "01" else { return count }
      return count + 1
    }
  }

  private static func seconds(_ value: timeval) -> Double {
    Double(value.tv_sec) + Double(value.tv_usec) / 1e6
  }
}

/// Least squares slope of `values` over `times`, in value units per time unit.
func slope(times: [Double], values: [Double]) -> Double {
  guard times.count >= 2 else { return 0 }
  let meanT = times.reduce(0, +) / Double(times.count)
  let meanV = values.reduce(0, +) / Double(values.count)
  var numerator = 0.0
  var denominator = 0.0
  for (t, v) in zip(times, values) {
    numerator += (t - meanT) * (v - meanV)
    denominator += (t - meanT) * (t - meanT)
  }
  return denominator == 0 ? 0 : numerator / denominator
}

func megabytes(_ bytes: UInt64) -> Double {
  Double(bytes) / 1_048_576
}

/// Monotonic nanoseconds, for durations within this process.
func monotonicNanos() -> UInt64 {
  var spec = timespec()
  clock_gettime(CLOCK_MONOTONIC, &spec)
  return UInt64(spec.tv_sec) * 1_000_000_000 + UInt64(spec.tv_nsec)
}

/// Wall clock nanoseconds, written into messages so latency can be measured across processes on
/// the same host.
func wallClockNanos() -> UInt64 {
  var spec = timespec()
  clock_gettime(CLOCK_REALTIME, &spec)
  return UInt64(spec.tv_sec) * 1_000_000_000 + UInt64(spec.tv_nsec)
}
