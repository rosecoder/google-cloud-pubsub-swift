import Darwin
import Foundation

struct ProcessSnapshot: Sendable {
  var footprintBytes: UInt64 = 0
  var residentBytes: UInt64 = 0
  var cpuSeconds: Double = 0
  var threads: Int = 0
}

enum ProcessMetrics {

  static func snapshot() -> ProcessSnapshot {
    var snapshot = ProcessSnapshot()
    let task = mach_task_self_

    var vmInfo = task_vm_info_data_t()
    var vmCount = mach_msg_type_number_t(
      MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
    let vmResult = withUnsafeMutablePointer(to: &vmInfo) {
      $0.withMemoryRebound(to: integer_t.self, capacity: Int(vmCount)) {
        task_info(task, task_flavor_t(TASK_VM_INFO), $0, &vmCount)
      }
    }
    if vmResult == KERN_SUCCESS {
      snapshot.footprintBytes = vmInfo.phys_footprint
      snapshot.residentBytes = vmInfo.resident_size
    }

    var usage = rusage()
    if getrusage(RUSAGE_SELF, &usage) == 0 {
      snapshot.cpuSeconds = seconds(usage.ru_utime) + seconds(usage.ru_stime)
    }

    var threads: thread_act_array_t?
    var threadCount: mach_msg_type_number_t = 0
    if task_threads(task, &threads, &threadCount) == KERN_SUCCESS, let threads {
      snapshot.threads = Int(threadCount)
      for index in 0..<Int(threadCount) {
        mach_port_deallocate(task, threads[index])
      }
      vm_deallocate(
        task, vm_address_t(UInt(bitPattern: threads)),
        vm_size_t(Int(threadCount) * MemoryLayout<thread_t>.stride))
    }
    return snapshot
  }

  /// CPU seconds and resident bytes of another process of the same user, such as the emulator.
  static func otherProcess(_ pid: Int32) -> (cpuSeconds: Double, residentBytes: UInt64)? {
    var info = proc_taskinfo()
    let size = Int32(MemoryLayout<proc_taskinfo>.size)
    guard proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &info, size) == size else {
      return nil
    }
    var timebase = mach_timebase_info_data_t()
    mach_timebase_info(&timebase)
    let nanos =
      Double(info.pti_total_user + info.pti_total_system) * Double(timebase.numer)
      / Double(timebase.denom)
    return (nanos / 1e9, info.pti_resident_size)
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
