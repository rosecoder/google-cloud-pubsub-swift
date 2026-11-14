import Synchronization

/// A clock whose time only moves when ``advance(by:)`` is called.
///
/// Uses the instants of `ContinuousClock`, starting at the real time it was created, so it can
/// drive code that stores `ContinuousClock.Instant`.
final class TestClock: Clock, Sendable {

  typealias Instant = ContinuousClock.Instant

  private struct Sleeper {
    let deadline: Instant
    let continuation: CheckedContinuation<Void, any Error>
  }

  private struct State {
    var now = ContinuousClock.now
    var sleepers: [UInt64: Sleeper] = [:]
    var nextSleeperID: UInt64 = 0
  }

  private let state = Mutex(State())

  var now: Instant {
    state.withLock { $0.now }
  }

  var minimumResolution: Duration {
    .zero
  }

  var sleeperCount: Int {
    state.withLock { $0.sleepers.count }
  }

  func sleep(until deadline: Instant, tolerance: Duration?) async throws {
    let id = state.withLock { state in
      defer { state.nextSleeperID += 1 }
      return state.nextSleeperID
    }
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = state.withLock { state in
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          if deadline <= state.now {
            return .success(())
          }
          state.sleepers[id] = Sleeper(deadline: deadline, continuation: continuation)
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      state.withLock { $0.sleepers.removeValue(forKey: id) }?
        .continuation.resume(throwing: CancellationError())
    }
  }

  func advance(by duration: Duration) {
    let due = state.withLock { state in
      state.now = state.now.advanced(by: duration)
      let now = state.now
      let due = state.sleepers.filter { $0.value.deadline <= now }
      for id in due.keys {
        state.sleepers[id] = nil
      }
      return due.values.map(\.continuation)
    }
    for continuation in due {
      continuation.resume()
    }
  }
}
