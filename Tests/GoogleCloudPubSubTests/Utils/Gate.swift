import Synchronization

final class Gate: Sendable {

  private struct State {
    var isOpen = false
    var waiters: [UInt64: CheckedContinuation<Void, any Error>] = [:]
    var nextWaiterID: UInt64 = 0
  }

  private let state = Mutex(State())

  /// Waits until the gate is open.
  ///
  /// - Throws: `CancellationError` if the task is cancelled before the gate opens.
  func wait() async throws {
    let id = state.withLock { state in
      defer { state.nextWaiterID += 1 }
      return state.nextWaiterID
    }
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let result: Result<Void, any Error>? = state.withLock { state in
          if state.isOpen {
            return .success(())
          }
          if Task.isCancelled {
            return .failure(CancellationError())
          }
          state.waiters[id] = continuation
          return nil
        }
        if let result {
          continuation.resume(with: result)
        }
      }
    } onCancel: {
      state.withLock { $0.waiters.removeValue(forKey: id) }?
        .resume(throwing: CancellationError())
    }
  }

  func open() {
    let waiters = state.withLock { state in
      state.isOpen = true
      defer { state.waiters = [:] }
      return state.waiters.values
    }
    for waiter in waiters {
      waiter.resume()
    }
  }
}
