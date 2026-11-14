import Synchronization

final class Gate: Sendable {

  private struct State {
    var isOpen = false
    var waiters: [CheckedContinuation<Void, Never>] = []
  }

  private let state = Mutex(State())

  func wait() async {
    await withCheckedContinuation { continuation in
      let isOpen = state.withLock { state in
        if !state.isOpen {
          state.waiters.append(continuation)
        }
        return state.isOpen
      }
      if isOpen {
        continuation.resume()
      }
    }
  }

  func open() {
    let waiters = state.withLock { state in
      state.isOpen = true
      defer { state.waiters = [] }
      return state.waiters
    }
    for waiter in waiters {
      waiter.resume()
    }
  }
}
