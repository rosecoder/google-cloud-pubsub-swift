/// - Returns: Whether `task` finished within `limit`. It's cancelled otherwise, so a hang fails the
///   test instead of blocking it.
func finishes(_ task: Task<Void, any Error>, within limit: Duration) async throws -> Bool {
  try await withThrowingTaskGroup(of: Bool.self) { group in
    group.addTask {
      try await task.value
      return true
    }
    group.addTask {
      try await Task.sleep(for: limit)
      return false
    }
    let hasFinished = try await group.next()!
    if !hasFinished {
      task.cancel()
    }
    group.cancelAll()
    return hasFinished
  }
}
