import Testing

struct WaitUntilTimeoutError: Error {}

/// Yields until `condition` holds. The timeout only bounds how long a failing test takes.
func waitUntil(
  timeout: Duration = .seconds(5),
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () -> Bool
) async throws {
  let deadline = ContinuousClock.now.advanced(by: timeout)
  while !condition() {
    guard ContinuousClock.now < deadline else {
      Issue.record("Condition not met within \(timeout)", sourceLocation: sourceLocation)
      throw WaitUntilTimeoutError()
    }
    await Task.yield()
  }
}
