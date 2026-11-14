extension Task where Failure == Never {

  /// Waits like `value`, but cancels the task when the waiting task is cancelled, so that a time
  /// limit stops a test instead of it waiting forever.
  var cancellableValue: Success {
    get async {
      await withTaskCancellationHandler { await value } onCancel: { cancel() }
    }
  }
}

extension Task where Failure == any Error {

  /// Waits like `value`, but cancels the task when the waiting task is cancelled, so that a time
  /// limit stops a test instead of it waiting forever.
  var cancellableValue: Success {
    get async throws {
      try await withTaskCancellationHandler { try await value } onCancel: { cancel() }
    }
  }
}
