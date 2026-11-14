import Logging
import Synchronization

/// Records the entries logged by loggers made with ``logger(label:)``, including their metadata.
final class LogRecorder: Sendable {

  struct Entry: Sendable {
    let level: Logger.Level
    let message: String
    let metadata: Logger.Metadata
  }

  private let state = Mutex<[Entry]>([])

  var entries: [Entry] {
    state.withLock { $0 }
  }

  func logger(label: String) -> Logger {
    Logger(label: label) { _ in CapturingLogHandler(recorder: self) }
  }

  fileprivate func record(_ entry: Entry) {
    state.withLock { $0.append(entry) }
  }
}

private struct CapturingLogHandler: LogHandler {

  let recorder: LogRecorder
  var metadata: Logger.Metadata = [:]
  var logLevel: Logger.Level = .trace

  subscript(metadataKey key: String) -> Logger.Metadata.Value? {
    get { metadata[key] }
    set { metadata[key] = newValue }
  }

  func log(
    level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String,
    file: String, function: String, line: UInt
  ) {
    recorder.record(
      LogRecorder.Entry(
        level: level, message: message.description,
        metadata: self.metadata.merging(metadata ?? [:]) { $1 }))
  }
}
