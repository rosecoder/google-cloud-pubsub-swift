import Foundation
import Logging
import Synchronization

/// Counts log messages by level and normalized text, echoing the first few of each kind.
final class LogCounter: Sendable {

  static let shared = LogCounter()

  private static let echoLimit = 3

  private let counts = Mutex<[String: Int]>([:])

  func record(level: Logger.Level, label: String, message: String) {
    let key = "\(level) [\(Self.normalize(label: label))] \(Self.normalize(message: message))"
    let count = counts.withLock { counts in
      counts[key, default: 0] += 1
      return counts[key]!
    }
    if count <= Self.echoLimit, level >= .warning {
      FileHandle.standardError.write(Data("[log] \(level) \(label): \(message)\n".utf8))
    }
  }

  var snapshot: [String: Int] {
    counts.withLock { $0 }
  }

  func total(atLeast level: Logger.Level) -> Int {
    counts.withLock { counts in
      counts.reduce(0) { sum, entry in
        let entryLevel = Logger.Level(rawValue: String(entry.key.prefix { $0 != " " }))
        return sum + ((entryLevel ?? .trace) >= level ? entry.value : 0)
      }
    }
  }

  private static func normalize(label: String) -> String {
    if let range = label.range(of: Config.namePrefix) {
      return label[..<range.lowerBound] + "*"
    }
    return label
  }

  /// Replaces numbers and run specific names so messages of the same kind count together.
  static func normalize(message: String) -> String {
    var result = ""
    var lastWasDigit = false
    for character in message.prefix(240) {
      if character.isNumber {
        if !lastWasDigit {
          result.append("#")
        }
        lastWasDigit = true
      } else {
        result.append(character)
        lastWasDigit = false
      }
    }
    if let range = result.range(of: Config.namePrefix) {
      let end = result[range.upperBound...].firstIndex(where: { $0 == " " || $0 == "\"" || $0 == "," })
        ?? result.endIndex
      result.replaceSubrange(range.lowerBound..<end, with: "<stress-resource>")
    }
    return String(result.prefix(160))
  }
}

struct CountingLogHandler: LogHandler {

  let label: String
  var logLevel: Logger.Level
  var metadata: Logger.Metadata = [:]

  subscript(metadataKey key: String) -> Logger.Metadata.Value? {
    get { metadata[key] }
    set { metadata[key] = newValue }
  }

  func log(
    level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String,
    file: String, function: String, line: UInt
  ) {
    LogCounter.shared.record(level: level, label: label, message: message.description)
  }
}

/// Writes straight to standard output, unbuffered even when redirected to a file, so progress can
/// be followed while a run is in progress.
func print(_ items: Any..., separator: String = " ", terminator: String = "\n") {
  let text = items.map { "\($0)" }.joined(separator: separator) + terminator
  FileHandle.standardOutput.write(Data(text.utf8))
}
