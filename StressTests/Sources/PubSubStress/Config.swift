import Foundation
import Logging

struct Config: Sendable, CustomStringConvertible {

  enum Mode: String, Sendable {
    /// Publish (optionally a backlog first), receive, drain, verify.
    case run
    /// Like `run`, but the subscriber ServiceGroup is gracefully shut down and restarted
    /// `cycles` times while publishing continues.
    case churn
    /// Deletes every topic and subscription named `pubsub-swift-stress-*` in the project.
    case cleanup
  }

  static let namePrefix = "pubsub-swift-stress-"

  var mode = Mode.run
  var projectID = "stress-local"
  var runID = Config.makeRunID()
  var isEmulator = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"] != nil

  // Topology
  var subscriptions = 10
  var topics: Int?

  // Load
  var publishers = 16
  var batchSize = 100
  /// Messages per second published in total, 0 for as fast as backpressure allows.
  var rate = 0.0
  /// `rate:seconds` steps replacing `rate` and `duration`, e.g. `10000:180,30000:180`.
  var rateSteps: [(rate: Double, seconds: Double)] = []
  /// Publishing pauses while more deliveries than this are expected but not yet handled.
  var maxBacklog = 100_000
  var messageSize = 1024
  /// Messages published to each topic before subscribers start.
  var backlog = 0
  var duration = 60.0
  var drainTimeout = 120.0
  /// Whether the load generator gets its own gRPC connections instead of sharing the
  /// subscribers' PubSubService.
  var separatePublisherService = true

  // Handler behaviour
  var sleepMs = 0.0
  /// Sleep is uniformly random in `sleepMs ± sleepJitter * sleepMs`.
  var sleepJitter = 0.0
  var cpuMs = 0.0
  var failRate = 0.0
  var slowRate = 0.0
  var slowMs = 30_000.0

  // Subscriber options
  var maxOutstandingMessages = 1_000
  var maxOutstandingBytes = 100 * 1024 * 1024
  var ackDeadline = 10.0
  /// Subscription message retention in seconds; the emulator keeps messages in memory this long.
  var retention = 3600.0
  var shutdownTimeout = 30.0
  var nackImmediately = false
  var maxStreamsPerConnection = 80

  // Churn
  var cycles = 20
  var cycleMin = 3.0
  var cycleMax = 10.0
  /// Seconds a churn cycle's graceful shutdown may take before it counts as hung.
  var hangTimeout = 90.0

  // Reporting
  var sampleInterval = 5.0
  var logLevel = Logger.Level.warning
  var outputDirectory = "/tmp/pubsub-stress-results"
  var serverPID: Int32?
  var label = ""

  var topicCount: Int {
    max(1, min(topics ?? subscriptions, max(subscriptions, 1)))
  }

  var namePrefix: String {
    Config.namePrefix + runID + "-"
  }

  static func makeRunID() -> String {
    let alphabet = Array("abcdefghijklmnopqrstuvwxyz0123456789")
    return String((0..<6).map { _ in alphabet.randomElement()! })
  }

  static let usage = """
    PubSubStress [--key=value ...]   (also read from STRESS_<KEY> environment variables)

    Targets the emulator when PUBSUB_EMULATOR_HOST is set, otherwise Pub/Sub with Application
    Default Credentials. The project comes from --project or STRESS_PROJECT_ID.

      mode=run|churn|cleanup    subs=10 topics=<subs> publishers=16 batch=100
      rate=0 (msgs/s, 0 = max)  rate-steps=10000:180,30000:180   max-backlog=100000
      size=1024 backlog=0 (per topic, before subscribers start) duration=60 drain=120
      separate-publisher=true
      sleep-ms=0 sleep-jitter=0 cpu-ms=0 fail-rate=0 slow-rate=0 slow-ms=30000
      max-outstanding=1000 max-outstanding-bytes=104857600 ack-deadline=10 retention=3600
      shutdown-timeout=30 nack-immediately=false max-streams-per-connection=80
      cycles=20 cycle-min=3 cycle-max=10 hang-timeout=90
      sample=5 log-level=warning out=/tmp/pubsub-stress-results server-pid=<pid> label=<name>
    """

  static func parse(arguments: [String], environment: [String: String]) throws -> Config {
    var values: [String: String] = [:]
    for (key, value) in environment where key.hasPrefix("STRESS_") {
      values[key.dropFirst(7).lowercased().replacingOccurrences(of: "_", with: "-")] = value
    }
    for argument in arguments {
      if argument == "--help" || argument == "-h" {
        print(usage)
        exit(0)
      }
      let trimmed = argument.hasPrefix("--") ? String(argument.dropFirst(2)) : argument
      let parts = trimmed.split(separator: "=", maxSplits: 1).map(String.init)
      guard parts.count == 2 else {
        throw ConfigError.invalid(argument)
      }
      values[parts[0]] = parts[1]
    }

    var config = Config()
    for (key, value) in values {
      try config.set(key, value)
    }
    return config
  }

  private mutating func set(_ key: String, _ value: String) throws {
    func double() throws -> Double {
      guard let parsed = Double(value) else { throw ConfigError.invalid("\(key)=\(value)") }
      return parsed
    }
    func int() throws -> Int {
      guard let parsed = Int(value) else { throw ConfigError.invalid("\(key)=\(value)") }
      return parsed
    }
    func bool() -> Bool {
      ["1", "true", "yes"].contains(value.lowercased())
    }
    switch key {
    case "mode":
      guard let mode = Mode(rawValue: value) else { throw ConfigError.invalid("\(key)=\(value)") }
      self.mode = mode
    case "project", "project-id": projectID = value
    case "run-id": runID = value
    case "subs", "subscriptions": subscriptions = try int()
    case "topics": topics = try int()
    case "publishers": publishers = try int()
    case "batch": batchSize = try int()
    case "rate": rate = try double()
    case "rate-steps":
      rateSteps = try value.split(separator: ",").map { step in
        let parts = step.split(separator: ":")
        guard parts.count == 2, let rate = Double(parts[0]), let seconds = Double(parts[1]) else {
          throw ConfigError.invalid("\(key)=\(value)")
        }
        return (rate, seconds)
      }
    case "max-backlog": maxBacklog = try int()
    case "size": messageSize = try int()
    case "backlog": backlog = try int()
    case "duration": duration = try double()
    case "drain": drainTimeout = try double()
    case "separate-publisher": separatePublisherService = bool()
    case "sleep-ms": sleepMs = try double()
    case "sleep-jitter": sleepJitter = try double()
    case "cpu-ms": cpuMs = try double()
    case "fail-rate": failRate = try double()
    case "slow-rate": slowRate = try double()
    case "slow-ms": slowMs = try double()
    case "max-outstanding": maxOutstandingMessages = try int()
    case "max-outstanding-bytes": maxOutstandingBytes = try int()
    case "ack-deadline": ackDeadline = try double()
    case "retention": retention = try double()
    case "shutdown-timeout": shutdownTimeout = try double()
    case "nack-immediately": nackImmediately = bool()
    case "max-streams-per-connection": maxStreamsPerConnection = try int()
    case "cycles": cycles = try int()
    case "cycle-min": cycleMin = try double()
    case "cycle-max": cycleMax = try double()
    case "hang-timeout": hangTimeout = try double()
    case "sample": sampleInterval = try double()
    case "log-level":
      guard let level = Logger.Level(rawValue: value) else {
        throw ConfigError.invalid("\(key)=\(value)")
      }
      logLevel = level
    case "out": outputDirectory = value
    case "server-pid": serverPID = Int32(try int())
    case "label": label = value
    case "emulator-host":
      break  // Read by the library from PUBSUB_EMULATOR_HOST.
    default:
      throw ConfigError.unknown(key)
    }
  }

  var description: String {
    var parts = [
      "mode=\(mode.rawValue)", "target=\(isEmulator ? "emulator" : "gcp")", "run=\(runID)",
      "subs=\(subscriptions)", "topics=\(topicCount)", "publishers=\(publishers)",
      "batch=\(batchSize)", "size=\(messageSize)",
    ]
    if rateSteps.isEmpty {
      parts += ["rate=\(rate == 0 ? "max" : String(Int(rate)))", "duration=\(Int(duration))s"]
    } else {
      parts.append("rate-steps=" + rateSteps.map { "\(Int($0.rate)):\(Int($0.seconds))" }.joined(separator: ","))
    }
    parts += [
      "max-backlog=\(maxBacklog)", "backlog=\(backlog)", "drain=\(Int(drainTimeout))s",
      "sleep-ms=\(sleepMs)", "sleep-jitter=\(sleepJitter)", "cpu-ms=\(cpuMs)",
      "fail-rate=\(failRate)", "slow-rate=\(slowRate)", "slow-ms=\(slowMs)",
      "max-outstanding=\(maxOutstandingMessages)", "max-outstanding-bytes=\(maxOutstandingBytes)",
      "ack-deadline=\(Int(ackDeadline))", "shutdown=\(nackImmediately ? "nackImmediately" : "wait(\(Int(shutdownTimeout))s)")",
      "streams/conn=\(maxStreamsPerConnection)", "separate-publisher=\(separatePublisherService)",
    ]
    if mode == .churn {
      parts += ["cycles=\(cycles)", "cycle=\(cycleMin)-\(cycleMax)s"]
    }
    return parts.joined(separator: " ")
  }
}

enum ConfigError: Error, CustomStringConvertible {
  case invalid(String)
  case unknown(String)

  var description: String {
    switch self {
    case .invalid(let argument): "Invalid argument: \(argument)\n\n\(Config.usage)"
    case .unknown(let key): "Unknown option: \(key)\n\n\(Config.usage)"
    }
  }
}
