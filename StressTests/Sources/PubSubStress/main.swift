import Dispatch
import Foundation
import Logging

let config: Config
do {
  config = try Config.parse(
    arguments: Array(CommandLine.arguments.dropFirst()),
    environment: ProcessInfo.processInfo.environment)
} catch {
  print(error)
  exit(2)
}

LoggingSystem.bootstrap { label in
  CountingLogHandler(label: label, logLevel: config.logLevel)
}

for signalNumber in [SIGINT, SIGTERM] {
  signal(signalNumber, SIG_IGN)
}
let signalSources = [SIGINT, SIGTERM].map { signalNumber in
  let source = DispatchSource.makeSignalSource(signal: signalNumber, queue: .global())
  source.setEventHandler {
    if abortRequested.exchange(true, ordering: .relaxed) {
      print("Second signal, exiting without cleanup")
      exit(130)
    }
    print("Signal received, stopping load and cleaning up (send again to exit right away)")
  }
  source.resume()
  return source
}

let admin = Admin(projectID: config.projectID)

if config.mode == .cleanup {
  let deleted = try await admin.deleteAllStressResources()
  print("Deleted \(deleted.subscriptions.count) subscriptions: \(deleted.subscriptions)")
  print("Deleted \(deleted.topics.count) topics: \(deleted.topics)")
  let leftTopics = try await admin.listTopics()
  let leftSubscriptions = try await admin.listSubscriptions()
  print("Remaining stress topics: \(leftTopics.count), subscriptions: \(leftSubscriptions.count)")
  exit(leftTopics.isEmpty && leftSubscriptions.isEmpty ? 0 : 1)
}

let resultDirectory =
  config.outputDirectory + "/" + (config.label.isEmpty ? "" : config.label + "-") + config.runID
try FileManager.default.createDirectory(atPath: resultDirectory, withIntermediateDirectories: true)

let runner = try Runner(config: config, admin: admin, resultDirectory: resultDirectory)
var runError: (any Error)?
do {
  try await runner.run()
} catch {
  runError = error
  print("Run failed: \(error)")
}
runner.sampler.sample()

let summary = runner.summary(error: runError)
print("\n===== SUMMARY =====\n" + summary)
try? summary.write(toFile: resultDirectory + "/summary.txt", atomically: true, encoding: .utf8)

print("Deleting resources...")
await admin.delete(runner.resources)
let prefix = config.namePrefix
let leftovers =
  ((try? await admin.listTopics()) ?? []) + ((try? await admin.listSubscriptions()) ?? [])
let ownLeftovers = leftovers.filter { $0.contains("/" + prefix) }
print(ownLeftovers.isEmpty ? "All resources of run \(config.runID) deleted." : "LEFTOVERS: \(ownLeftovers)")

let lost = summary.contains("LOST 0,") || runner.trackers.isEmpty ? 0 : 1
_ = signalSources
if case .shutdownHung? = runError as? StressFailure {
  exit(3)
}
exit(runError == nil && lost == 0 ? 0 : 1)
