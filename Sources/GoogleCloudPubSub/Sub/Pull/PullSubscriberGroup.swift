import Logging
import ServiceLifecycle

/// Runs many subscribers as one service, shutting them all down at once.
///
/// A `ServiceGroup` shuts its services down one at a time in reverse order and waits for each to
/// return before signalling the next. Listed there individually, every ``PullSubscriber`` may wait
/// for its handlers for up to its ``PullSubscriberOptions/shutdownBehavior`` timeout while those
/// not yet signalled keep receiving messages, so shutting down 100 subscribers can take 100 such
/// timeouts. In a group they are all signalled when the group is, and the group returns once the
/// slowest has finished.
///
/// ```swift
/// let serviceGroup = ServiceGroup(
///   configuration: .init(
///     services: [publisher, PullSubscriberGroup(subscribers)],
///     gracefulShutdownSignals: [.sigterm],
///     logger: logger))
/// ```
///
/// List services the subscribers' handlers use, such as a ``Publisher``, before the group so they
/// are shut down after it. Set the service group's `maximumGracefulShutdownDuration` to at least
/// the longest `waitForProcessing` timeout plus about 2 seconds to send the final acks and close
/// the connections, and below the deadline the platform kills the process at, such as Kubernetes'
/// `terminationGracePeriodSeconds`.
///
/// If a subscriber throws, the others are shut down gracefully and ``run()`` then throws its error,
/// so a service group shuts down too. Cancelling ``run()`` cancels every subscriber right away.
public final class PullSubscriberGroup: Service {

  private static let logger = Logger(label: "pubsub.subscriber-group")

  public let subscribers: [any Service]

  /// - Parameter subscribers: Usually ``PullSubscriber``s, whose handlers may differ. Any service
  ///   that stops on graceful shutdown works.
  public init(_ subscribers: [any Service]) {
    self.subscribers = subscribers
  }

  public convenience init(_ subscribers: any Service...) {
    self.init(subscribers)
  }

  public func run() async throws {
    // A service group shuts down when one of its services returns early, such as a group of no
    // subscribers configured at runtime.
    if subscribers.isEmpty {
      try? await gracefulShutdown()
      return
    }

    // Only a service group can shut a service down gracefully on request. Each also forwards
    // graceful shutdown of the task running this group to its subscriber.
    let serviceGroups = subscribers.map { subscriber in
      ServiceGroup(configuration: .init(services: [subscriber], logger: Self.logger))
    }
    let firstError = await withTaskGroup(of: (any Error)?.self) { group in
      for serviceGroup in serviceGroups {
        group.addTask {
          do {
            try await serviceGroup.run()
            return nil
          } catch {
            return error
          }
        }
      }

      var firstError: (any Error)?
      for await case let error? in group {
        if firstError == nil {
          firstError = error
          Self.logger.error("Subscriber failed, shutting down the others: \(error)")
          for serviceGroup in serviceGroups {
            await serviceGroup.triggerGracefulShutdown()
          }
        } else if (error as? ServiceGroupError)?.errorCode != .alreadyFinished {
          // Subscribers that hadn't started when the others were shut down never run.
          Self.logger.error("Subscriber failed: \(error)")
        }
      }
      return firstError
    }

    if let firstError {
      throw firstError
    }
  }
}
