import Logging
import Synchronization
import Tracing

public final class HandlerContext: Sendable {

  private let _logger: Mutex<Logger>
  public var logger: Logger {
    get { _logger.withLock { $0 } }
    set { _logger.withLock { $0 = newValue } }
  }

  public let span: any Span

  package init(logger: Logger, span: any Span) {
    self._logger = Mutex(logger)
    self.span = span
  }
}
