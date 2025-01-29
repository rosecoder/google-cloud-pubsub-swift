import GoogleCloudPubSub
import Logging
import Tracing

extension HandlerContext {

  public static func fake(
    logger: Logger = Logger(label: "handler"),
    span: any Span = NoOpTracer.NoOpSpan(context: .topLevel)
  ) -> HandlerContext {
    HandlerContext(logger: logger, span: span)
  }
}
