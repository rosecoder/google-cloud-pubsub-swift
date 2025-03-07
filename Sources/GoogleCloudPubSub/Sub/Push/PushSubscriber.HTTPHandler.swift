import Foundation
import Logging
import NIO
import NIOHTTP1
import Synchronization
import Tracing

private struct HTTPHeadersExtractor: Extractor {

  func extract(key: String, from carrier: HTTPHeaders) -> String? {
    carrier.first(name: key)
  }
}

extension PushSubscriber {

  final class HTTPHandler: ChannelInboundHandler, Sendable {

    typealias InboundIn = HTTPServerRequestPart

    private let handle: @Sendable (Incoming) async -> Response

    private let _isKeepAlive = Mutex(false)
    private let _context = Mutex<ServiceContext>(.topLevel)
    private let _buffer = Mutex<ByteBuffer?>(nil)

    private var isKeepAlive: Bool {
      get { _isKeepAlive.withLock { $0 } }
      set { _isKeepAlive.withLock { $0 = newValue } }
    }

    private var context: ServiceContext {
      get { _context.withLock { $0 } }
      set { _context.withLock { $0 = newValue } }
    }

    private var buffer: ByteBuffer? {
      get { _buffer.withLock { $0 } }
      set { _buffer.withLock { $0 = newValue } }
    }

    let logger = Logger(label: "pubsub.subscriber")

    static let decoder: JSONDecoder = {
      let decoder = JSONDecoder()
      decoder.dataDecodingStrategy = .base64

      let dateFormats: [String] = [
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX",
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
      ]
      let dateFormatters = dateFormats.map {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .iso8601)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = $0
        return formatter
      }
      decoder.dateDecodingStrategy = .custom({ decoder in
        let container = try decoder.singleValueContainer()
        let string = try container.decode(String.self)
        for formatter in dateFormatters {
          if let date = formatter.date(from: string) {
            return date
          }
        }
        throw DecodingError.dataCorrupted(
          .init(
            codingPath: decoder.codingPath,
            debugDescription: "Date string does not match format expected by formatter."
          ))
      })

      return decoder
    }()

    init(handle: @Sendable @escaping (Incoming) async -> Response) {
      self.handle = handle
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
      let reqPart = unwrapInboundIn(data)
      let channel = context.channel

      switch reqPart {
      case .head(let head):
        self.isKeepAlive = head.isKeepAlive

        self.context = .topLevel
        InstrumentationSystem.instrument.extract(
          head.headers,
          into: &self.context,
          using: HTTPHeadersExtractor()
        )
      case .body(var body):
        if buffer != nil {
          self.buffer!.writeBuffer(&body)
        } else {
          self.buffer = body
        }
      case .end:
        guard let buffer else {
          logger.warning("Channel read end without head and/or buffer")
          context.close(promise: nil)
          return
        }
        self.buffer = nil

        Task { [handle, isKeepAlive, logger, serviceContext = self.context] in
          let response: Response
          do {
            _ = buffer
            let incoming = try HTTPHandler.decoder.decode(Incoming.self, from: buffer)

            response = await ServiceContext.withValue(serviceContext) {
              await handle(incoming)
            }
          } catch {
            logger.error(
              "Error parsing incoming message: \(error)",
              metadata: [
                "data": .string(String(buffer: buffer))
              ])
            response = .unexpectedCallerBehavior
          }

          //                    if !(await Environment.current.isCPUAlwaysAllocated) {
          //                        await Tracing.shared.writeIfNeeded()
          //                        await Tracing.shared.waitForWrite()
          //                    }

          var head = HTTPResponseHead(version: .http1_1, status: response.httpStatus)
          if isKeepAlive {
            head.headers.add(name: "Keep-Alive", value: "timeout=5, max=1000")
          }
          _ = channel.write(HTTPServerResponsePart.head(head))
          _ = channel.write(HTTPServerResponsePart.body(.byteBuffer(.init())))
          channel.writeAndFlush(HTTPServerResponsePart.end(nil)).whenComplete {
            [isKeepAlive] _ in
            if !isKeepAlive {
              channel.close(promise: nil)
            }
          }
        }
      }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
      logger.warning("Socket error, closing connection: \(error)")
      context.close(promise: nil)
    }
  }
}
