/// Opaque identifier the server assigns to each delivery of a message, used to acknowledge it or
/// modify its ack deadline.
struct AckID: RawRepresentable, Hashable, Sendable, CustomStringConvertible {

  let rawValue: String

  init(rawValue: String) {
    self.rawValue = rawValue
  }

  init(_ rawValue: String) {
    self.rawValue = rawValue
  }

  var description: String {
    rawValue
  }

  /// Bytes this ID adds to a request when encoded as a repeated `ack_ids` string field: one tag
  /// byte, the varint encoded length and the UTF-8 bytes.
  var encodedSize: Int {
    let length = rawValue.utf8.count
    var varintSize = 1
    var remaining = length >> 7
    while remaining > 0 {
      varintSize += 1
      remaining >>= 7
    }
    return 1 + varintSize + length
  }
}
