import Testing

extension Tag {

  @Tag static var integration: Self

  /// Takes tens of seconds, for example to wait for ack deadlines to pass.
  @Tag static var slow: Self
}
