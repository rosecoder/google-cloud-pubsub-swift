import GoogleCloudPubSub
import GoogleCloudPubSubTesting
import Testing

extension Topics {

  fileprivate static let test = Topic<PlainTextMessage>(name: "FakePublisherTests")
}

@Suite struct FakePublisherTests {

  let publisher = FakePublisher()

  @Test func shouldRememberPublishedCount() async throws {
    let messages1 = try await publisher.publish(
      to: Topics.test,
      messages: [.init(body: "Hello")]
    )
    #expect(publisher.publishedCount == 1)
    #expect(messages1.count == 1)
    #expect(messages1.first?.id == "1")

    let messages2 = try await publisher.publish(
      to: Topics.test,
      messages: [
        .init(body: "Hello there"),
        .init(body: "Hello again"),
      ]
    )
    #expect(publisher.publishedCount == 3)
    #expect(messages2.count == 2)
    #expect(messages2.first?.id == "2")
    #expect(messages2.last?.id == "3")
  }
}
