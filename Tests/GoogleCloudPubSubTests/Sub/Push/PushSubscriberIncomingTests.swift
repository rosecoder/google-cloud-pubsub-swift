import Foundation
import Synchronization
import Testing

@testable import GoogleCloudPubSub

@Suite struct PushSubscriberIncomingTests {

  @Test func shouldParse() throws {
    let decoder = PushSubscriber.HTTPHandler.decoder

    let incoming = try decoder.decode(
      PushSubscriber.Incoming.self,
      from: """
        {
          \"message\": {
            \"attributes\": {},
            \"data\": \"SGVq\",
            \"messageId\": \"7893897826387963\",
            \"message_id\": \"7893897826387963\",
            \"publishTime\": \"2023-07-21T14:00:07Z\",
            \"publish_time\": \"2023-07-21T14:00:07Z\"
          },
          \"subscription\": \"projects/proj/subscriptions/sub\"
        }
        """.data(using: .utf8)!)
    #expect(incoming.message.data == "Hej".data(using: .utf8)!)
    #expect(incoming.message.id == "7893897826387963")
    #expect(incoming.message.published.timeIntervalSince1970 == 1_689_948_007)
  }
}
