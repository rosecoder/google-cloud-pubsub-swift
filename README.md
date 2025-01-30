# Google Cloud PubSub for Swift

A Swift implementation for interacting with Google Cloud PubSub, designed as a server-first solution. This package provides a high-level, type-safe way to interact with Google Cloud PubSub while maintaining excellent performance.

The vision of this package is to provide a high-level, but still performant, way to interact with PubSub from Swift.

## Features

- Publishing messages
- Subscribing to topics – supporting both pull and push methods.

The goal of this project is to provide a high-level and super-type safe way to interact with PubSub. Feel free to contrinubte with either creating an issue or a pull request.

## Usage

### Publishing

```swift
let publisher = try await Publisher()
Task { publisher.run() }

extension Topics {

  static let myTopic = Topic<PlainTextMessage>(name: "my-topic")
}

try await publisher.publish(to: Topics.myTopic, body: "Hello")
```

There's a few different message types built-in, but you can also implement your own. The built-in types are:

- `PlainTextMessage` – a simple message with a string body.
- `JSONMessage` – a message with a JSON body using Swift's `Encodable` protocol.
- `ProtobufMessage` – a message with a protobuf body using [swift-protobuf](https://github.com/apple/swift-protobuf).
- `DataMessage` – a message with a binary body.

You can also implement your own message type by implementing the `Message` protocol.

Here's an example of `JSONMessage`:

```swift
extension Topics {

  static let myTopic = Topic<JSONMessage<Message>>(name: "my-topic")
}

struct Message: Encodable {

    let data: String
}

try await publisher.publish(
    to: Topics.myTopic,
    body: Message(data: "Hello world!")
)
```

### Subscribing

To subscribe to a topic using pull, you need to implement a handler which handles all incoming messages. The same message types as described above is also supported for subscribing.

Example:

```swift
struct MyMessage: Decodable {

    let data: String
}

extension Topics {

  static let myTopic = Topic<JSONMessage<MyMessage>>(name: "my-topic")
}

struct MyHandler: Handler {

    let subscription = Subscription(name: "my-subscription", topic: Topics.myTopic)

    func handle(message: Incoming, context: Context) async throws {
        print("Message received: \(message.body.data)")
    }
}

let subscriber = try await PullSubscriber(handler: MyHandler())
try await subscriber.run()
```

Subscribe using push is also supported. This starts up a HTTP server which will handle messages.

Example:

```swift
let subscriber = PushSubscriber()
subscriber.register(handler: MyHandler())
try await subscriber.run()
```

Note: The push subscriber will fallback to pull during local development against the Pub/Sub emulator due to the emulator is not supporting push.

## Testing

The package includes a testing module `GoogleCloudPubSubTesting` that provides utilities for testing your Pub/Sub implementation:

```swift
import GoogleCloudPubSubTesting

let fakePublisher = FakePublisher()
// Use in your tests
```

## License

MIT License. See [LICENSE](./LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
