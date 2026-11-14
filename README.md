# Google Cloud PubSub for Swift

A Swift implementation for interacting with Google Cloud PubSub, designed as a server-first solution. This package provides a high-level, type-safe way to interact with Google Cloud PubSub while maintaining excellent performance.

The vision of this package is to provide a high-level, but still performant, way to interact with PubSub from Swift.

## Features

- Publishing messages
- Subscribing to topics – supporting both pull and push methods.

The goal of this project is to provide a high-level and super-type safe way to interact with PubSub. Feel free to contrinubte with either creating an issue or a pull request.

## Usage

### Publishing

The publisher is a [swift-service-lifecycle](https://github.com/swift-server/swift-service-lifecycle) `Service` that must be running while publishing. List it before the services that publish, so it's shut down after them:

```swift
let publisher = try await Publisher()

let serviceGroup = ServiceGroup(
  configuration: .init(
    services: [publisher, myServer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger))
try await serviceGroup.run()
```

While it's running, for example in `myServer`:

```swift
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

### Running many subscribers

A `ServiceGroup` shuts its services down one at a time, waiting for each to return before signalling the next. Each pull subscriber waits up to its shutdown timeout for running handlers, and the ones not yet signalled keep receiving messages meanwhile, so 100 subscribers listed individually can take 100 × 30 seconds to shut down. Run them in a `PullSubscriberGroup` instead, which shuts them all down at once and takes as long as the slowest:

```swift
let options = PullSubscriberOptions(
  flowControl: .init(maxOutstandingMessages: 150, maxOutstandingBytes: 16 * 1024 * 1024),
  shutdownBehavior: .waitForProcessing(timeout: .seconds(30)))

let publisher = try await Publisher()
let subscribers: [any Service] = [
  try await PullSubscriber(handler: OrderHandler(), options: options),
  try await PullSubscriber(handler: InvoiceHandler(), options: options),
  // ...
]

var configuration = ServiceGroupConfiguration(
  services: [publisher, PullSubscriberGroup(subscribers)],
  gracefulShutdownSignals: [.sigterm],
  logger: logger)
configuration.maximumGracefulShutdownDuration = .seconds(32)
try await ServiceGroup(configuration: configuration).run()
```

The publisher is listed first so handlers can still publish while the subscribers shut down. If a subscriber fails, the group shuts the others down gracefully and then throws its error, which shuts down the service group.

**Flow control.** Every subscription keeps up to `maxOutstandingMessages` messages in memory, at about 10 KB each in load tests. The default of 1,000 messages and 100 MiB per subscription adds up to about 1 GB for 100 subscriptions, so for 50–135 subscriptions in one process use 100–200 messages and 10–20 MiB per subscription. Since each outstanding message runs in its own handler task, this also bounds how many handlers run concurrently.

**Shutdown timeout.** Set `maximumGracefulShutdownDuration` to at least the `waitForProcessing` timeout plus about 2 seconds for the final acks and closing the connections, and keep it below the time the platform kills the process at. On Kubernetes that's `terminationGracePeriodSeconds`, 30 seconds by default, which is too short for the default 30-second timeout: raise it, for example to 45 seconds, or with `terminationGracePeriodSeconds: 180` use a timeout of up to about 170 seconds. Messages still being handled when the timeout passes are nacked and redelivered.

## Testing

The package includes a testing module `GoogleCloudPubSubTesting` that provides utilities for testing your Pub/Sub implementation:

```swift
import GoogleCloudPubSubTesting

let fakePublisher = FakePublisher()
// Use in your tests
```

### Running the package's tests

Integration tests run against the [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) with `PUBSUB_EMULATOR_HOST=localhost:8085 swift test`; skip them with `swift test --skip IntegrationTests`.

In CI, pass `--event-stream-output-path events.jsonl` as well. The event stream is written as tests run, so if a run is killed, the tests with a `testStarted` but no `testEnded` event are the ones that hung. `--xunit-output results.xml` is only written once the run finishes.

## License

MIT License. See [LICENSE](./LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
