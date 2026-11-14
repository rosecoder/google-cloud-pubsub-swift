# Stress tests

`PubSubStress` runs pull subscribers and a publisher in one process against the Pub/Sub emulator or
Google Cloud Pub/Sub, verifies that every published message is handled successfully, and reports
throughput, latency, memory, CPU and log messages. It's a separate package so it doesn't affect the
library's consumers.

It only uses the library's public API: every subscription is a `PullSubscriber` in one
`PullSubscriberGroup` in a `ServiceGroup`, publishing goes through `Publisher`, and all of them
share the library's connections as in an application. Build it like a production binary:

```sh
cd StressTests
swift build -c release
.build/release/PubSubStress --help
```

## Running

Options are `key=value` arguments, or `STRESS_<KEY>` environment variables. The project comes from
`project=` or `STRESS_PROJECT_ID`, so nothing private needs to be written to a file:

```sh
# Against the emulator
PUBSUB_EMULATOR_HOST=localhost:8085 STRESS_PROJECT_ID=stress-local \
  .build/release/PubSubStress subs=10 rate=1000 duration=60

# Against Pub/Sub, with Application Default Credentials
STRESS_PROJECT_ID=<project> .build/release/PubSubStress subs=100 rate=10000 duration=900 \
  batch=10 max-outstanding=200 max-outstanding-bytes=20971520 sleep-ms=20 sleep-jitter=0.5
```

Every run creates its own topics and subscriptions named `pubsub-swift-stress-<run>-…` through
Pub/Sub's REST API and deletes them when it ends, also after Ctrl-C (press twice to exit right
away). `mode=cleanup` deletes every topic and subscription starting with `pubsub-swift-stress-` in
the project, for example after a run was killed. The credentials need `roles/pubsub.editor`.

### Modes

- `mode=run` (default): creates resources, optionally publishes `backlog` messages per topic before
  the subscribers start, publishes at `rate` (or `rate-steps`) for `duration` seconds, waits until
  every message was handled, shuts down gracefully and verifies.
- `mode=churn`: like `run`, but shuts the subscriber group down gracefully and starts a new one
  `cycles` times while publishing continues. With `gap-max` publishing pauses for a random time
  after each cycle, with one ad-hoc publish in the middle. Together with `publisher-service=false`
  this lets the shared clients stop and restart between groups.
- `mode=publish`: only publishes, to `topics` topics without subscriptions.
- `mode=cleanup`: see above.

### Main options

| Option | Default | |
| --- | --- | --- |
| `subs`, `topics` | 10, `subs` | Subscription `i` subscribes to topic `i % topics`. |
| `rate`, `rate-steps` | 0 (max) | Total messages per second, or `rate:seconds,…` steps. |
| `publishers`, `batch` | 16, 100 | Concurrent publish tasks and messages per publish RPC. |
| `size` | 1024 | Message body bytes. |
| `max-backlog` | 100000 | Publishing pauses while more deliveries are pending. |
| `sleep-ms`, `sleep-jitter`, `cpu-ms` | 0 | Handler work: sleeping, or busy CPU without suspending. |
| `fail-rate`, `slow-rate`, `slow-ms` | 0, 0, 30000 | Share of handlers that throw (nack), or take `slow-ms`. |
| `max-outstanding`, `max-outstanding-bytes` | 1000, 100 MiB | Flow control per subscription. |
| `ack-deadline` | 10 | Subscription ack deadline in seconds. |
| `shutdown-timeout`, `nack-immediately` | 30, false | Subscriber shutdown behaviour. |
| `publisher-service` | true | Run a `Publisher` service for the whole run. |
| `log-level` | warning | Library log messages at or above it are counted and the first few echoed. |
| `out`, `label` | /tmp/pubsub-stress-results | Results go to `<out>/<label>-<run>/`. |

### What's verified and reported

Each message carries a per-topic sequence number and its publish time (wall clock nanoseconds) as
attributes. Per subscription the handler records:

- **LOST**: published (and confirmed) but never handled successfully. Must be 0.
- **REDELIVERED AFTER SUCCESS**: handled successfully, then delivered again, which means an ack was
  lost or arrived after its deadline, or Pub/Sub delivered a duplicate.
- **Concurrent duplicates**: delivered again while still being handled, which means a lease expired.
- **Redeliveries after nack**: expected with `fail-rate`.
- **Latency** from publish to the handler starting, of first deliveries.

Every `sample` seconds a line is printed and appended to `samples.csv`: publish and receive rates,
handlers in flight, pending deliveries, RSS, threads, file descriptors, established TLS connections
(Linux), CPU and latency. `summary.txt` has the totals per phase, graceful shutdown times and every
library log message counted by kind. The process exits non-zero if a message was lost or a graceful
shutdown hung.

The harness can't see inside the library, so ack RPCs aren't counted; failed acks and nacks show up
as the library's log messages, and lost acks as redeliveries after success.

## Running on a Compute Engine VM

`scripts/gce/` has the scripts used to run scenarios on a VM in the same region as the Pub/Sub
traffic, building and running in the `swift:6.4-noble` Docker image:

1. Create a VM with a service account that has `roles/pubsub.editor`, for example
   `--max-run-duration=8h --instance-termination-action=DELETE` as a safety net.
2. Copy the package (with `git archive`), `run.sh`, `queue.sh` and a scenario file to the VM and
   build: `docker run --rm -v ~/work/main:/src -w /src/StressTests swift:6.4-noble swift build -c release`.
3. `nohup ./queue.sh scenarios.txt &`. Each line is `<build> <label> [--cpus=N] options…`, where
   `<build>` is a directory under `~/work`, so several versions of the library can be compared.
   `run.sh` reads the project from the metadata server. Logs go to `~/results/logs/`, results to
   `~/results/<build>/`.
4. Delete the VM, and run `mode=cleanup` if a run was killed.
