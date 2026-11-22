# rabbit-sim

A single-binary AMQP 0-9-1 broker for local development and tests. Point any RabbitMQ client at it instead of running RabbitMQ.
In-memory by default, with optional SQLite persistence and a small web UI.

## Install

    pip install rabbit-sim      # or: pipx install rabbit-sim / uvx rabbit-sim
    cargo install --git https://github.com/vivainio/rabbit-sim   # from source

Standalone binaries for Linux, macOS (Intel/Apple Silicon) and Windows are attached to each
[GitHub release](https://github.com/vivainio/rabbit-sim/releases).

## Run

    rabbit-sim                         # amqp://guest:guest@127.0.0.1:5672/  and  http://127.0.0.1:15672/
    rabbit-sim --port 5679 -v          # custom port, log connections and protocol errors
    rabbit-sim --data broker.db        # persist durable state to SQLite

| Option | Default | |
| --- | --- | --- |
| `-b, --bind ADDR` | `127.0.0.1` | address to listen on (use `0.0.0.0` for containers) |
| `-p, --port PORT` | `5672` | AMQP port |
| `-m, --http-port PORT` | `15672` | management UI/API port, `0` disables it |
| `-d, --data FILE` | none | SQLite file for persistence; omit for pure in-memory |
| `-r, --rabbitmq 3\|4` | `3` | emulate the stricter behaviour of a RabbitMQ major version (see below) |
| `-c, --confirm-delay MS` | `0` | hold every publisher confirm back by MS milliseconds, like a loaded or distant broker |
| `-v, --verbose` | off | log connects, disconnects and errors |

Any username and password are accepted, and all clients share one vhost.

## Supported
- Exchanges: direct, fanout, topic, headers, exchange-to-exchange bindings, default exchange and `amq.*`
- Queues: declare (passive, exclusive, auto-delete, server-named), bind/unbind, purge, delete (if-unused/if-empty)
- Publishing: mandatory + `basic.return`, publisher confirms (ack/nack frames)
- Consuming: `basic.get`, consume/cancel, ack/nack/reject/recover, `basic.qos` prefetch (per channel),
  round-robin dispatch, redelivery when a channel or connection closes
- Heartbeats, consumer cancel notification, RabbitMQ-style channel/connection error codes and texts

### Queue arguments
| Argument | Behaviour |
| --- | --- |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | messages that are rejected/nacked without requeue, expire, overflow or hit the delivery limit are republished there with RabbitMQ's `x-death` and `x-first-death-*` headers (`expiration` is dropped; cycles are broken the way RabbitMQ does) |
| `x-message-ttl`, per-message `expiration` | expired messages are dead-lettered or dropped; like RabbitMQ, only the head of a queue expires |
| `x-max-length`, `x-overflow` | `drop-head` (default, dead-letters the oldest), `reject-publish` (publisher confirm becomes a nack), `reject-publish-dlx` |
| `x-queue-type=quorum`, `x-delivery-limit` | quorum rules (durable, non-exclusive, no auto-delete) and delivery counting: redelivered messages carry `x-delivery-count`, and a message returned more than the limit (default 20, as in RabbitMQ 4.x) is dead-lettered with reason `delivery_limit`. Classic queues don't count. |

Redeclaring a queue with different arguments fails with `PRECONDITION_FAILED - inequivalent arg ...`, as in RabbitMQ.

Tested with the Rust `lapin` client (the test suite) and the .NET `RabbitMQ.Client` 6.x, including a broker kill and restart.

## Persistence (optional)
With `--data`, state is saved to a SQLite file and restored on start, following RabbitMQ's rules:
durable exchanges, durable non-exclusive queues, bindings between durable entities, and messages published with
`delivery_mode=2` into durable queues. Everything else stays transient. Writes go through a background thread in
batched transactions (WAL, `synchronous=NORMAL`), and Ctrl-C/SIGTERM flushes before exit. Messages that were unacked
when the process stopped come back as ready.

The broker keeps all messages in RAM; the file provides durability, not paging. A hard kill can lose the last few
milliseconds of writes.

## Management
`GET /` serves a one-page UI (refreshes every 2s, purge button per queue). JSON, read-only except purge, no auth:

    GET    /api/{overview,queues,exchanges,bindings,connections}
    DELETE /api/queues/%2F/{name}/contents        # purge ready messages

### Fault injection (`/chaos/*`)
Time-dependent behaviour is slow to test, so the broker's TTL clock can be driven over HTTP:

    POST /chaos/clock/advance?ms=3600000          # move the TTL clock forward and expire what is due
    POST /chaos/queues/%2F/{name}/expire          # expire every ready message in a queue now

    POST /chaos/latency?confirm=MS                # hold every publisher confirm back by MS ms (0 turns it off)
    POST /chaos/nack?count=N                      # decline the next N publishes on confirm channels

Confirms are held back in order, so publishes made together are confirmed together after about one delay, and a client that waits for
a confirm per message becomes visibly slow, as it does on a real broker. A declined publish is dropped and answered with `basic.nack`,
which is how a test reaches the path where a publisher is not told its message was accepted. Both use real time.

Expired messages are dead-lettered exactly as real expiry would. Only message TTLs use this clock;
heartbeats and everything else run on real time.

## Version behaviour
rabbit-sim is not a particular RabbitMQ release. With `--rabbitmq 4` it refuses what RabbitMQ 4 refuses by default, so a test that would fail there fails here too. Today that is declaring a transient (non-durable) queue that is not exclusive, which RabbitMQ 4 rejects with `INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated` and a connection close. The default, `--rabbitmq 3`, accepts such queues. Run against both settings, and still check a real broker before a release.

## Not supported
- Clustering, replication, TLS, real authentication, multiple vhosts, a drop-in RabbitMQ management API
- Transactions (`tx.*` returns NOT_IMPLEMENTED)
- Other queue arguments (`x-expires`, `x-max-priority`, `x-max-length-bytes`, `x-single-active-consumer`, ...)
  are accepted but ignored; quorum queues only emulate delivery counting/limits, and streams are rejected
- `basic.qos` limits apply per channel whether or not the client sets the global flag

## Development

    cargo test            # integration tests drive the broker with a real AMQP client
    cargo clippy --all-targets -- -D warnings

## License
MIT
