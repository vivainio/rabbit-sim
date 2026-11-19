# rabbit-sim

A single-binary, in-memory AMQP 0-9-1 broker for local development. Point any RabbitMQ client at it.

    pip install rabbit-sim        # or: pipx install rabbit-sim / uvx rabbit-sim
    rabbit-sim --port 5672 -v

    # or from source
    cargo run --release -- --port 5672 -v
    # amqp://guest:guest@127.0.0.1:5672/
    # web UI + API: http://127.0.0.1:15672/   (--http-port 0 to disable)

## Supported
- Exchanges: direct, fanout, topic, headers, plus exchange-to-exchange bindings; default exchange and `amq.*`
- Queues: declare (passive, exclusive, auto-delete, server-named), bind/unbind, purge, delete (if-unused/if-empty)
- Publish (mandatory + `basic.return`), publisher confirms, `basic.get`, consume/cancel, ack/nack/reject/recover
- `basic.qos` prefetch (applied per channel), round-robin dispatch, redelivery on channel/connection close
- Heartbeats, consumer cancel notification, RabbitMQ-style channel/connection errors

## Persistence (optional)
By default everything is in memory. With `--data broker.db`, state is saved to a SQLite file and restored on start,
using RabbitMQ's rules: durable exchanges, durable non-exclusive queues, bindings between durable things, and
messages published with `delivery_mode=2` into durable queues. Everything else stays transient.
Writes go through a background thread in batched transactions (WAL, `synchronous=NORMAL`), and Ctrl-C/SIGTERM
flushes before exit. Messages unacked when the process stops come back as ready.
Note: the broker still holds all messages in RAM; the file is for durability, not for paging.

## Management
`GET /` serves a one-page UI (auto-refreshes every 2s, with a purge button per queue).
JSON: `GET /api/{overview,queues,exchanges,bindings,connections}`, and
`DELETE /api/queues/%2F/{name}/contents` to purge. Read-only otherwise, no auth.

## Not supported (by design)
- Persistence, clustering, fault tolerance: everything lives in memory and dies with the process
- Auth (any credentials accepted), vhosts (all share one), TLS
- Transactions (`tx.*` returns NOT_IMPLEMENTED), TTL/max-length/dead-lettering/priority/quorum/streams
