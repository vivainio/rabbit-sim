# What it supports

- Exchanges: direct, fanout, topic, headers, exchange-to-exchange bindings, the default exchange and `amq.*`.
- Queues: declare (passive, exclusive, auto-delete, server-named), bind and unbind, purge, delete (if-unused, if-empty).
- Publishing: `mandatory` with `basic.return`, and publisher confirms (ack and nack frames).
- Consuming: `basic.get`, consume and cancel, ack, nack, reject and recover, `basic.qos` prefetch (per channel), round-robin dispatch, and redelivery when a channel or connection closes.
- Heartbeats, consumer cancel notification, and RabbitMQ-style channel and connection error codes and texts.

## Vhosts

Connect to any vhost and it exists. Each vhost has its own default exchange, `amq.*` exchanges, queues and bindings, so two vhosts can both declare a queue called `orders` and they are separate queues. A dead-letter exchange named in a queue's arguments is looked up in that queue's own vhost.

There is nothing to create or delete, and no permissions: any client may use any vhost, and a misspelled vhost in a connection string quietly gives you an empty one. Fault-injection settings and the TTL clock are global, not per vhost.

It is tested with the Rust `lapin` client (the test suite) and the .NET `RabbitMQ.Client` 6.x, including a broker kill and restart.

## Queue arguments

| Argument | Behaviour |
| --- | --- |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | Messages that are rejected or nacked without requeue, expire, overflow or hit the delivery limit are republished there with RabbitMQ's `x-death` and `x-first-death-*` headers. `expiration` is dropped, and cycles are broken the way RabbitMQ does. |
| `x-message-ttl`, per-message `expiration` | Expired messages are dead-lettered or dropped. Like RabbitMQ, only the head of a queue expires. |
| `x-max-length`, `x-overflow` | `drop-head` (the default, dead-letters the oldest), `reject-publish` (the publisher confirm becomes a nack), `reject-publish-dlx`. |
| `x-queue-type=quorum`, `x-delivery-limit` | Quorum rules (durable, non-exclusive, no auto-delete) and delivery counting. Redelivered messages carry `x-delivery-count`, and a message returned more than the limit (default 20, as in RabbitMQ 4.x) is dead-lettered with reason `delivery_limit`. Classic queues do not count. |

Redeclaring a queue with different arguments fails with `PRECONDITION_FAILED - inequivalent arg ...`, as in RabbitMQ.
