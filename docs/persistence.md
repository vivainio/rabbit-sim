# Persistence

With `--data`, state is saved to a SQLite file and restored on start, following RabbitMQ's rules. These are kept:

- durable exchanges
- durable, non-exclusive queues
- bindings between durable entities
- messages published with `delivery_mode=2` into durable queues

Everything else stays transient. Writes go through a background thread in batched transactions (WAL, `synchronous=NORMAL`), and Ctrl-C or SIGTERM flushes before exit. Messages that were unacked when the process stopped come back as ready.

!!! note
    The broker keeps all messages in RAM. The file provides durability, not paging. A hard kill can lose the last few milliseconds of writes.
