# The rabbit-sim book

rabbit-sim is a single-binary AMQP 0-9-1 broker for local development and tests. Point any RabbitMQ client at it instead of running RabbitMQ. It is in memory by default, with optional SQLite persistence and a small web UI.

It is not RabbitMQ. It behaves like RabbitMQ where tests usually look, and it adds what a real broker cannot give you: faults on demand. You can hold publisher confirms, deliveries and acks back, decline publishes, and move the TTL clock forward, all over HTTP.

- [Getting started](getting-started.md) installs the broker, runs it and lists its options.
- [What it supports](supported.md) lists exchanges, queues, consuming and queue arguments.
- [Persistence](persistence.md) explains what survives a restart with `--data`.
- [Management UI and API](management.md) covers the web page and its JSON endpoints.
- [RabbitMQ version behaviour](versions.md) explains `--rabbitmq 3|4`.
- [What it leaves out](omissions.md) lists what is not emulated.
- [Fault injection](fault-injection.md) is the reference for `POST /chaos`, the TTL clock and the start-up flags.
- [Testing slow brokers](testing-slow-brokers.md) shows what to test with those faults: prefetch, concurrency, slow consumers and redelivery.
- [Building and releasing](development.md) covers the tests and the release steps.
