# RabbitMQ version behaviour

rabbit-sim is not a particular RabbitMQ release. With `--rabbitmq 4` it refuses what RabbitMQ 4 refuses by default, so a test that would fail there fails here too.

Today that is declaring a transient (non-durable) queue that is not exclusive. RabbitMQ 4 rejects it with ``INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated`` and a connection close. The default, `--rabbitmq 3`, accepts such queues.

With `--rabbitmq 4` the broker also reports itself as version 4.4.0 and supports [delayed messages](testing-delayed-messages.md) on quorum queues, which RabbitMQ added in 4.4. Without it the broker reports 3.13.7 and ignores the delay header, as RabbitMQ 3.x and 4.0 to 4.3 do. The version a client reads from the connection is the emulated one. The simulator's own version is in the `rabbit_sim_version` server property.

Run against both settings, and still check a real broker before a release.
