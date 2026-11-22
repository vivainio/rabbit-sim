# RabbitMQ version behaviour

rabbit-sim is not a particular RabbitMQ release. With `--rabbitmq 4` it refuses what RabbitMQ 4 refuses by default, so a test that would fail there fails here too.

Today that is declaring a transient (non-durable) queue that is not exclusive. RabbitMQ 4 rejects it with ``INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated`` and a connection close. The default, `--rabbitmq 3`, accepts such queues.

Run against both settings, and still check a real broker before a release.
