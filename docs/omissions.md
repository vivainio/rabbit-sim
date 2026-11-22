# What it leaves out

- Clustering, replication, TLS, real authentication, multiple vhosts, and a drop-in RabbitMQ management API.
- Transactions: `tx.*` returns `NOT_IMPLEMENTED`.
- Other queue arguments (`x-expires`, `x-max-priority`, `x-max-length-bytes`, `x-single-active-consumer` and so on) are accepted but ignored. Quorum queues only emulate delivery counting and limits, and streams are rejected.
- `basic.qos` limits apply per channel whether or not the client sets the global flag.
