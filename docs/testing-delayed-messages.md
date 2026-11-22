# Testing delayed messages

Timeouts and delayed sends are slow to test with real time. rabbit-sim lets a test move the clock, so a delay of ten seconds, or ten days, takes no time to pass.

There are two ways a system can ask RabbitMQ to delay a message, and the simulator supports both.

## Native delayed messages

RabbitMQ 4.4 added delayed messages on quorum queues. A publisher sets the `x-opt-delivery-time` header to the delivery time in milliseconds since the Unix epoch. The message is routed and confirmed at once, but the queue holds it until that time. Delayed messages count toward the queue's length limit.

The simulator does the same when it emulates RabbitMQ 4 (`--rabbitmq 4`):

- A **quorum** queue holds the message. It shows as `messages_delayed` in `GET /api/queues`, and in the Delayed column of the web page, and it is not ready.
- A **classic** queue ignores the header and the message is ready at once, as on a real broker. This is the usual mistake, so a test for it is worthwhile.
- With `--rabbitmq 3`, which reports version 3.13.7, the header is ignored on every queue.
- A message that is already due when it arrives is ready at once.

```text
POST /chaos/clock/advance?ms=61000      # the message due in 60 seconds is now ready
```

## Chains of delay queues

Before 4.4, a delay is built from queues that each have a fixed `x-message-ttl` and dead-letter into the next one. Each hop adds its TTL, so a few queues of different lengths can make any delay. The simulator runs these chains: TTLs expire and dead-letter exactly as in the broker.

Advancing the clock follows the chain. A message that expires in one queue is dead-lettered into the next from the moment it expired, not from the moment you advanced to. So one `clock/advance` of seven seconds takes a message through hops of four, two and one seconds. There is no need to advance one hop at a time.

## A test

1. Start the simulator with `--rabbitmq 4` if the system under test uses native delay. Otherwise start it as usual.
2. Make the system send the delayed message, or a timeout.
3. Check that nothing arrives at once, and that the message is waiting: in a queue's `messages_delayed`, or in a delay queue.
4. `POST /chaos/clock/advance?ms=...` by the delay.
5. Check that it arrives now, and not before.
6. `POST /chaos/clock/reset` when the test ends. The clock is shared by every test on the broker, and absolute delivery times are compared with it.

Two things to check on a real broker as well, because the simulator is not one: that the destination queue is a quorum queue when native delay is used, and that the broker version really is 4.4 or later. The simulator reports the version it emulates, so a system that reads the version from the connection can be tested for both outcomes.
