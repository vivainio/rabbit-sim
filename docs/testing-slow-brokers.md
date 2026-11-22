# Testing slow brokers

A real broker is fast on a developer machine, so a consumer's prefetch, concurrency and redelivery behaviour is hard to reach. The delays in [fault injection](fault-injection.md) make those paths visible.

Each recipe below sets a behaviour, runs a burst of messages, and resets with `{}` afterwards. Make the test skip itself when the broker is not rabbit-sim, or is too old to have `/chaos`: a request to a real broker or an older simulator fails, which is the signal.

## Slow acks

Set `ack_delay_ms` to a few hundred milliseconds with a consumer concurrency of 4. Up to four messages are in flight, and each is still unacked at the broker for the delay after the handler finishes. Assert that the handler concurrency peak stays within the limit and that every message is handled once.

## Prefetch bounds

Set both `deliver_delay_ms` and `ack_delay_ms` with prefetch 2 and a higher consumer concurrency. The broker never has more than two messages unacked, so the peak number of running handlers is at most two, whatever the concurrency setting says.

## A handler slower than the delivery delay

Set `deliver_delay_ms` below the handler's run time. Deliveries overlap with handling, and the test shows that no message is lost or handled twice.

## Slow publisher confirms

Set `confirm_delay_ms` to show that batching publishes with one confirm beats one confirm per message, and that a confirm which never arrives in time is treated as a failed publish. With `nack_next` you can also check that a declined publish is sent again.

!!! tip
    Reset in a `finally` block with `{}`. A fault left on by a failed test slows every test after it.
