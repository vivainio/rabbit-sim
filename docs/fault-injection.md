# Fault injection

Faults are set over HTTP on the management port, so a test can change the broker's behaviour while it runs.

## `POST /chaos`

One JSON body describes the whole behaviour:

```sh
curl -X POST http://127.0.0.1:15672/chaos \
  -d '{"confirm_delay_ms":0,"deliver_delay_ms":40,"ack_delay_ms":250,"nack_next":0}'
```

The reply is the settings now in force. `GET /chaos` returns them without changing anything.

The body replaces everything. A field it leaves out is 0, so `{}` turns every fault off, which makes it the reset call for a test's cleanup. Unknown fields, non-integers and an empty body are refused with 400 and change nothing.

| Field | Effect |
| --- | --- |
| `confirm_delay_ms` | Holds every publisher confirm back, in order. Publishes made together are confirmed together after about one delay, and a client that waits for a confirm per message becomes visibly slow, as it does on a real broker. |
| `deliver_delay_ms` | Holds every delivery to a consumer back, in order. The message counts as unacked, and against prefetch, while it waits. |
| `ack_delay_ms` | Makes the broker act on each consumer ack only after the delay. The message stays unacked, and holds a prefetch slot, meanwhile. An ack still waiting when its channel closes is dropped. |
| `nack_next` | Declines the next N publishes on confirm channels. Each is dropped and answered with `basic.nack`, which reaches the path where a publisher is not told its message was accepted. It counts down as publishes are declined. |

All delays use real time. The first three can also be set at start-up with `--confirm-delay`, `--deliver-delay` and `--ack-delay`.

## The TTL clock

Time-dependent behaviour is slow to test, so the broker's TTL clock can be driven over HTTP:

```text
POST /chaos/clock/advance?ms=3600000          # move the TTL clock forward and expire what is due
POST /chaos/clock/reset                       # put the clock back to real time
POST /chaos/queues/%2F/{name}/expire          # expire every ready message in a queue now (%2F is the vhost)
```

Expired messages are dead-lettered exactly as real expiry would. Only message TTLs and delayed messages use this clock. Heartbeats and everything else run on real time.

`clock/advance` lets everything due in that time happen, in order, as if the time had passed. A message that expires in one queue and is dead-lettered into another carries on from the moment it expired. So one advance of 10 seconds takes a message through a chain of delay queues that adds up to 10 seconds, with no need to step through them one hop at a time. See [testing delayed messages](testing-delayed-messages.md).

The clock is one for the whole broker and an advance stays until it is reset. That matters for absolute delivery times as well as for TTLs: after an advance of ten seconds, a delivery time five seconds ahead on the real clock is already past on the broker's. A test that advances the clock should call `clock/reset` when it finishes, or the tests after it will see messages delivered early.
