# Getting started

## Install

```sh
pip install rabbit-sim      # or: pipx install rabbit-sim / uvx rabbit-sim
cargo install --git https://github.com/vivainio/rabbit-sim   # from source
```

Standalone binaries for Linux, macOS (Intel and Apple Silicon) and Windows are attached to each [GitHub release](https://github.com/vivainio/rabbit-sim/releases).

## Run

```sh
rabbit-sim                         # amqp://guest:guest@127.0.0.1:5672/  and  http://127.0.0.1:15672/
rabbit-sim --port 5679 -v          # custom port, log connections and protocol errors
rabbit-sim --data broker.db        # persist durable state to SQLite
```

Any username and password are accepted, and all clients share one vhost.

## Options

| Option | Default | |
| --- | --- | --- |
| `-b, --bind ADDR` | `127.0.0.1` | address to listen on (use `0.0.0.0` for containers) |
| `-p, --port PORT` | `5672` | AMQP port |
| `-m, --http-port PORT` | `15672` | management UI and API port, `0` disables it |
| `-d, --data FILE` | none | SQLite file for persistence; omit for pure in-memory |
| `-r, --rabbitmq 3\|4` | `3` | emulate the stricter behaviour of a RabbitMQ major version, see [version behaviour](versions.md) |
| `-c, --confirm-delay MS` | `0` | hold every publisher confirm back by MS milliseconds |
| `--deliver-delay MS` | `0` | hold every delivery to a consumer back by MS milliseconds |
| `--ack-delay MS` | `0` | act on every consumer ack only MS milliseconds after it arrives |
| `-v, --verbose` | off | log connects, disconnects and errors |

The three delay options set the starting state of [fault injection](fault-injection.md), which a test can change later over HTTP.

## In a test run

Start the broker, point the client at `amqp://guest:guest@127.0.0.1:5672/`, and stop it afterwards. With port `0` for `--http-port` the management API is off. A test that drives faults needs it on, and needs to know its port.
