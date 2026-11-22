# Building and releasing

```sh
cargo test            # integration tests drive the broker with a real AMQP client
cargo clippy --all-targets -- -D warnings
```

The tests start the broker in process and talk to it with the Rust `lapin` client, including the HTTP endpoints.

## Releasing

1. Push `main` and wait for the CI workflow to pass.
2. Create the release: `gh release create vX.Y.Z --target main`. The Release workflow builds the binaries and the wheels.
3. Wait for the wheel on PyPI. The pip index can lag a few minutes.
4. Update the `rabbit-sim==` pin in any project that installs it.

## License

MIT
