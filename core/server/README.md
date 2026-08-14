# Apache Iggy Server

The core server component of Apache Iggy: a persistent, append-only log for message streaming. It runs thread-per-core and shared-nothing on `io_uring` (through `compio`), and commits every write through Viewstamped Replication (VSR), so the same binary serves a standalone node and a multi-node cluster.

Clients connect over TCP (custom binary protocol), QUIC, WebSocket, or the HTTP REST API.

## Running

```sh
cargo run --bin iggy-server --release
```

The Docker image `apache/iggy:latest` ships the server together with the CLI; the `edge` tag tracks the latest development build.

To run one node of a cluster, pass its replica ID from the `cluster.nodes` roster:

```sh
cargo run --bin iggy-server --release -- --replica-id 0
```

`--replica-id` is the only command line argument; everything else is configuration.

## Configuration

Settings are read from [config.toml](config.toml), resolved relative to the working directory. Set `IGGY_CONFIG_PATH` to load a different file.

Any single value can be overridden with an `IGGY_`-prefixed environment variable that mirrors the TOML path:

```sh
IGGY_TCP_ADDRESS=0.0.0.0:8090 IGGY_HTTP_ENABLED=false cargo run --bin iggy-server
```

Cluster membership, quorum and replica addressing live under `[cluster]`.

## Systemd integration

Build with the `systemd` feature to enable readiness and watchdog notifications:

```sh
cargo build --bin iggy-server --release --features systemd
```

The server sends `READY=1` only after every enabled transport is bound and accepting, so a unit ordered after it can dial as soon as it is notified. When the unit sets `WatchdogSec=`, the server pings `WATCHDOG=1` at half that interval. On shutdown it sends `STOPPING=1`, which stops a long drain from counting against the watchdog.

![Server](../../assets/server.png)

![Architecture](../../assets/iggy_architecture.png)
