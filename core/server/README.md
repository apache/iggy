# Apache Iggy Server

The core server component of Apache Iggy: a persistent, append-only log for message streaming. It runs thread-per-core and shared-nothing on `io_uring` (through `compio`), and commits every write through Viewstamped Replication (VSR), so the same binary serves a standalone node and a multi-node cluster.

Clients connect over TCP (custom binary protocol), QUIC, WebSocket, or the HTTP REST API.

## Running

```sh
cargo run --bin iggy-server --release
```

The Docker image `apache/iggy:latest` ships the server together with the CLI; the `edge` tag tracks the latest development build.

The image binds every listener to `0.0.0.0` so the container is reachable from outside it. A wildcard bind says which interfaces accept connections, not where a client reaches the server, so the address to publish in cluster metadata has to be supplied and the server refuses to start without it. On a single host with published ports that address is `localhost`:

```sh
docker run -p 3000:3000 -p 8090:8090 \
  -e IGGY_NODE_ADVERTISED_ADDRESS=localhost apache/iggy:latest
```

Use the hostname or load balancer address clients actually dial when they are not on the same host. The Helm chart derives it from the Service DNS name.

To run one node of a cluster, pass its replica ID from the `cluster.nodes` roster:

```sh
cargo run --bin iggy-server --release -- --replica-id 0
```

`--replica-id` is the only command line argument; everything else is configuration.

## Configuration

Settings are read from [config.toml](config.toml), resolved relative to the working directory. Set `IGGY_CONFIG_PATH` to load a different file.

Any single value can be overridden with an `IGGY_`-prefixed environment variable that mirrors the TOML path:

```sh
IGGY_TCP_ADDRESS=127.0.0.1:8090 IGGY_HTTP_ENABLED=false cargo run --bin iggy-server
```

Cluster membership, quorum and replica addressing live under `[cluster]`.

During a rolling upgrade from `server-0.9.0`, stop HTTP clients until every
server runs the new version. A `server-0.9.0` node seeds its HTTP client id
counter from the ids that newer nodes mint, so it can mint an id that a newer
node also mints. If two HTTP sessions share a client id, the server can
acknowledge a write of one session from the deduplication record of the other
and not append it.

Upgrade every server before any client stores an external group offset, for
example through the Kafka gateway. A `server-0.9.0` replica cannot decode that
consumer kind. It never acknowledges the prepare, and journal repair fails on it
in the same way. A partition with a majority of `server-0.9.0` replicas then
stops committing.

For cluster auto-commit consumers, upgrade all servers and binary SDKs together.
Pause those consumers, upgrade every server, then update their SDKs and restart
them to rejoin their groups. Primary polling uses binary commands 14, 103 and
104; routed manual and interval offset writes also use command 123.
Older SDKs can lose membership when a backup refuses an offset commit,
and new SDKs require servers supporting those commands. HTTP polling is
forwarded by the server and keeps its existing client API.

## Upgrade recovery

Partition recovery refuses a superblock whose nonzero `log_view`
is below the partition's committed `created_view`. It also refuses a WAL
certificate with a nonzero `log_view` below that floor. The log names the
superblock directory or the `prepares-{revision}` WAL directory and the
recorded views. The affected partition is tombstoned locally and is not served;
its durable files are preserved.

For a superblock with `log_view` zero, recovery raises `view` to at least
`created_view` without changing `log_view`. The raised view must be persisted
before any view-scoped send, just as when no superblock record exists.

Such state can have been written by older servers, including `server-0.9.0`,
after restarting before the partition's first superblock write. The creation
view was not restored on that path. Raising a nonzero `log_view` during an
upgrade would certify history that the replica may not hold, so the server
does not repair those records automatically.

Partition directories hold a `created.revision` file with the revision that
created the partition. Recovery deletes a directory whose file names an older
revision. The partition then starts empty, as if it had no directory, and in a
cluster the replica copies its data from the other replicas.

A directory that an older server made has no such file. A delete that did not
finish before a restart leaves such a directory on disk, and a later create
with the same stream, topic and partition ids finds it. Recovery then loads the
leftover directory as the new partition. Only a leftover superblock with a
nonzero `log_view` below `created_view` trips the superblock refusal. The WAL
refusal does not apply, because each partition has its own `prepares-{revision}`
WAL directory. Recovery logs `adopting a nonempty partition directory without
created.revision` before adopting such a directory, since it cannot distinguish
a live legacy incarnation from a deleted one.

If recovery logs `falls below committed creation view` or the adoption warning
above, look at the history of the affected topic or partition first. If it was
deleted and created again with the same ids, delete it again, and then create
it again. The delete removes the leftover directory on every replica. It also
removes every message in that topic or partition, so do this only if you can
lose them.

In every other case:

1. Pause writes to the affected partition and stop the affected cluster before
   changing any recovery files. Keep the refusal logs.
2. Preserve a consistent copy of every replica's metadata, partition
   superblocks, WAL directories and segment files together with its configuration.
3. Restore only from a verified consistent backup, or use a separately validated
   recovery procedure based on retained replica history. Confirm that it preserves
   acknowledged writes and agrees with the committed partition creation metadata.

There is no automatic in-place migration for below-floor log certificates. Do not edit
view numbers or delete superblocks or WAL directories to bypass the refusal:
an empty history could then replace committed data during a view change.

## Systemd integration

Build with the `systemd` feature to enable readiness and watchdog notifications:

```sh
cargo build --bin iggy-server --release --features systemd
```

The server sends `READY=1` only after every enabled transport is bound and accepting, so a unit ordered after it can dial as soon as it is notified. When the unit sets `WatchdogSec=`, the server pings `WATCHDOG=1` at half that interval. On shutdown it sends `STOPPING=1`, which stops a long drain from counting against the watchdog.

![Server](../../assets/server.png)

![Architecture](../../assets/iggy_architecture.png)
