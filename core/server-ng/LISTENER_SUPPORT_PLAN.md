# `server-ng` Listener Support Plan

## Scope

Add `server-ng` bootstrap support for the `message_bus` client listener planes that were added upstream:

- QUIC
- WebSocket
- TCP-TLS

`HTTP` remains out of scope for this change.

This plan is based on the current rebased tree, not on the intended end state.

## What Exists Today

### `server-ng` is still effectively TCP-only

In [`src/bootstrap.rs`](./src/bootstrap.rs), `RunServerNg::run()` builds only:

- `AcceptedReplicaFn`
- `AcceptedClientFn`

and then calls `start_tcp_runtime(...)`.

The runtime split is:

- `start_single_node_tcp_runtime(...)`
- `start_cluster_tcp_runtime(...)`

Both branches only wire plain TCP listeners today.

### The new `message_bus` entry point already supports the extra planes

[`core/message_bus/src/replica/io.rs`](../message_bus/src/replica/io.rs) exposes:

- `replica_io::start_on_shard_zero(...)`

That function can bind:

- replica TCP
- client TCP
- WS
- QUIC
- TCP-TLS
- WSS

and returns `BoundPlanes { replica, client, ws, quic, tcp_tls, wss }`.

`server-ng` is still calling `start_on_shard_zero_default(...)`, which hardcodes all optional listener planes to `None`.

### `server-ng` is not yet using the `ServerNgConfig` / `message_bus` config path

This is the most important bootstrap mismatch in the current tree:

- `load_config()` still loads `configs::server::ServerConfig`
- `bootstrap()` still constructs the bus with `IggyMessageBus::new(SHARD_ID)`

That means:

- the `[message_bus]` config section is not being consumed
- bus WS tuning is ignored
- bus reconnect / handshake / queue tunables are still defaulted
- QUIC tuning from `ServerNgConfig` is not flowing into the bus constructor

Because `IggyMessageBus::with_config(...)` expects `configs::server_ng::ServerNgConfig`, listener support should be implemented together with config wiring.

## Recommended Implementation Shape

### 1. Move `server-ng` bootstrap to `ServerNgConfig`

Update `core/server-ng` to load and pass around `configs::server_ng::ServerNgConfig` instead of the legacy `ServerConfig`.

Why this should happen first:

- `message_bus` production constructor is `IggyMessageBus::with_config(shard_id, cfg)`
- the QUIC listener needs bus-side QUIC tuning
- the WS listener needs bus-side WS handshake config
- the repo already has a dedicated `server_ng_config` schema for exactly this wiring

Expected touch points:

- [`src/bootstrap.rs`](./src/bootstrap.rs)
- [`src/config_writer.rs`](./src/config_writer.rs)
- any `server-ng` call sites currently typed to `ServerConfig`

### 2. Replace `IggyMessageBus::new(...)` with `IggyMessageBus::with_config(...)`

In `bootstrap()`, build the bus from the validated `ServerNgConfig`.

This makes the listener work use the correct runtime tunables immediately:

- `message_bus.handshake_grace`
- `message_bus.close_grace`
- `message_bus.close_peer_timeout`
- `message_bus.reconnect_period`
- `message_bus.ws_*`
- `quic.*` transport tuning

### 3. Generalize the TCP-only bootstrap helpers into transport bootstrap

The current names and signatures are too narrow:

- `resolve_tcp_topology(...)`
- `start_tcp_runtime(...)`
- `start_cluster_tcp_runtime(...)`
- `start_single_node_tcp_runtime(...)`

Recommended refactor:

- keep `TcpTopology` for replica/client TCP addressing if convenient
- add a listener-settings helper that derives all optional listen addresses from config
- rename runtime helpers to reflect multi-transport startup rather than TCP-only startup

At minimum, bootstrap needs to derive:

- replica TCP address
- client TCP address
- optional WS listen address from `[websocket]`
- optional QUIC listen address from `[quic]`
- optional TCP-TLS listen address

Important detail:

- TCP-TLS does not have its own address section
- it is derived from `[tcp].address` when `[tcp.tls].enabled = true`

Same pattern exists for WSS via `[websocket]` + `[websocket.tls]`, even if WSS is not part of the first implementation slice.

### 4. In cluster mode, switch from `start_on_shard_zero_default(...)` to `start_on_shard_zero(...)`

This is the main listener wiring change.

Instead of:

- `replica_io::start_on_shard_zero_default(...)`

call:

- `replica_io::start_on_shard_zero(...)`

and populate the optional planes based on config:

- `ws_listen_addr`
- `quic_listen_addr`
- `tcp_tls_listen_addr`
- optionally `wss_listen_addr`

Also provide the matching accept callbacks:

- `AcceptedWsClientFn`
- `AcceptedQuicClientFn`
- `AcceptedTlsClientFn`
- optionally `AcceptedWssClientFn`

### 5. Add transport-specific accepted-client closures in `server-ng`

`server-ng` already has:

- `make_local_client_accept_fn(...)`

Add transport-specific variants that mint `client_id` the same way and then install through the correct `message_bus::installer` entry point.

Expected closures:

- plain TCP: `installer::install_client_tcp(...)` or existing wrapper path
- WS: reuse the pre-upgrade local path, most likely by duping the accepted fd and calling `ConnectionInstaller::install_client_ws_fd(...)`
- QUIC: `installer::install_client_quic(...)`
- TCP-TLS: `installer::install_client_tcp_tls(...)`

Important nuance for WS:

- upstream `message_bus` docs describe WS as a pre-upgrade TCP accept path that can be fd-shipped to another shard
- `server-ng` is single-shard today, so it can terminate WS locally on shard 0
- the accepted callback receives raw `TcpStream`, so `installer::install_client_ws(...)` is not directly callable there because it expects a post-upgrade `WebSocketStream`
- the clean reuse path is to keep using the existing pre-upgrade installer flow (`install_client_ws_fd(...)`) locally on shard 0 rather than inventing a second WS handshake path in `server-ng`

Important nuance for QUIC / TCP-TLS:

- these are shard-0 terminal by design in `message_bus`
- do not try to thread them through a cross-shard setup path

### 6. Add credential-loading helpers for TCP-TLS and QUIC

Bootstrap needs to construct:

- `message_bus::TlsServerCredentials`
- `message_bus::replica_io::QuicServerCredentials`

Reuse existing primitives where possible:

- `message_bus::transports::tls::load_pem(...)`
- `message_bus::transports::tls::self_signed_for_loopback()`
- `message_bus::transports::tls::install_default_crypto_provider()`

For QUIC, mirror the existing legacy-server behavior:

- self-signed when configured and files are absent
- otherwise load cert/key from configured paths

This logic should live in `server-ng`, not be duplicated inside `message_bus`.

### 7. Single-node mode should also use the same transport bootstrap path

Right now single-node startup bypasses `replica_io` entirely and binds plain TCP directly via `client_listener::bind(...)`.

That should be replaced with one transport-aware path so cluster and non-cluster mode do not diverge on feature support.

Recommended shape:

- cluster mode: bind replica + client planes through `replica_io::start_on_shard_zero(...)`
- single-node mode: either
  - reuse `start_on_shard_zero(...)` with a degenerate one-node topology, or
  - add a small `client_planes_only(...)` helper if the replica-plane assumptions make that cleaner

The important part is to avoid maintaining separate TCP-only and multi-transport startup logic.

## `current_config.toml` Follow-Up

[`src/config_writer.rs`](./src/config_writer.rs) currently writes only:

- `tcp.address`
- `cluster.nodes[*].ports.tcp`
- `cluster.nodes[*].ports.tcp_replica`

That is insufficient once new listeners are enabled.

Extend it to write the effective bound addresses for:

- `quic.address`
- `websocket.address`

and update cluster node ports when relevant:

- `ports.quic`
- `ports.websocket`

For TCP-TLS there is no separate top-level address field today, so there may be nothing new to serialize beyond the shared TCP address unless the config model changes.

## Error Handling Changes

[`src/server_error.rs`](./src/server_error.rs) still exposes:

- `StartTcpListeners`

That name is too narrow after this change. Rename or replace it with something transport-neutral, for example:

- `StartListeners`
- `StartTransportListeners`

Also expect new bootstrap errors for:

- TLS credential loading
- QUIC credential loading
- invalid derived listener configuration

## Proposed Execution Order

1. Switch `server-ng` from `ServerConfig` to `ServerNgConfig`.
2. Construct the bus with `IggyMessageBus::with_config(...)`.
3. Add credential-loading helpers for TLS-family and QUIC listeners.
4. Introduce transport-specific accepted-client closures.
5. Replace `start_on_shard_zero_default(...)` with `start_on_shard_zero(...)` in cluster mode.
6. Unify single-node startup with the same multi-transport bootstrap path.
7. Extend `write_current_config(...)` to include QUIC / WS bound addresses.
8. Rename the TCP-specific listener-startup error variant and add any new bootstrap errors.

## Test Plan

### Compile and unit-level checks

- `cargo check -p server-ng`
- `cargo check -p message_bus`
- `cargo check -p integration`

## First Implementation Slice

To keep the change set controlled, the first slice should be:

1. move to `ServerNgConfig`
2. wire `IggyMessageBus::with_config(...)`
3. add QUIC / WS / TCP-TLS listener startup in cluster mode
4. mirror the same startup in single-node mode
5. update `current_config.toml` for QUIC and WS

WSS can be added immediately after if desired, because the bootstrap pattern is the same once the transport-aware startup path exists.
