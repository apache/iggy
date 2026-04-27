// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Connection installer trait.
//!
//! Shard 0 accepts / dials all TCP connections and ships the duplicated fd
//! to the owning shard via the inter-shard `ShardFrame` channel. The owning
//! shard's router handler wraps the fd on its own compio runtime and
//! registers the connection on its local bus. This trait exposes that
//! registration surface in a way the shard layer can call without knowing
//! the concrete bus type.

use crate::client_listener::RequestHandler;
use crate::fd_transfer::{self, DupedFd};
use crate::lifecycle::{InstanceToken, RejectedRegistration, Shutdown};
use crate::replica_listener::MessageHandler;
use crate::socket_opts::{apply_keepalive_for_connection, apply_nodelay_for_connection};
use crate::transports::quic::QuicTransportConn;
use crate::transports::tcp::TcpTransportConn;
use crate::transports::tcp_tls::TcpTlsTransportConn;
use crate::transports::ws::WsTransportConn;
use crate::transports::wss::WssTransportConn;
use crate::transports::{ActorContext, TransportConn};
use crate::{IggyMessageBus, lifecycle::ShutdownToken};
use async_channel::Receiver;
use compio::net::TcpStream;
use futures::FutureExt;
use iggy_binary_protocol::{Command2, GenericHeader, Message};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Operations a shard needs to perform on its local bus when the router
/// receives an inter-shard connection-setup or mapping frame.
///
/// The production implementation is on `Rc<IggyMessageBus>`. The simulator
/// does not exercise this path; if it ever does, add a no-op impl on
/// `SharedSimOutbox`.
pub trait ConnectionInstaller {
    /// Wrap a duplicated TCP fd into a `TcpStream` on the local compio
    /// runtime, spawn writer + reader tasks, and register the replica
    /// connection on this shard.
    ///
    /// Takes ownership of `fd`. On registration failure the fd is closed
    /// by dropping the wrapping `TcpStream`; on caller-side failure (e.g.
    /// inter-shard send drops the setup frame) the `DupedFd` closes the
    /// fd on drop.
    fn install_replica_fd(&self, fd: DupedFd, replica_id: u8, on_message: MessageHandler);

    /// Same for an SDK client connection. The owning shard is already
    /// encoded in the top 16 bits of `client_id`.
    fn install_client_fd(&self, fd: DupedFd, client_id: u128, on_request: RequestHandler);

    /// Same for an SDK WebSocket client's pre-upgrade TCP fd. The
    /// receiving shard wraps the fd, runs `compio_ws::accept_hdr_async`
    /// with the iggy.consensus.v1 subprotocol callback to drive the
    /// HTTP-Upgrade handshake, then installs WS reader / writer tasks
    /// via [`install_client_ws_stream`] on success. On handshake
    /// failure (e.g. wrong / missing subprotocol) the fd is closed by
    /// dropping the wrapping `TcpStream`.
    fn install_client_ws_fd(&self, fd: DupedFd, client_id: u128, on_request: RequestHandler);

    /// Update the replica -> owning shard mapping used by the `send_to_replica`
    /// slow path on non-owning shards.
    fn set_shard_mapping(&self, replica: u8, owning_shard: u16);

    /// Forget the replica -> owning shard mapping (e.g. after a connection
    /// loss, before the next allocate).
    fn remove_shard_mapping(&self, replica: u8);
}

impl ConnectionInstaller for Rc<IggyMessageBus> {
    fn install_replica_fd(&self, fd: DupedFd, replica_id: u8, on_message: MessageHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_replica_stream(self, replica_id, stream, on_message);
    }

    fn install_client_fd(&self, fd: DupedFd, client_id: u128, on_request: RequestHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        install_client_stream(self, client_id, stream, on_request);
    }

    fn install_client_ws_fd(&self, fd: DupedFd, client_id: u128, on_request: RequestHandler) {
        let stream = fd_transfer::wrap_duped_fd(fd);
        let bus = Self::clone(self);
        let handle = compio::runtime::spawn(async move {
            match compio_ws::accept_hdr_async(stream, ws_subprotocol_callback).await {
                Ok(ws) => {
                    if !bus.is_shutting_down() {
                        install_client_ws_stream(&bus, client_id, ws, on_request);
                    }
                }
                Err(e) => {
                    warn!(client_id, "WS upgrade failed: {e}");
                }
            }
        });
        self.track_background(handle);
    }

    fn set_shard_mapping(&self, replica: u8, owning_shard: u16) {
        IggyMessageBus::set_shard_mapping(self, replica, owning_shard);
    }

    fn remove_shard_mapping(&self, replica: u8) {
        IggyMessageBus::remove_shard_mapping(self, replica);
    }
}

/// HTTP-Upgrade callback for the WS client plane.
///
/// Inspects `Sec-WebSocket-Protocol`. Accepts only the exact value
/// [`crate::transports::ws::WS_SUBPROTOCOL`] (`iggy.consensus.v1`); any
/// other value (or absence) yields HTTP 400 with a body naming the
/// expected subprotocol. The accepted value is mirrored back on the
/// response so the negotiated subprotocol is unambiguous to the
/// client.
#[allow(clippy::result_large_err)] // tungstenite-defined Callback signature; not on hot path
fn ws_subprotocol_callback(
    req: &tungstenite::handshake::server::Request,
    mut resp: tungstenite::handshake::server::Response,
) -> Result<tungstenite::handshake::server::Response, tungstenite::handshake::server::ErrorResponse>
{
    let want = crate::transports::ws::WS_SUBPROTOCOL.as_bytes();
    let proto = req
        .headers()
        .get(tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL);
    if proto.is_some_and(|hv| hv.as_bytes() == want) {
        resp.headers_mut().insert(
            tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL,
            tungstenite::http::HeaderValue::from_static(crate::transports::ws::WS_SUBPROTOCOL),
        );
        return Ok(resp);
    }
    let mut err = tungstenite::http::Response::new(Some(format!(
        "missing or wrong subprotocol; expected {}",
        crate::transports::ws::WS_SUBPROTOCOL
    )));
    *err.status_mut() = tungstenite::http::StatusCode::BAD_REQUEST;
    Err(err)
}

/// TCP entry point: apply socket options (keepalive, `TCP_NODELAY`) on
/// the raw stream and delegate to the transport-generic install path.
///
/// Socket options live here (not on the generic path) because they are
/// TCP-specific; other transports (WSS via TLS terminator, QUIC) lack a
/// raw-fd level at this layer. Kept as a separate function so the
/// `install_replica_fd` fd-delegation entry and the accept callbacks
/// (`AcceptedReplicaFn` in tests and shard-0 coordinator) converge on
/// one place for those options.
#[allow(clippy::future_not_send)]
pub fn install_replica_stream(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    stream: TcpStream,
    on_message: MessageHandler,
) {
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(replica = peer_id, "keepalive failed on delegated fd: {e}");
    }
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        // Linux does not propagate TCP_NODELAY from the listener to the
        // accepted fd, so we toggle it here on every installed stream.
        // A miss means we stay Nagle-on for this peer, not a failure.
        warn!(replica = peer_id, "nodelay failed on delegated fd: {e}");
    }
    install_replica_conn(bus, peer_id, TcpTransportConn::new(stream), on_message);
}

/// Install a pre-wrapped replica connection on the bus.
///
/// Generic over [`TransportConn`] so alternate transports (WS via
/// shard-0 TLS terminator, QUIC via `compio-quic`) plug in behind the
/// same registry-insert + instance-token fencing + install-race
/// handling. TCP-specific socket options live in
/// [`install_replica_stream`]; transports with no equivalent layer call
/// this entry directly with their already-configured connection.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub fn install_replica_conn<C: TransportConn>(
    bus: &Rc<IggyMessageBus>,
    peer_id: u8,
    conn: C,
    on_message: MessageHandler,
) {
    if bus.replicas().contains(peer_id) {
        debug!(
            replica = peer_id,
            "replica already registered on this shard, dropping delegated fd"
        );
        drop(conn);
        return;
    }

    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());
    let (in_tx, in_rx) =
        async_channel::bounded::<Message<GenericHeader>>(bus.peer_queue_capacity());

    // Writer and reader both observe abnormal close and used to fire
    // `notify_connection_lost` twice per disconnect, causing shard 0 to
    // broadcast two `ReplicaMappingClear` rounds and churn the mapping.
    // Shared one-shot guard: whichever post-loop runs first wins.
    let notified = Rc::new(Cell::new(false));
    // If the registry insert below races with a concurrent install for
    // the same peer id and loses, both spawned halves must skip their
    // post-loop cleanup: the loser's `replicas().remove` /
    // `close_peer_if_token_matches` calls would no-op against the winner's
    // generation token (so they can't evict the live entry), but
    // `notify_connection_lost` has no token guard and would still broadcast
    // a spurious mapping-clear round. `compio::runtime::JoinHandle::drop`
    // does not cancel the spawned task, so we have to tell the tasks to
    // stand down in-band.
    let install_aborted = Rc::new(Cell::new(false));

    // Generation token published by the registry on a successful insert.
    // Writer and reader post-loops release the slot only when the stored
    // token matches; a stale-install exit that wakes up after a later
    // reinstall would otherwise evict the new slot.
    let install_token: Rc<Cell<Option<InstanceToken>>> = Rc::new(Cell::new(None));

    // Per-connection shutdown used to kick the transport off its
    // `io_uring` read SQE when the registry insert below loses a race.
    // The bus-wide token cannot be triggered here (it would tear down
    // every other connection); closing the bus-side outbound sender
    // also does not reach a reader blocked on
    // [`framing::read_message`]. The `Shutdown` is moved into the
    // registry entry on success so its `Sender` survives the
    // connection's lifetime: dropping the `Shutdown` would close the
    // broadcast channel and falsely wake the `ShutdownToken`'s
    // listeners. On insert race the loser receives the `Shutdown` back
    // via `RejectedRegistration` and triggers it before draining the
    // orphan tasks.
    let (conn_shutdown, conn_token) = Shutdown::new();

    // The transport observes a single [`ShutdownToken`] in its
    // `ActorContext`; bridge the bus-wide token and the per-connection
    // token onto a fresh fused token so either firing wakes the
    // transport's reader off `io_uring`. The bridge task self-terminates
    // on first fire; on a clean run it parks until bus shutdown, when
    // the bus-wide token guarantees a fire.
    let bus_token_for_bridge = bus.token();
    let conn_token_for_bridge = conn_token.clone();
    let (transport_shutdown, transport_token) = Shutdown::new();
    let bridge_handle = compio::runtime::spawn(async move {
        futures::select! {
            () = bus_token_for_bridge.wait().fuse() => {}
            () = conn_token_for_bridge.wait().fuse() => {}
        }
        transport_shutdown.trigger();
    });
    bus.track_background(bridge_handle);

    let label: &'static str = "replica";
    let peer_fmt = format!("{peer_id}");
    let ctx = ActorContext {
        in_tx,
        rx,
        shutdown: transport_token,
        max_batch: bus.config().max_batch,
        max_message_size: bus.config().max_message_size,
        label,
        peer: peer_fmt,
    };

    let bus_for_transport = Rc::clone(bus);
    let aborted_transport = Rc::clone(&install_aborted);
    let token_for_transport = Rc::clone(&install_token);
    let notified_transport = Rc::clone(&notified);
    let transport_handle = compio::runtime::spawn(async move {
        conn.run(ctx).await;
        if aborted_transport.get() || bus_for_transport.is_shutting_down() {
            return;
        }
        let Some(token) = token_for_transport.get() else {
            return;
        };
        if !bus_for_transport
            .replicas()
            .remove_if_token_matches(peer_id, token)
        {
            return;
        }
        if !notified_transport.replace(true) {
            bus_for_transport.notify_connection_lost(peer_id);
        }
    });

    let bus_for_dispatch = Rc::clone(bus);
    let bus_token_dispatch = bus.token();
    let conn_token_dispatch = conn_token;
    let aborted_dispatch = Rc::clone(&install_aborted);
    let token_for_dispatch = Rc::clone(&install_token);
    let notified_dispatch = Rc::clone(&notified);
    let close_peer_timeout = bus.config().close_peer_timeout;
    let dispatch_handle = compio::runtime::spawn(async move {
        replica_dispatch_loop(
            peer_id,
            in_rx,
            &on_message,
            &bus_token_dispatch,
            &conn_token_dispatch,
            &aborted_dispatch,
        )
        .await;
        if aborted_dispatch.get() {
            debug!(
                replica = peer_id,
                "aborted replica install: skipping post-loop cleanup"
            );
            return;
        }
        if !bus_token_dispatch.is_triggered() {
            let Some(token) = token_for_dispatch.get() else {
                return;
            };
            let closed = bus_for_dispatch
                .replicas()
                .close_peer_if_token_matches(peer_id, token, close_peer_timeout)
                .await;
            if closed && !notified_dispatch.replace(true) {
                bus_for_dispatch.notify_connection_lost(peer_id);
            }
        }
        info!(replica = peer_id, "peer replica disconnected");
    });

    match bus.replicas().insert(
        peer_id,
        tx,
        transport_handle,
        dispatch_handle,
        conn_shutdown,
    ) {
        Ok(token) => {
            install_token.set(Some(token));
        }
        Err(rejected) => {
            // Tell both halves to stand down: the winner's entry is live and
            // must not be touched by this losing install.
            install_aborted.set(true);
            warn!(replica = peer_id, "replica registry insert raced");
            // `drain_rejected_registration` triggers the per-connection
            // shutdown (returned with `rejected`) to wake the transport
            // off its `io_uring` read SQE, then awaits transport /
            // dispatch handles. `compio::runtime::JoinHandle::drop` only
            // detaches, so without this drain the reader would outlive
            // the race on a half-open socket until peer EOF. Hand the
            // handle to `track_background` so `IggyMessageBus::shutdown`
            // awaits the drain before returning - `.detach()` would
            // orphan it and leak the half-closed socket across shutdown.
            let drain_handle =
                compio::runtime::spawn(drain_rejected_registration(rejected, close_peer_timeout));
            bus.track_background(drain_handle);
        }
    }
}

/// TCP entry point for client installs. Applies socket options and
/// delegates to [`install_client_conn`]. See [`install_replica_stream`]
/// for the plane-symmetric docs.
#[allow(clippy::future_not_send)]
pub fn install_client_stream(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    stream: TcpStream,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(
            client = client_id,
            "keepalive failed on delegated client fd: {e}"
        );
    }
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(
            client = client_id,
            "nodelay failed on delegated client fd: {e}"
        );
    }
    install_client_conn(bus, client_id, TcpTransportConn::new(stream), on_request);
}

/// QUIC entry point for client installs.
///
/// Wraps the [`compio_quic::Connection`] and its first bidirectional
/// `(SendStream, RecvStream)` pair (already accepted by shard 0 via
/// `Connection::accept_bi().await` so the install path never
/// re-handshakes) in a [`QuicTransportConn`] and delegates to the
/// existing generic [`install_client_conn`].
///
/// No socket-options analog runs here: QUIC keepalive lives in
/// [`compio_quic::TransportConfig::keep_alive_interval`] set at endpoint
/// construction time. The connection is encrypted end-to-end and there
/// is no plaintext fd to dup, which is why this never crosses an
/// inter-shard channel: shard 0 owns the QUIC `Endpoint`, terminates
/// every connection locally, and uses the existing
/// `ForwardClientSend` / `Consensus` shard-frame variants for outbound
/// + inbound traffic respectively.
#[allow(clippy::future_not_send)]
pub fn install_client_quic_conn(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    connection: compio_quic::Connection,
    streams: (compio_quic::SendStream, compio_quic::RecvStream),
    on_request: RequestHandler,
) {
    install_client_conn(
        bus,
        client_id,
        QuicTransportConn::new(connection, streams),
        on_request,
    );
}

/// TCP-TLS entry point for client installs.
///
/// Wraps a freshly accepted plaintext [`TcpStream`] (TLS handshake has
/// NOT run yet) plus the shared [`Arc<rustls::ServerConfig>`] in a
/// [`TcpTlsTransportConn`] and delegates to the existing generic
/// [`install_client_conn`]. The rustls handshake then runs inside the
/// transport's `run` body on the per-connection install task, so a slow
/// or malicious peer never blocks the listener accept loop.
///
/// `TCP_NODELAY` + `SO_KEEPALIVE` apply to the underlying TCP socket
/// before the handshake starts, matching [`install_client_stream`]'s
/// plaintext behaviour. Linux does not propagate these from the listener
/// to accepted sockets, so toggling here is required.
///
/// TCP-TLS is shard-0 terminal: the rustls `UnbufferedConnection` state
/// machine is non-serialisable and tied to the local task; pre-handshake
/// the fd is plain TCP and could in principle be dup'd to another shard,
/// but the receiving shard would then have to re-handshake against
/// shard-0-resident key material — losing the point of the cross-shard
/// handover. Both reasons compose into invariant I5's TLS-family clause
/// (formalised in P9-T13).
#[allow(clippy::future_not_send)]
pub fn install_client_tls_stream(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(
            client = client_id,
            "keepalive failed on accepted TLS client fd: {e}"
        );
    }
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(
            client = client_id,
            "nodelay failed on accepted TLS client fd: {e}"
        );
    }
    install_client_conn(
        bus,
        client_id,
        TcpTlsTransportConn::new_server(stream, config).with_drain_budget(cfg.tls_drain_budget),
        on_request,
    );
}

/// WSS entry point for client installs.
///
/// Wraps a freshly accepted plaintext [`TcpStream`] (neither the TLS
/// handshake nor the WS HTTP-Upgrade has run yet) plus the shared
/// [`Arc<rustls::ServerConfig>`] in a [`WssTransportConn`] and delegates
/// to the existing generic [`install_client_conn`]. Both handshakes
/// then run inside the transport's `run` body on the per-connection
/// install task. Subprotocol enforcement (`iggy.consensus.v1`) lives
/// inside that body and returns HTTP 400 on missing or wrong
/// `Sec-WebSocket-Protocol`; the install path stays thin.
///
/// Socket options (`TCP_NODELAY` + `SO_KEEPALIVE`) apply pre-handshake
/// for symmetry with [`install_client_tls_stream`].
///
/// WSS is shard-0 terminal for the same reasons as the TCP-TLS plane;
/// see [`install_client_tls_stream`] for the rustls non-serialisability
/// argument and the TLS-family clause of invariant I5.
#[allow(clippy::future_not_send)]
pub fn install_client_wss_stream(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    if let Err(e) = apply_keepalive_for_connection(
        &stream,
        cfg.keepalive_idle,
        cfg.keepalive_interval,
        cfg.keepalive_retries,
    ) {
        warn!(
            client = client_id,
            "keepalive failed on accepted WSS client fd: {e}"
        );
    }
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(
            client = client_id,
            "nodelay failed on accepted WSS client fd: {e}"
        );
    }
    install_client_conn(
        bus,
        client_id,
        WssTransportConn::new_server(stream, config).with_drain_budget(cfg.tls_drain_budget),
        on_request,
    );
}

/// WebSocket entry point for client installs.
///
/// Wraps a post-upgrade [`compio_ws::WebSocketStream`] in a
/// [`WsTransportConn`] and delegates to the existing generic
/// [`install_client_conn`]. The HTTP-Upgrade handshake has already been
/// driven on shard 0; the install path never re-runs it.
///
/// Like QUIC, this never crosses an inter-shard channel:
/// `WebSocketStream<TcpStream>` is `!Send` (it holds compio `Rc<...>`
/// driver state) so shard 0 terminates locally and uses the existing
/// `ForwardClientSend` / `Consensus` variants. The post-upgrade socket
/// IS still a raw TCP fd, but compio-ws has no "rebuild from fd" API
/// and re-attaching the tungstenite state machine across shards would
/// lose protocol invariants the dispatcher already enforced.
#[allow(clippy::future_not_send)]
pub fn install_client_ws_stream(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    stream: compio_ws::WebSocketStream<TcpStream>,
    on_request: RequestHandler,
) {
    install_client_conn(
        bus,
        client_id,
        WsTransportConn::new_server(stream),
        on_request,
    );
}

/// Install a pre-wrapped client connection on the bus. Generic over
/// [`TransportConn`]; plane-symmetric with [`install_replica_conn`].
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub fn install_client_conn<C: TransportConn>(
    bus: &Rc<IggyMessageBus>,
    client_id: u128,
    conn: C,
    on_request: RequestHandler,
) {
    let (tx, rx) = async_channel::bounded(bus.peer_queue_capacity());
    let (in_tx, in_rx) =
        async_channel::bounded::<Message<GenericHeader>>(bus.peer_queue_capacity());

    // If the registry insert below loses a race for `client_id`, the
    // losing reader must NOT invoke `on_request` (it would route
    // responses through the wrong registry entry) and must NOT call
    // `close_peer` (it would evict the winner). See the replica path
    // above for the same pattern.
    let install_aborted = Rc::new(Cell::new(false));

    // See replica path for the rationale on instance-token fencing.
    let install_token: Rc<Cell<Option<InstanceToken>>> = Rc::new(Cell::new(None));

    // Per-connection shutdown for fast reader wake on insert race; see
    // the replica installer for the full rationale.
    let (conn_shutdown, conn_token) = Shutdown::new();

    // Bridge bus-wide + per-connection shutdown into the single
    // `ActorContext::shutdown` token consumed by the transport.
    let bus_token_for_bridge = bus.token();
    let conn_token_for_bridge = conn_token.clone();
    let (transport_shutdown, transport_token) = Shutdown::new();
    let bridge_handle = compio::runtime::spawn(async move {
        futures::select! {
            () = bus_token_for_bridge.wait().fuse() => {}
            () = conn_token_for_bridge.wait().fuse() => {}
        }
        transport_shutdown.trigger();
    });
    bus.track_background(bridge_handle);

    let label: &'static str = "client";
    let peer_fmt = format!("{client_id:#034x}");
    let ctx = ActorContext {
        in_tx,
        rx,
        shutdown: transport_token,
        max_batch: bus.config().max_batch,
        max_message_size: bus.config().max_message_size,
        label,
        peer: peer_fmt,
    };

    let bus_for_transport = Rc::clone(bus);
    let aborted_transport = Rc::clone(&install_aborted);
    let token_for_transport = Rc::clone(&install_token);
    let transport_handle = compio::runtime::spawn(async move {
        conn.run(ctx).await;
        if aborted_transport.get() || bus_for_transport.is_shutting_down() {
            return;
        }
        let Some(token) = token_for_transport.get() else {
            return;
        };
        bus_for_transport
            .clients()
            .remove_if_token_matches(client_id, token);
    });

    let bus_for_dispatch = Rc::clone(bus);
    let bus_token_dispatch = bus.token();
    let conn_token_dispatch = conn_token;
    let aborted_dispatch = Rc::clone(&install_aborted);
    let token_for_dispatch = Rc::clone(&install_token);
    let close_peer_timeout = bus.config().close_peer_timeout;
    let dispatch_handle = compio::runtime::spawn(async move {
        client_dispatch_loop(
            client_id,
            in_rx,
            &on_request,
            &bus_token_dispatch,
            &conn_token_dispatch,
            &aborted_dispatch,
        )
        .await;
        if aborted_dispatch.get() {
            debug!(
                client = client_id,
                "aborted client install: skipping post-loop cleanup"
            );
            return;
        }
        if !bus_token_dispatch.is_triggered() {
            let Some(token) = token_for_dispatch.get() else {
                return;
            };
            bus_for_dispatch
                .clients()
                .close_peer_if_token_matches(client_id, token, close_peer_timeout)
                .await;
        }
        info!(client = client_id, "consensus client disconnected");
    });

    match bus.clients().insert(
        client_id,
        tx,
        transport_handle,
        dispatch_handle,
        conn_shutdown,
    ) {
        Ok(token) => {
            install_token.set(Some(token));
        }
        Err(rejected) => {
            // Shard 0 mints client ids as `(target_shard << 112) | seq` with a
            // monotonic `seq` starting at 1, so wrap requires 2^112 mints and
            // a collision here is a bootstrap bug or a foreign id leaking
            // into the setup path. Flip `install_aborted` so the orphan
            // reader drops inbound frames instead of forwarding them via
            // `on_request` (which would route responses through the winner's
            // entry and silently misroute).
            install_aborted.set(true);
            warn!(
                client_id,
                "duplicate client id in registry, dropping delegated fd \
                 (shard 0 counter invariant violated)"
            );
            // See replica installer for the track_background rationale.
            let drain_handle =
                compio::runtime::spawn(drain_rejected_registration(rejected, close_peer_timeout));
            bus.track_background(drain_handle);
        }
    }
}

/// Drive a losing-insert's transport + dispatch [`JoinHandle`]s to
/// completion (or force-cancel at the deadline).
///
/// The winning entry must never be touched here; `install_aborted` has
/// already told both tasks to skip post-loop cleanup. Closing the
/// sender wakes the transport's writer; triggering the per-connection
/// shutdown wakes the transport's reader off its `io_uring` read SQE
/// without waiting for peer EOF. Awaiting both handles with
/// `close_peer_timeout` budget guarantees neither task can outlive the
/// race on a half-open socket. `compio::runtime::JoinHandle::drop`
/// detaches, so letting the handles go out of scope would leak the
/// tasks.
///
/// Both handles share a single `timeout` budget: after the transport
/// returns (or is cancelled) the dispatch only gets the remaining time.
/// Two independent full-timeout awaits would let a stuck loser occupy
/// up to `2 * timeout` of shutdown wall-clock on its own.
#[allow(clippy::future_not_send)]
async fn drain_rejected_registration(rejected: RejectedRegistration, timeout: Duration) {
    let RejectedRegistration {
        sender,
        writer_handle,
        reader_handle,
        conn_shutdown,
    } = rejected;
    sender.close();
    conn_shutdown.trigger();
    let deadline = Instant::now() + timeout;
    let _ = compio::time::timeout(timeout, writer_handle).await;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        let _ = compio::time::timeout(remaining, reader_handle).await;
    }
}

/// Dispatch loop for a delegated replica connection. Pulls inbound
/// consensus messages from the transport's per-connection inbound
/// channel and hands each off to the bus-installed handler.
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-replica-id race. The loop checks it before dispatching
/// each message so the losing dispatcher can never invoke `on_message`
/// with the replica id owned by the winning install — otherwise two
/// physical peers would feed the same VSR slot and break replication
/// safety.
///
/// `conn_token` is a per-connection shutdown the installer triggers
/// from the insert-race path; alongside the bus-wide token, both wake
/// this loop without waiting for the transport to drop its
/// `Sender<Message>`.
#[allow(clippy::future_not_send)]
async fn replica_dispatch_loop(
    replica_id: u8,
    in_rx: Receiver<Message<GenericHeader>>,
    on_message: &MessageHandler,
    token: &ShutdownToken,
    conn_token: &ShutdownToken,
    aborted: &Cell<bool>,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(replica = replica_id, "replica dispatch loop shutting down");
                return;
            }
            () = conn_token.wait().fuse() => {
                debug!(replica = replica_id, "replica dispatch loop aborted by per-connection shutdown");
                return;
            }
            result = in_rx.recv().fuse() => {
                let Ok(msg) = result else {
                    debug!(replica = replica_id, "replica dispatch: inbound queue closed");
                    return;
                };
                if aborted.get() {
                    return;
                }
                on_message(replica_id, msg);
            }
        }
    }
}

/// Dispatch loop for a delegated client connection. Rejects any command
/// other than `Request` (the client side of the consensus protocol only
/// speaks request/reply).
///
/// `aborted` is set by the installer when the registry insert loses a
/// duplicate-client-id race. The loop checks it before dispatching each
/// request so the losing dispatcher can never invoke `on_request` with
/// the client id owned by the winning install.
///
/// `conn_token` is a per-connection shutdown the installer triggers
/// from the insert-race path; alongside the bus-wide token, both wake
/// this loop without waiting for the transport to drop its
/// `Sender<Message>`.
#[allow(clippy::future_not_send)]
async fn client_dispatch_loop(
    client_id: u128,
    in_rx: Receiver<Message<GenericHeader>>,
    on_request: &RequestHandler,
    token: &ShutdownToken,
    conn_token: &ShutdownToken,
    aborted: &Cell<bool>,
) {
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!(client = client_id, "client dispatch loop shutting down");
                return;
            }
            () = conn_token.wait().fuse() => {
                debug!(client = client_id, "client dispatch loop aborted by per-connection shutdown");
                return;
            }
            result = in_rx.recv().fuse() => {
                let Ok(msg) = result else {
                    debug!(client = client_id, "client dispatch: inbound queue closed");
                    return;
                };
                if aborted.get() {
                    return;
                }
                let cmd = msg.header().command;
                if cmd != Command2::Request {
                    warn!(
                        client = client_id,
                        ?cmd,
                        "unexpected command from client, expected Request"
                    );
                    continue;
                }
                on_request(client_id, msg);
            }
        }
    }
}
