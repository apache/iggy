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

//! Runtime tunables for the message bus.
//!
//! Single source of truth for these knobs is the on-disk schema
//! [`configs::server::ServerConfig`]. The bus consumes that
//! schema at construction (see [`crate::IggyMessageBus::with_config`])
//! and converts the schema-typed fields
//! ([`iggy_common::IggyDuration`] / [`iggy_common::IggyByteSize`])
//! into runtime types ([`Duration`] / `usize`) once, so hot paths read
//! fields directly without per-call conversion.
//!
//! The WebSocket frame-layer config the bus consumes lives under the
//! schema's `[websocket]` block (buffer sizes, message / frame
//! ceilings, unmasked-frame acceptance): the bus IS the server's
//! WS / WSS install path, so the listener section carries the frame
//! tuning (see `configs::websocket`).
//! [`From<&ServerConfig> for MessageBusConfig`](MessageBusConfig)
//! folds that section into [`WebSocketConfig`] once at boot.
//!
//! Liveness detection is NOT done via TCP keepalive on the bus: SDK
//! clients manage their own keepalive policy at the application layer,
//! and replica<->replica liveness is observed by VSR heartbeats rather
//! than by `SO_KEEPALIVE`.
//!
//! Neither plane is authenticated at the bus layer: identity and
//! credential checks belong to the caller (`core/server`) via
//! `LOGIN_*` commands. This struct therefore carries no secret /
//! token-source state.

pub use compio::ws::tungstenite::protocol::WebSocketConfig;

use configs::server::ServerConfig;
use std::time::Duration;

/// Pre-converted QUIC transport tuning derived from
/// [`ServerConfig::quic`](configs::quic::QuicConfig).
///
/// Threaded into [`crate::transports::quic::transport_config_from`] at
/// every bind site so the schema's `[quic]` block actually drives
/// `quinn-proto`'s `TransportConfig`. Hot paths read these fields
/// directly without per-bind `IggyDuration` / `IggyByteSize`
/// conversion.
///
/// `keep_alive_interval` and `max_idle_timeout` follow the legacy
/// QUIC server's convention: a zero `Duration` means *disabled* and
/// the corresponding quinn knob is left unset.
///
/// Hardcoded knobs the bus does NOT expose: `max_concurrent_uni_streams = 0`
/// and the CUBIC congestion controller. Both are architectural
/// invariants of the SDK-client plane (no datagram or unidirectional
/// traffic).
#[derive(Debug, Clone)]
pub struct QuicTuning {
    /// Maximum number of concurrent bidirectional streams per
    /// connection. Each SDK command opens a fresh bidi stream, so
    /// this caps how many commands one client connection may have
    /// in flight; 1 disables pipelining.
    pub max_concurrent_bidi_streams: u32,

    /// Initial path MTU advertised to the peer, in bytes.
    pub initial_mtu: u16,

    /// Send-flow control window per connection, in bytes.
    pub send_window: u64,

    /// Receive-flow control window per connection, in bytes.
    pub receive_window: u32,

    /// Receive-flow control window per stream, in bytes. Never above
    /// [`Self::receive_window`], and equal to it under the shipped
    /// single-stream default. Setting it strictly below is what keeps
    /// one unread stream from pinning the whole connection window, so
    /// it only buys anything once several streams share a connection.
    pub stream_receive_window: u32,

    /// Interval between QUIC keep-alive PINGs. `Duration::ZERO`
    /// disables keep-alive; the connection then relies entirely on
    /// [`Self::max_idle_timeout`] for liveness.
    pub keep_alive_interval: Duration,

    /// Idle timeout after which quinn closes the connection.
    /// `Duration::ZERO` disables the timer (not recommended).
    pub max_idle_timeout: Duration,
}

/// Hard upper bound on `max_batch`, in iovecs.
///
/// Linux's `IOV_MAX` is 1024 (`/usr/include/bits/uio_lim.h`). Future WS
/// transports emit one iovec for the header and one for the body, so a
/// batch of N messages costs `2 * N` iovecs; cap `max_batch` at
/// `IOV_MAX / 2 = 512` to keep that worst case below the syscall limit.
/// Bus construction asserts this in [`crate::IggyMessageBus::with_config`];
/// breaching it at boot panics rather than silently delivering writev
/// `EMSGSIZE` errors on every batch.
pub const IOV_MAX_LIMIT: usize = 512;

/// Pre-converted runtime tunables in effect on a `IggyMessageBus`
/// instance.
///
/// Built from a fully-validated [`ServerConfig`] via
/// [`From<&ServerConfig>`] at boot. All fields are runtime-typed
/// (`Duration`, `usize`, `tungstenite::WebSocketConfig`) so hot paths
/// read them directly without `.get_duration()` / `.as_bytes_u64()`
/// conversion.
///
/// Test code that wants to override a single field can use the
/// struct-update syntax:
/// ```ignore
/// let t = MessageBusConfig {
///     peer_queue_capacity: 8,
///     ..MessageBusConfig::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct MessageBusConfig {
    /// Maximum number of `BusMessage` entries coalesced into a single
    /// `writev(2)` call by the writer task. Higher values improve
    /// syscall amortization at the cost of tail latency.
    pub max_batch: usize,

    /// Wire-level cap on a single framed message, in bytes. Read-side
    /// validator; undersize or oversize frames are rejected.
    pub max_message_size: usize,

    /// Bound on the per-peer mpsc queue. The writer task drains; the
    /// `send_to_*` path enqueues. Too small drops under burst; too
    /// large delays backpressure signalling.
    pub peer_queue_capacity: usize,

    /// Interval between outbound reconnect attempts to peers with
    /// `peer_id > self_id`.
    pub reconnect_period: Duration,

    /// Number of peer replicas this node expects in its mesh
    /// (`cluster.nodes.len() - 1`, or 0 with clustering disabled).
    /// Used only for mesh-formation observability: an info log fires
    /// when the owner table first holds this many peers.
    pub mesh_expected_peers: usize,

    /// Timeout for per-peer close drain (flush writer, tear down
    /// reader) before force-cancellation.
    pub close_peer_timeout: Duration,

    /// Wall-clock bound on a single `stream.shutdown()` (or `ws.close()`)
    /// invocation in the safe-shutdown sequence. Threaded into the
    /// single-task close path of the TLS-family transports
    /// (`transports::tcp_tls`, `transports::wss`) and consumed inside
    /// `compio::time::timeout(close_grace, ...)`.
    ///
    /// Independent of [`Self::close_peer_timeout`] (which bounds the
    /// registry-level drain over both reader and writer joins).
    pub close_grace: Duration,

    /// Wall-clock bound on a single connection's handshake phase: the
    /// rustls accept (TCP-TLS), the WS HTTP-Upgrade (plain WS), the
    /// combined TLS + WS handshakes (WSS, sharing one budget end-to-end),
    /// and the QUIC `connecting.await` + first `accept_bi.await` pair.
    /// Threaded into `compio::time::timeout(handshake_grace, ...)` at
    /// each handshake site so a slowloris peer cannot pin per-conn
    /// channels + registry slot + spawned task indefinitely.
    pub handshake_grace: Duration,

    /// WebSocket frame-layer tunables (read/write buffer sizes, max
    /// frame size, max message size, accept-unmasked-frames flag).
    /// Threaded into `compio_ws::accept_async_with_config` on the WS
    /// install path and into `WssTransportConn::ws_handshake` for WSS.
    /// Built once at boot by `build_ws_config` (see the
    /// [`From<&ServerConfig> for MessageBusConfig`](MessageBusConfig) impl below)
    /// from the schema's `[websocket]` section, the live frame-tuning
    /// source for the server's WS plane.
    ///
    /// The [`WebSocketConfig`] type is re-exported from `compio_ws`'s
    /// vendored `tungstenite` so callers do not need a direct dep on
    /// `compio_ws` to construct or pattern-match this field.
    pub ws_config: WebSocketConfig,

    /// QUIC transport tuning, pre-converted from
    /// [`ServerConfig::quic`](configs::quic::QuicConfig) at boot.
    pub quic: QuicTuning,
}

impl From<&ServerConfig> for MessageBusConfig {
    fn from(cfg: &ServerConfig) -> Self {
        let bus = &cfg.message_bus;
        // Production load goes through `ServerConfig::validate()`, which
        // already exercises `bus.validate()`. This debug-assert catches
        // direct callers (tests, simulators) that build a `ServerConfig`
        // by hand and forget to validate before converting.
        debug_assert!(
            <configs::message_bus::MessageBusConfig as iggy_common::Validatable<
                configs::ConfigurationError,
            >>::validate(bus)
            .is_ok(),
            "MessageBusConfig::from(&ServerConfig) called on an unvalidated bus config",
        );
        Self {
            max_batch: bus.max_batch,
            max_message_size: usize::try_from(bus.max_message_size.as_bytes_u64())
                .expect("message_bus.max_message_size fits usize on supported targets"),
            peer_queue_capacity: bus.peer_queue_capacity,
            reconnect_period: bus.reconnect_period.get_duration(),
            mesh_expected_peers: if cfg.cluster.enabled {
                cfg.cluster.nodes.len().saturating_sub(1)
            } else {
                0
            },
            close_peer_timeout: bus.close_peer_timeout.get_duration(),
            close_grace: bus.close_grace.get_duration(),
            handshake_grace: bus.handshake_grace.get_duration(),
            ws_config: build_ws_config(&cfg.websocket),
            quic: build_quic_tuning(&cfg.quic),
        }
    }
}

/// Convert the schema's [`configs::quic::QuicConfig`]
/// (`IggyByteSize` / `IggyDuration` typed) into the runtime
/// [`QuicTuning`] (plain integer / `Duration` fields).
///
/// Range invariants (each numeric field fits its target type, MTU at
/// least 1200, `receive_window` within `u32::MAX`, `send_window` within
/// quinn's `VarInt` cap) are enforced by `QuicConfig::validate()`. The
/// `unwrap_or` arms below are still bounded saturations that keep
/// the build unconditionally infallible if a future caller skips
/// validation in dev / test code.
fn build_quic_tuning(quic: &configs::quic::QuicConfig) -> QuicTuning {
    QuicTuning {
        max_concurrent_bidi_streams: u32::try_from(quic.max_concurrent_bidi_streams)
            .unwrap_or(u32::MAX),
        initial_mtu: u16::try_from(quic.initial_mtu.as_bytes_u64()).unwrap_or(u16::MAX),
        send_window: quic.send_window.as_bytes_u64(),
        receive_window: u32::try_from(quic.receive_window.as_bytes_u64()).unwrap_or(u32::MAX),
        stream_receive_window: u32::try_from(quic.stream_receive_window.as_bytes_u64())
            .unwrap_or(u32::MAX),
        keep_alive_interval: quic.keep_alive_interval.get_duration(),
        max_idle_timeout: quic.max_idle_timeout.get_duration(),
    }
}

impl Default for QuicTuning {
    /// Mirrors the `[quic]` defaults in
    /// `core/server/config.toml`: 64 MiB send/receive windows, a per-stream
    /// window equal to the connection receive window, 30 s idle timeout, 10 s
    /// keep-alive, 1200 B initial MTU, one in-flight command per connection.
    ///
    /// Intended for tests and direct callers; production builds
    /// derive the field from [`ServerConfig`] so the values stay in
    /// lock-step with the on-disk schema.
    fn default() -> Self {
        Self {
            max_concurrent_bidi_streams: 1,
            initial_mtu: 1200,
            send_window: 64 * 1024 * 1024,
            receive_window: 64 * 1024 * 1024,
            stream_receive_window: 64 * 1024 * 1024,
            keep_alive_interval: Duration::from_secs(10),
            max_idle_timeout: Duration::from_secs(30),
        }
    }
}

/// Fold the schema's `[websocket]` frame-tuning knobs into a single
/// [`tungstenite::WebSocketConfig`].
///
/// The standalone `tungstenite` crate may be a different major version
/// than the one re-exported by `compio_ws`, so the conversion lives in
/// this crate (next to the `compio_ws` dependency) and constructs the
/// config through the re-export to guarantee type compatibility.
///
/// Each `Some` overrides the compio-ws default; `None` keeps it.
/// Conversion to `usize` saturates on platforms where `IggyByteSize`
/// would overflow, but on supported targets `usize` is at least 32
/// bits, so saturation is unreachable in practice.
fn build_ws_config(websocket: &configs::websocket::WebSocketConfig) -> WebSocketConfig {
    let mut ws = WebSocketConfig::default();
    if let Some(sz) = websocket.read_buffer_size {
        ws = ws.read_buffer_size(byte_size_to_usize(sz));
    }
    if let Some(sz) = websocket.write_buffer_size {
        ws = ws.write_buffer_size(byte_size_to_usize(sz));
    }
    if let Some(sz) = websocket.max_write_buffer_size {
        ws = ws.max_write_buffer_size(byte_size_to_usize(sz));
    }
    if let Some(sz) = websocket.max_message_size {
        ws = ws.max_message_size(Some(byte_size_to_usize(sz)));
    }
    if let Some(sz) = websocket.max_frame_size {
        ws = ws.max_frame_size(Some(byte_size_to_usize(sz)));
    }
    ws.accept_unmasked_frames(websocket.accept_unmasked_frames)
}

#[allow(clippy::cast_possible_truncation)]
fn byte_size_to_usize(sz: iggy_common::IggyByteSize) -> usize {
    let bytes = sz.as_bytes_u64();
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

impl Default for MessageBusConfig {
    fn default() -> Self {
        Self::from(&ServerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `QuicTuning::default()` carries hand-coded literals that must
    /// match the schema-derived path through
    /// `From<&ServerConfig> for MessageBusConfig`. If the embedded
    /// TOML or the literals drift, every test that uses
    /// `QuicTuning::default()` (e.g. `quic_client_roundtrip`) silently
    /// observes different bytes than production. Pin both sides here.
    #[test]
    fn quic_tuning_default_matches_schema() {
        let schema_quic = MessageBusConfig::from(&ServerConfig::default()).quic;
        let literal = QuicTuning::default();

        assert_eq!(
            schema_quic.max_concurrent_bidi_streams,
            literal.max_concurrent_bidi_streams
        );
        assert_eq!(
            schema_quic.initial_mtu, literal.initial_mtu,
            "schema initial_mtu {} bytes vs literal {} bytes",
            schema_quic.initial_mtu, literal.initial_mtu
        );
        assert_eq!(schema_quic.send_window, literal.send_window);
        assert_eq!(schema_quic.receive_window, literal.receive_window);
        assert_eq!(
            schema_quic.stream_receive_window,
            literal.stream_receive_window
        );
        assert_eq!(schema_quic.keep_alive_interval, literal.keep_alive_interval);
        assert_eq!(schema_quic.max_idle_timeout, literal.max_idle_timeout);
    }
}
