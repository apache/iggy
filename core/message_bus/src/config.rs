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
//! [`configs::server_ng_config::ServerNgConfig`]. The bus consumes that
//! schema at construction (see [`IggyMessageBus::with_config`]) and
//! converts the schema-typed fields ([`IggyDuration`] / [`IggyByteSize`])
//! into runtime types ([`Duration`] / `usize`) once, so hot paths read
//! fields directly without per-call conversion.
//!
//! The WebSocket frame-layer config lives under `[websocket]` in the
//! schema (single source for both the server's WS listener tuning and
//! the bus's WS install path); the bus pulls
//! `cfg.websocket.to_tungstenite_config()` here.
//!
//! Liveness detection is NOT done via TCP keepalive on the bus: SDK
//! clients manage their own keepalive policy at the application layer,
//! and replica<->replica liveness is observed by VSR heartbeats rather
//! than by `SO_KEEPALIVE`.
//!
//! Neither plane is authenticated at the bus layer: identity and
//! credential checks belong to the caller (`core/server-ng`) via
//! `LOGIN_*` commands. This struct therefore carries no secret /
//! token-source state.

pub use compio::ws::tungstenite::protocol::WebSocketConfig;

use configs::server_ng::ServerNgConfig;
use std::time::Duration;

/// Pre-converted QUIC transport tuning derived from
/// [`ServerNgConfig::quic`](configs::ng_quic::QuicConfig).
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
/// invariants of the SDK-client plane (single bidi stream per peer,
/// no datagram or unidirectional traffic).
#[derive(Debug, Clone)]
pub struct QuicTuning {
    /// Maximum number of concurrent bidirectional streams per
    /// connection. The bus opens exactly one per peer; setting this
    /// above 1 just preallocates unused quinn-proto state.
    pub max_concurrent_bidi_streams: u32,

    /// Buffer size handed to quinn's outbound datagram queue, in
    /// bytes.
    pub datagram_send_buffer_size: usize,

    /// Initial path MTU advertised to the peer, in bytes.
    pub initial_mtu: u16,

    /// Send-flow control window per connection, in bytes.
    pub send_window: u64,

    /// Receive-flow control window per connection, in bytes.
    pub receive_window: u32,

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
/// Built from a fully-validated [`ServerNgConfig`] via
/// [`From<&ServerNgConfig>`] at boot. All fields are runtime-typed
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
    /// Built once at boot from `cfg.websocket.to_tungstenite_config()`
    /// in the [`From<&ServerNgConfig>`] impl below.
    ///
    /// The [`WebSocketConfig`] type is re-exported from `compio_ws`'s
    /// vendored `tungstenite` so callers do not need a direct dep on
    /// `compio_ws` to construct or pattern-match this field.
    pub ws_config: WebSocketConfig,

    /// QUIC transport tuning, pre-converted from
    /// [`ServerNgConfig::quic`](configs::ng_quic::QuicConfig) at boot.
    pub quic: QuicTuning,
}

impl From<&ServerNgConfig> for MessageBusConfig {
    fn from(cfg: &ServerNgConfig) -> Self {
        let bus = &cfg.message_bus;
        Self {
            max_batch: bus.max_batch,
            max_message_size: usize::try_from(bus.max_message_size.as_bytes_u64())
                .expect("message_bus.max_message_size fits usize on supported targets"),
            peer_queue_capacity: bus.peer_queue_capacity,
            reconnect_period: bus.reconnect_period.get_duration(),
            close_peer_timeout: bus.close_peer_timeout.get_duration(),
            close_grace: bus.close_grace.get_duration(),
            handshake_grace: bus.handshake_grace.get_duration(),
            ws_config: build_ws_config(bus),
            quic: build_quic_tuning(&cfg.quic),
        }
    }
}

/// Convert the schema's [`configs::ng_quic::QuicConfig`]
/// (`IggyByteSize` / `IggyDuration` typed) into the runtime
/// [`QuicTuning`] (plain integer / `Duration` fields).
///
/// `expect`s rely on the schema validator (and the embedded default
/// TOML) keeping byte-size and stream-count fields inside
/// `quinn-proto`'s `VarInt` range. On supported targets `usize::MAX`
/// covers anything operators write into the schema, so the conversion
/// only fails for genuinely impossible inputs.
fn build_quic_tuning(quic: &configs::ng_quic::QuicConfig) -> QuicTuning {
    QuicTuning {
        max_concurrent_bidi_streams: u32::try_from(quic.max_concurrent_bidi_streams)
            .expect("quic.max_concurrent_bidi_streams fits in u32"),
        datagram_send_buffer_size: usize::try_from(quic.datagram_send_buffer_size.as_bytes_u64())
            .expect("quic.datagram_send_buffer_size fits usize on supported targets"),
        initial_mtu: u16::try_from(quic.initial_mtu.as_bytes_u64())
            .expect("quic.initial_mtu fits in u16"),
        send_window: quic.send_window.as_bytes_u64(),
        receive_window: u32::try_from(quic.receive_window.as_bytes_u64())
            .expect("quic.receive_window fits in u32 (quinn VarInt accepts u32)"),
        keep_alive_interval: quic.keep_alive_interval.get_duration(),
        max_idle_timeout: quic.max_idle_timeout.get_duration(),
    }
}

impl Default for QuicTuning {
    /// Mirrors the `[quic]` defaults in
    /// `core/server-ng/config.toml`: 64 MiB send/receive windows,
    /// 30 s idle timeout, 10 s keep-alive, 8 KiB initial MTU, 100 KB
    /// datagram send buffer, single bidi stream per peer.
    ///
    /// Intended for tests and direct callers; production builds
    /// derive the field from [`ServerNgConfig`] so the values stay in
    /// lock-step with the on-disk schema.
    fn default() -> Self {
        Self {
            max_concurrent_bidi_streams: 1,
            datagram_send_buffer_size: 100 * 1024,
            initial_mtu: 8 * 1024,
            send_window: 64 * 1024 * 1024,
            receive_window: 64 * 1024 * 1024,
            keep_alive_interval: Duration::from_secs(10),
            max_idle_timeout: Duration::from_secs(30),
        }
    }
}

/// Fold the bus's `ws_*` schema knobs into a single
/// [`tungstenite::WebSocketConfig`].
///
/// Each `Some` overrides the tungstenite default; `None` keeps the
/// crate-wide default. Conversion to `usize` saturates on platforms
/// where `IggyByteSize` would overflow, but the schema validator
/// already constrains all sizes to fit in `u64`, and on supported
/// targets `usize` is at least 32 bits, so saturation is unreachable
/// in practice.
fn build_ws_config(bus: &configs::message_bus::MessageBusConfig) -> WebSocketConfig {
    let mut ws = WebSocketConfig::default();
    if let Some(sz) = bus.ws_max_message_size {
        ws = ws.max_message_size(Some(byte_size_to_usize(sz)));
    }
    if let Some(sz) = bus.ws_max_frame_size {
        ws = ws.max_frame_size(Some(byte_size_to_usize(sz)));
    }
    if let Some(sz) = bus.ws_write_buffer_size {
        ws = ws.write_buffer_size(byte_size_to_usize(sz));
    }
    ws.accept_unmasked_frames(bus.ws_accept_unmasked_frames)
}

#[allow(clippy::cast_possible_truncation)]
fn byte_size_to_usize(sz: iggy_common::IggyByteSize) -> usize {
    let bytes = sz.as_bytes_u64();
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

impl Default for MessageBusConfig {
    fn default() -> Self {
        Self::from(&ServerNgConfig::default())
    }
}
