/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! On-disk schema for the inter-shard / inter-replica message bus.
//!
//! Mirrors the runtime `core::message_bus::config::MessageBusConfig`,
//! using configs-crate idioms:
//!
//! - `Duration` -> [`IggyDuration`] (DisplayFromStr-serde, `"5 s"` syntax)
//! - `usize` / `u32` -> kept as the underlying integer type
//!
//! Transport-section concerns the bus does NOT own:
//!
//! - TCP-TLS / WSS listen addresses derive from `[tcp]` + `[tcp.tls]`
//!   and `[websocket]` + `[websocket.tls]` respectively. The future
//!   wiring PR populates the bus's runtime listen-addr inputs from
//!   those sections at bootstrap.
//!
//! Tunables the bus owns directly:
//!
//! - Bus-internal abstractions the operator does not see anywhere else
//!   in the schema (batch sizing, per-peer queue depth, close-grace,
//!   reconnect period, handshake-grace).
//! - WebSocket frame-layer tunables for the bus's WS / WSS install
//!   path (`ws_max_message_size`, `ws_max_frame_size`,
//!   `ws_write_buffer_size`, `ws_accept_unmasked_frames`). The bus
//!   plane carries SDK-client traffic with cardinality and burst
//!   characteristics distinct from the legacy `[websocket]` listener,
//!   so the bus owns its own frame-layer ceiling rather than aliasing
//!   the listener's. The runtime conversion folds these into a
//!   `tungstenite::WebSocketConfig` once at bus construction.
//!
//! Liveness detection is NOT done via TCP keepalive on the bus: SDK
//! clients manage their own keepalive policy; replica<->replica
//! liveness is observed by VSR heartbeats. No keepalive knobs live in
//! this section.
//!
//! Construction of the runtime type from this struct happens in the
//! follow-up PR that wires `core/server-ng` to call
//! [`super::server_ng::ServerNgConfig::load`].

use super::COMPONENT_NG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};

/// Hard upper bound on [`MessageBusConfig::max_batch`], in iovecs.
///
/// Mirrors `core::message_bus::config::IOV_MAX_LIMIT`. Duplicated here
/// rather than depended on, so `core/configs` does not need a build-time
/// dependency on `core/message_bus` (the runtime crate is the eventual
/// consumer of this config; reversing the edge would invert the workspace
/// graph). The runtime crate re-asserts the invariant inside
/// `IggyMessageBus::with_config`. A unit test below pins the literal so
/// any future bump on the runtime side surfaces as a configs-build
/// failure until both are reconciled.
pub const IOV_MAX_LIMIT_NG: usize = 512;

/// Tunables for the message bus that ships consensus traffic between
/// replicas and SDK-client traffic between shards.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessageBusConfig {
    /// Maximum number of `BusMessage` entries the writer task coalesces
    /// into a single `writev(2)` call. Higher values amortise syscalls
    /// at the cost of tail latency. Capped at [`IOV_MAX_LIMIT_NG`].
    pub max_batch: usize,

    /// Wire-level cap on a single framed message. Read-side validator;
    /// undersize or oversize frames are rejected and the connection torn
    /// down.
    #[config_env(leaf)]
    pub max_message_size: IggyByteSize,

    /// Bound on the per-peer mpsc queue. Writer task drains; the
    /// `send_to_*` path enqueues. Too small drops under burst; too
    /// large delays backpressure signalling.
    pub peer_queue_capacity: usize,

    /// Interval between outbound reconnect attempts to peers with
    /// `peer_id > self_id`.
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub reconnect_period: IggyDuration,

    /// Timeout for per-peer close drain (flush writer, tear down
    /// reader) before force-cancellation.
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub close_peer_timeout: IggyDuration,

    /// Wall-clock bound on a single `stream.shutdown()` (or
    /// `ws.close()`) invocation in the safe-shutdown sequence of the
    /// TLS-family transports. Independent of [`Self::close_peer_timeout`]
    /// (which bounds the registry-level drain over both reader and
    /// writer joins).
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub close_grace: IggyDuration,

    /// Wall-clock bound on a single connection's handshake phase: the
    /// rustls accept (TCP-TLS), the WS HTTP-Upgrade (plain WS), the
    /// combined TLS + WS handshakes (WSS, sharing one budget end-to-end),
    /// and the QUIC `connecting.await` + first `accept_bi.await` pair.
    /// Threaded into `compio::time::timeout(handshake_grace, ...)` at
    /// each handshake site so a slowloris peer cannot pin per-conn
    /// channels + registry slot + spawned task indefinitely.
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub handshake_grace: IggyDuration,

    /// Hard upper bound on a single inbound WebSocket message
    /// (post-fragment-reassembly). `None` keeps the tungstenite
    /// default (currently 64 MiB). Threaded into
    /// `tungstenite::WebSocketConfig::max_message_size` at bus
    /// construction. Distinct from [`Self::max_message_size`], which
    /// caps a single framed bus payload regardless of transport.
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ws_max_message_size: Option<IggyByteSize>,

    /// Hard upper bound on a single inbound WebSocket frame
    /// (pre-fragment-reassembly). `None` keeps the tungstenite default
    /// (currently 16 MiB). Threaded into
    /// `tungstenite::WebSocketConfig::max_frame_size` at bus
    /// construction.
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ws_max_frame_size: Option<IggyByteSize>,

    /// Target buffer size for batched WebSocket writes before tungstenite
    /// flushes. `None` keeps the tungstenite default. Threaded into
    /// `tungstenite::WebSocketConfig::write_buffer_size` at bus
    /// construction.
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ws_write_buffer_size: Option<IggyByteSize>,

    /// Whether the bus accepts unmasked frames from clients in violation
    /// of RFC 6455 client-to-server framing rules. Default `false`
    /// (strict). Set to `true` only for non-browser test clients that
    /// emit unmasked frames; production deployments leave this at
    /// `false`. Threaded into
    /// `tungstenite::WebSocketConfig::accept_unmasked_frames` at bus
    /// construction.
    #[serde(default)]
    pub ws_accept_unmasked_frames: bool,
}

impl Validatable<ConfigurationError> for MessageBusConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_batch == 0 {
            eprintln!("{COMPONENT_NG} message_bus.max_batch must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_batch > IOV_MAX_LIMIT_NG {
            eprintln!(
                "{COMPONENT_NG} message_bus.max_batch ({}) exceeds IOV_MAX_LIMIT ({IOV_MAX_LIMIT_NG})",
                self.max_batch
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.peer_queue_capacity == 0 {
            eprintln!("{COMPONENT_NG} message_bus.peer_queue_capacity must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_message_size.as_bytes_u64() == 0 {
            eprintln!("{COMPONENT_NG} message_bus.max_message_size must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.handshake_grace.as_micros() == 0 {
            eprintln!("{COMPONENT_NG} message_bus.handshake_grace must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.close_grace.as_micros() == 0 {
            eprintln!("{COMPONENT_NG} message_bus.close_grace must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.close_peer_timeout.as_micros() == 0 {
            eprintln!("{COMPONENT_NG} message_bus.close_peer_timeout must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.reconnect_period.as_micros() == 0 {
            eprintln!("{COMPONENT_NG} message_bus.reconnect_period must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // WS frame chain: ws_max_frame_size <= ws_max_message_size <=
        // max_message_size. The `Option<...>` knobs only enforce a ceiling
        // when present; an absent value defers to tungstenite's default
        // (which is itself <= 64 MiB and so satisfies the chain in practice).
        if let (Some(frame), Some(message)) = (self.ws_max_frame_size, self.ws_max_message_size)
            && frame.as_bytes_u64() > message.as_bytes_u64()
        {
            eprintln!(
                "{COMPONENT_NG} message_bus.ws_max_frame_size ({}) exceeds ws_max_message_size ({})",
                frame.as_bytes_u64(),
                message.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(message) = self.ws_max_message_size
            && message.as_bytes_u64() > self.max_message_size.as_bytes_u64()
        {
            eprintln!(
                "{COMPONENT_NG} message_bus.ws_max_message_size ({}) exceeds max_message_size ({})",
                message.as_bytes_u64(),
                self.max_message_size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(frame) = self.ws_max_frame_size
            && frame.as_bytes_u64() > self.max_message_size.as_bytes_u64()
        {
            eprintln!(
                "{COMPONENT_NG} message_bus.ws_max_frame_size ({}) exceeds max_message_size ({})",
                frame.as_bytes_u64(),
                self.max_message_size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> MessageBusConfig {
        MessageBusConfig::default()
    }

    #[test]
    fn default_validates() {
        baseline().validate().expect("default config validates");
    }

    #[test]
    fn rejects_zero_max_batch() {
        let mut c = baseline();
        c.max_batch = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_max_batch_above_iov_max() {
        let mut c = baseline();
        c.max_batch = IOV_MAX_LIMIT_NG + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_max_batch_at_iov_max() {
        let mut c = baseline();
        c.max_batch = IOV_MAX_LIMIT_NG;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_peer_queue_capacity() {
        let mut c = baseline();
        c.peer_queue_capacity = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_max_message_size() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(0_u64);
        assert!(c.validate().is_err());
    }

    /// Tripwire: pins the local copy of `IOV_MAX_LIMIT` against the
    /// runtime crate's value. If `core/message_bus` ever bumps its
    /// `IOV_MAX_LIMIT`, this test fails the configs build until the
    /// duplicate here is updated. We pin the literal because
    /// `core/configs` does not depend on `core/message_bus`.
    #[test]
    fn iov_max_limit_matches_runtime_crate() {
        assert_eq!(IOV_MAX_LIMIT_NG, 512);
    }

    #[test]
    fn rejects_zero_close_grace() {
        let mut c = baseline();
        c.close_grace = IggyDuration::from(std::time::Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_close_peer_timeout() {
        let mut c = baseline();
        c.close_peer_timeout = IggyDuration::from(std::time::Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_reconnect_period() {
        let mut c = baseline();
        c.reconnect_period = IggyDuration::from(std::time::Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_ws_frame_size_above_ws_message_size() {
        let mut c = baseline();
        c.ws_max_message_size = Some(IggyByteSize::from(1024_u64));
        c.ws_max_frame_size = Some(IggyByteSize::from(2048_u64));
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_ws_message_size_above_bus_max_message_size() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(1024_u64);
        c.ws_max_message_size = Some(IggyByteSize::from(2048_u64));
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_ws_frame_size_above_bus_max_message_size() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(1024_u64);
        c.ws_max_frame_size = Some(IggyByteSize::from(2048_u64));
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_ws_chain_in_ascending_order() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(64_u64 * 1024 * 1024);
        c.ws_max_message_size = Some(IggyByteSize::from(32_u64 * 1024 * 1024));
        c.ws_max_frame_size = Some(IggyByteSize::from(16_u64 * 1024 * 1024));
        assert!(c.validate().is_ok());
    }
}
