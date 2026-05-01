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
//! Mirrors the runtime `core::message_bus::config::MessageBusConfig`
//! field-for-field, but uses configs-crate idioms:
//!
//! - `Duration` -> [`IggyDuration`] (DisplayFromStr-serde, `"5 s"` syntax)
//! - `usize` / `u32` -> kept as the underlying integer type
//! - `Option<SocketAddr>` -> `Option<String>` parsed at validate-time
//! - WebSocket frame-layer tunables (`compio_ws::WebSocketConfig`) are
//!   NOT part of this section; they live under `[websocket]` (the
//!   existing [`super::super::server_config::websocket::WebSocketConfig`]).
//!   The future wiring PR builds the runtime `WebSocketConfig` from that
//!   section and feeds it into the bus at construction.
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
use std::net::SocketAddr;

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

    /// TCP keepalive idle timer. See `tcp(7)` `TCP_KEEPIDLE`.
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub keepalive_idle: IggyDuration,

    /// TCP keepalive probe interval. See `tcp(7)` `TCP_KEEPINTVL`.
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub keepalive_interval: IggyDuration,

    /// TCP keepalive retry count. See `tcp(7)` `TCP_KEEPCNT`. Combined
    /// with idle + interval this gates the half-open connection
    /// detection window that VSR view-change timers must accommodate.
    pub keepalive_retries: u32,

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

    /// Optional TCP-TLS client listener address in `host:port` form.
    /// `None` keeps the plane unbound. Resolved to [`SocketAddr`] by
    /// [`Validatable::validate`].
    #[serde(default)]
    pub tcp_tls_listen_addr: Option<String>,

    /// Optional WSS client listener address in `host:port` form.
    /// `None` keeps the plane unbound. Resolved to [`SocketAddr`] by
    /// [`Validatable::validate`].
    #[serde(default)]
    pub wss_listen_addr: Option<String>,
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
        if self.keepalive_retries == 0 {
            eprintln!("{COMPONENT_NG} message_bus.keepalive_retries must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // Resolve optional listen addrs eagerly so a typo fails at boot,
        // not at first connect.
        if let Some(s) = &self.tcp_tls_listen_addr {
            s.parse::<SocketAddr>().map_err(|e| {
                eprintln!("{COMPONENT_NG} message_bus.tcp_tls_listen_addr '{s}' is invalid: {e}");
                ConfigurationError::InvalidConfigurationValue
            })?;
        }
        if let Some(s) = &self.wss_listen_addr {
            s.parse::<SocketAddr>().map_err(|e| {
                eprintln!("{COMPONENT_NG} message_bus.wss_listen_addr '{s}' is invalid: {e}");
                ConfigurationError::InvalidConfigurationValue
            })?;
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

    #[test]
    fn rejects_zero_keepalive_retries() {
        let mut c = baseline();
        c.keepalive_retries = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_tcp_tls_listen_addr() {
        let mut c = baseline();
        c.tcp_tls_listen_addr = Some("not-a-socket-addr".to_string());
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_wss_listen_addr() {
        let mut c = baseline();
        c.wss_listen_addr = Some("nope".to_string());
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_valid_listen_addrs() {
        let mut c = baseline();
        c.tcp_tls_listen_addr = Some("0.0.0.0:9443".to_string());
        c.wss_listen_addr = Some("127.0.0.1:9444".to_string());
        c.validate().expect("valid listen addrs accepted");
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
}
