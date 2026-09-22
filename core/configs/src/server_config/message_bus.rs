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
//!   and `[websocket]` + `[websocket.tls]` respectively.
//! - WebSocket frame-layer tuning (buffer sizes, message / frame
//!   ceilings, unmasked-frame acceptance) lives in `[websocket]`
//!   ([`super::websocket::WebSocketConfig`]): the bus IS the server's
//!   WS / WSS install path, so the listener section carries the frame
//!   tuning and the runtime folds it into a compio-ws
//!   `WebSocketConfig` once at bus construction. The
//!   `websocket.max_*` <= `message_bus.max_message_size` chain is
//!   enforced as a cross-section check in
//!   [`super::server::ServerConfig`]'s validator.
//!
//! Tunables the bus owns directly: bus-internal abstractions the
//! operator does not see anywhere else in the schema (batch sizing,
//! per-peer queue depth, close-grace, reconnect period,
//! handshake-grace).
//!
//! Liveness detection is NOT done via TCP keepalive on the bus: SDK
//! clients manage their own keepalive policy; replica<->replica
//! liveness is observed by VSR heartbeats. No keepalive knobs live in
//! this section.
//!
//! Construction of the runtime type from this struct happens in the
//! follow-up PR that wires `core/server` to call
//! [`super::server::ServerConfig::load`].

use super::COMPONENT;
use super::defaults::SERVER_CONFIG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, IggyDuration, MAX_MESSAGE_SIZE_UPPER_BYTES, Validatable};
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
pub const IOV_MAX_LIMIT: usize = 512;

/// Floor for a nonzero [`MessageBusConfig::replica_read_buffer_size`].
/// A buffer below one page costs more reads and copies than no buffer.
const MIN_REPLICA_READ_BUFFER_BYTES: u64 = 4 * 1024;

/// Ceiling for a nonzero [`MessageBusConfig::replica_read_buffer_size`].
/// The buffer is allocated per replica link on every shard, and a fill
/// cannot take more than the socket receive queue holds, so a value above
/// this is a typo rather than a tuning choice.
const MAX_REPLICA_READ_BUFFER_BYTES: u64 = 16 * 1024 * 1024;

/// Tunables for the message bus that ships consensus traffic between
/// replicas and SDK-client traffic between shards.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessageBusConfig {
    /// Maximum number of `BusMessage` entries the writer task coalesces
    /// into a single `writev(2)` call. Higher values amortise syscalls
    /// at the cost of tail latency. Capped at [`IOV_MAX_LIMIT`].
    pub max_batch: usize,

    /// Wire-level cap on a single framed message. Read-side validator;
    /// undersize or oversize frames are rejected and the connection torn
    /// down.
    #[config_env(leaf)]
    pub max_message_size: IggyByteSize,

    /// Read-ahead buffer per plaintext replica link. Costs one buffer of
    /// this size per installed connection. The framing layer decodes
    /// every complete frame a fill delivered, so a burst costs one read
    /// instead of one or two per frame. A read at least this size goes
    /// straight to the socket, so a body larger than the buffer crosses
    /// it only for the part the header's fill already pulled in. Zero
    /// keeps the unbuffered path. No effect under `cluster.tls`, whose
    /// reader buffers inside compio's `SyncStream` adapter, nor on the
    /// client plane, which never wraps its read half.
    #[serde(default = "default_replica_read_buffer_size")]
    #[config_env(leaf)]
    pub replica_read_buffer_size: IggyByteSize,

    /// Bound on each replica peer's mpsc queue. Writer task drains; the
    /// `send_to_*` path enqueues. Too small drops under burst; too
    /// large delays backpressure signalling.
    pub peer_queue_capacity: usize,

    /// Bound on each SDK connection's inbound and outbound queues. Separate
    /// from replica queues so repair bursts do not enlarge every client allocation.
    #[serde(default = "default_client_queue_capacity")]
    pub client_queue_capacity: usize,

    /// Interval between outbound reconnect attempts to peers with
    /// `peer_id > self_id`. Also bounds each dial, so a peer that drops
    /// SYNs cannot stall the sweep.
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
}

impl Validatable<ConfigurationError> for MessageBusConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_batch == 0 {
            eprintln!("{COMPONENT} message_bus.max_batch must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_batch > IOV_MAX_LIMIT {
            eprintln!(
                "{COMPONENT} message_bus.max_batch ({}) exceeds IOV_MAX_LIMIT ({IOV_MAX_LIMIT})",
                self.max_batch
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // The one key in this section where zero is legal: it selects the
        // unbuffered read path, so the A/B baseline is a config change
        // rather than a separate build.
        let replica_read_buffer = self.replica_read_buffer_size.as_bytes_u64();
        if replica_read_buffer != 0
            && !(MIN_REPLICA_READ_BUFFER_BYTES..=MAX_REPLICA_READ_BUFFER_BYTES)
                .contains(&replica_read_buffer)
        {
            eprintln!(
                "{COMPONENT} message_bus.replica_read_buffer_size ({replica_read_buffer}) must be \
                 0 (unbuffered) or between {MIN_REPLICA_READ_BUFFER_BYTES} and \
                 {MAX_REPLICA_READ_BUFFER_BYTES} bytes"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.peer_queue_capacity == 0 {
            eprintln!("{COMPONENT} message_bus.peer_queue_capacity must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.client_queue_capacity == 0 {
            eprintln!("{COMPONENT} message_bus.client_queue_capacity must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_message_size.as_bytes_u64() == 0 {
            eprintln!("{COMPONENT} message_bus.max_message_size must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_message_size.as_bytes_u64() > MAX_MESSAGE_SIZE_UPPER_BYTES {
            eprintln!(
                "{COMPONENT} message_bus.max_message_size ({}) exceeds the frozen ceiling of \
                 {MAX_MESSAGE_SIZE_UPPER_BYTES} bytes: boot-time segment recovery derives fixed \
                 scan and allocation limits from the widest legal wire frame, so batches admitted \
                 above the ceiling would be refused as implausible by recovery on a later boot",
                self.max_message_size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.handshake_grace.as_micros() == 0 {
            eprintln!("{COMPONENT} message_bus.handshake_grace must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.close_grace.as_micros() == 0 {
            eprintln!("{COMPONENT} message_bus.close_grace must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.close_peer_timeout.as_micros() == 0 {
            eprintln!("{COMPONENT} message_bus.close_peer_timeout must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.reconnect_period.as_micros() == 0 {
            eprintln!("{COMPONENT} message_bus.reconnect_period must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

/// Zero fails validation, so the value comes from the embedded `config.toml`.
fn default_client_queue_capacity() -> usize {
    SERVER_CONFIG.message_bus.client_queue_capacity as usize
}

/// [`IggyByteSize`]'s own `Default` is 0 bytes, which turns the read-ahead
/// buffer off, so the value comes from the embedded `config.toml`.
fn default_replica_read_buffer_size() -> IggyByteSize {
    SERVER_CONFIG
        .message_bus
        .replica_read_buffer_size
        .parse()
        .expect("message_bus.replica_read_buffer_size is a byte size")
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
        c.max_batch = IOV_MAX_LIMIT + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_max_batch_at_iov_max() {
        let mut c = baseline();
        c.max_batch = IOV_MAX_LIMIT;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_peer_queue_capacity() {
        let mut c = baseline();
        c.peer_queue_capacity = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn missing_client_queue_capacity_keeps_independent_default() {
        let mut config = serde_json::to_value(baseline()).unwrap();
        config
            .as_object_mut()
            .unwrap()
            .remove("client_queue_capacity");
        config["peer_queue_capacity"] = serde_json::json!(8192);
        let decoded: MessageBusConfig = serde_json::from_value(config).unwrap();
        assert_eq!(
            decoded.client_queue_capacity,
            baseline().client_queue_capacity
        );
        assert_eq!(decoded.peer_queue_capacity, 8192);
        decoded.validate().unwrap();
    }

    #[test]
    fn rejects_zero_client_queue_capacity() {
        let mut config = baseline();
        config.client_queue_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_replica_read_buffer_is_32_kib() {
        assert_eq!(
            baseline().replica_read_buffer_size.as_bytes_u64(),
            32 * 1024
        );
    }

    /// Zero is the unbuffered path, not a misconfiguration: it is the
    /// A/B baseline arm and must survive validation.
    #[test]
    fn accepts_zero_replica_read_buffer() {
        let mut c = baseline();
        c.replica_read_buffer_size = IggyByteSize::from(0_u64);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn accepts_replica_read_buffer_at_floor() {
        let mut c = baseline();
        c.replica_read_buffer_size = IggyByteSize::from(MIN_REPLICA_READ_BUFFER_BYTES);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_replica_read_buffer_below_floor() {
        let mut c = baseline();
        c.replica_read_buffer_size = IggyByteSize::from(MIN_REPLICA_READ_BUFFER_BYTES - 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_replica_read_buffer_at_ceiling() {
        let mut c = baseline();
        c.replica_read_buffer_size = IggyByteSize::from(MAX_REPLICA_READ_BUFFER_BYTES);
        assert!(c.validate().is_ok());
    }

    /// A mistyped size allocates per link at connect time, long after
    /// boot, so validation is the only place that can still refuse it.
    #[test]
    fn rejects_replica_read_buffer_above_ceiling() {
        let mut c = baseline();
        c.replica_read_buffer_size = IggyByteSize::from(MAX_REPLICA_READ_BUFFER_BYTES + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn replica_read_buffer_parses_byte_size_strings() {
        for (text, expected) in [("0", 0_u64), ("64 KiB", 64 * 1024)] {
            let mut config = serde_json::to_value(baseline()).unwrap();
            config["replica_read_buffer_size"] = serde_json::json!(text);
            let decoded: MessageBusConfig = serde_json::from_value(config).unwrap();
            assert_eq!(decoded.replica_read_buffer_size.as_bytes_u64(), expected);
            decoded.validate().unwrap();
        }
    }

    #[test]
    fn missing_replica_read_buffer_keeps_config_default() {
        let mut config = serde_json::to_value(baseline()).unwrap();
        config
            .as_object_mut()
            .unwrap()
            .remove("replica_read_buffer_size");
        let decoded: MessageBusConfig = serde_json::from_value(config).unwrap();
        assert_eq!(
            decoded.replica_read_buffer_size,
            baseline().replica_read_buffer_size
        );
    }

    #[test]
    fn rejects_zero_max_message_size() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(0_u64);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_max_message_size_above_frozen_ceiling() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(MAX_MESSAGE_SIZE_UPPER_BYTES + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_max_message_size_at_frozen_ceiling() {
        let mut c = baseline();
        c.max_message_size = IggyByteSize::from(MAX_MESSAGE_SIZE_UPPER_BYTES);
        assert!(c.validate().is_ok());
    }

    /// Tripwire: pins the local copy of `IOV_MAX_LIMIT` against the
    /// runtime crate's value. If `core/message_bus` ever bumps its
    /// `IOV_MAX_LIMIT`, this test fails the configs build until the
    /// duplicate here is updated. We pin the literal because
    /// `core/configs` does not depend on `core/message_bus`.
    #[test]
    fn iov_max_limit_matches_runtime_crate() {
        assert_eq!(IOV_MAX_LIMIT, 512);
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
}
