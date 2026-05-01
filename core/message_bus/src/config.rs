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
//! These knobs are consensus-liveness-critical (keepalive + reconnect
//! timers gate view-change latency; batch sizes gate throughput under
//! backpressure) and must be deployment-tunable.
//!
//! TODO: move this module into `core/configs` (as a `MessageBusConfig`
//! section on `ServerConfig`) once the downstream bootstrap that
//! constructs [`crate::IggyMessageBus`] from `ServerConfig` lands.
//! Keeping the type in-crate for now avoids churning the configs crate
//! ahead of that wiring.

use std::time::Duration;

/// Aggregated runtime configuration for an `IggyMessageBus` instance.
///
/// All fields map onto constants that previously lived inline across
/// `writer_task`, `framing`, `socket_opts`, `installer`, `connector`,
/// and `lifecycle::connection_registry`. [`Default`] returns the prior
/// hardcoded values so existing call sites (tests, single-shard
/// simulators) can migrate without behaviour change.
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

    /// TCP keepalive idle timer. See `tcp(7)` `TCP_KEEPIDLE`.
    pub keepalive_idle: Duration,

    /// TCP keepalive probe interval. See `tcp(7)` `TCP_KEEPINTVL`.
    pub keepalive_interval: Duration,

    /// TCP keepalive retry count. See `tcp(7)` `TCP_KEEPCNT`. Combined
    /// with idle + interval this gates the half-open connection
    /// detection window that VSR view-change timers must accommodate.
    pub keepalive_retries: u32,

    /// Timeout for per-peer close drain (flush writer, tear down
    /// reader) before force-cancellation.
    pub close_peer_timeout: Duration,
}

impl Default for MessageBusConfig {
    fn default() -> Self {
        Self {
            // Match `peer_queue_capacity` so a saturated queue drains in a
            // single `writev(2)` call. Previously capped at 64, which
            // forced 4x writevs per full burst and delayed the next
            // backpressure signal by the extra syscalls. Still well below
            // `IOV_MAX=1024` on Linux.
            max_batch: 256,
            max_message_size: 64 * 1024 * 1024,
            peer_queue_capacity: 256,
            reconnect_period: Duration::from_secs(5),
            keepalive_idle: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(5),
            keepalive_retries: 3,
            close_peer_timeout: Duration::from_secs(2),
        }
    }
}
