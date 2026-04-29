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

//! `Default` impls for the server-ng config surface.
//!
//! Section sub-types are reused verbatim from the legacy server config;
//! their existing `Default` impls (declared in [`crate::defaults`]) read
//! from `core/server/config.toml` via `static_toml!`. This file
//! delegates to those impls for every reused section, so drift between
//! the two TOMLs is intentionally absorbed at the `server-ng` consumer
//! level (the wiring PR will read `core/server-ng/config.toml`
//! end-to-end through [`super::server_ng::ServerNgConfig::load`] and
//! overrides will take effect there).
//!
//! Only [`MessageBusConfig`] needs its own `Default` because no legacy
//! section maps to it. The defaults are hardcoded to match
//! `core::message_bus::config::MessageBusConfig::default()` byte-for-byte.

use super::message_bus::MessageBusConfig;
use super::server_ng::ServerNgConfig;
use crate::server_config::cluster::ClusterConfig;
use crate::server_config::http::HttpConfig;
use crate::server_config::quic::QuicConfig;
use crate::server_config::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MessageSaverConfig,
    PersonalAccessTokenConfig, TelemetryConfig,
};
use crate::server_config::system::SystemConfig;
use crate::server_config::tcp::TcpConfig;
use crate::server_config::websocket::WebSocketConfig;
use iggy_common::IggyByteSize;
use std::sync::Arc;

impl Default for ServerNgConfig {
    fn default() -> ServerNgConfig {
        ServerNgConfig {
            consumer_group: ConsumerGroupConfig::default(),
            data_maintenance: DataMaintenanceConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            message_saver: MessageSaverConfig::default(),
            personal_access_token: PersonalAccessTokenConfig::default(),
            system: Arc::new(SystemConfig::default()),
            quic: QuicConfig::default(),
            tcp: TcpConfig::default(),
            websocket: WebSocketConfig::default(),
            http: HttpConfig::default(),
            telemetry: TelemetryConfig::default(),
            cluster: ClusterConfig::default(),
            message_bus: MessageBusConfig::default(),
        }
    }
}

impl Default for MessageBusConfig {
    fn default() -> MessageBusConfig {
        // Keep these literals in lock-step with
        // `core::message_bus::config::MessageBusConfig::default()` and
        // with the `[message_bus]` block in
        // `core/server-ng/config.toml`. The unit test
        // `message_bus::tests::default_validates` proves the values
        // survive `Validatable::validate`; a future PR may wire a
        // round-trip test against the embedded TOML once
        // `ServerNgConfig::load` is exercised end-to-end.
        MessageBusConfig {
            max_batch: 256,
            max_message_size: IggyByteSize::from(64_u64 * 1024 * 1024),
            peer_queue_capacity: 256,
            reconnect_period: "5 s".parse().expect("'5 s' parses as IggyDuration"),
            keepalive_idle: "10 s".parse().expect("'10 s' parses as IggyDuration"),
            keepalive_interval: "5 s".parse().expect("'5 s' parses as IggyDuration"),
            keepalive_retries: 3,
            close_peer_timeout: "2 s".parse().expect("'2 s' parses as IggyDuration"),
            close_grace: "2 s".parse().expect("'2 s' parses as IggyDuration"),
            tcp_tls_listen_addr: None,
            wss_listen_addr: None,
        }
    }
}
