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

//! `Display` impls for the server-ng config surface.
//!
//! Reused section types pick up [`Display`] from
//! [`crate::displays`]; this module only adds the top-level
//! [`ServerNgConfig`] formatter and the new [`MessageBusConfig`]
//! section formatter.

use super::message_bus::MessageBusConfig;
use super::server_ng::ServerNgConfig;
use std::fmt::{Display, Formatter};

impl Display for ServerNgConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ consumer_group: {}, data_maintenance: {}, message_saver: {}, heartbeat: {}, \
             system: {}, quic: {}, tcp: {}, http: {}, telemetry: {}, message_bus: {} }}",
            self.consumer_group,
            self.data_maintenance,
            self.message_saver,
            self.heartbeat,
            self.system,
            self.quic,
            self.tcp,
            self.http,
            self.telemetry,
            self.message_bus,
        )
    }
}

impl Display for MessageBusConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_batch: {}, max_message_size: {}, peer_queue_capacity: {}, \
             reconnect_period: {}, keepalive_idle: {}, keepalive_interval: {}, \
             keepalive_retries: {}, close_peer_timeout: {}, close_grace: {}, \
             tcp_tls_listen_addr: {:?}, wss_listen_addr: {:?} }}",
            self.max_batch,
            self.max_message_size,
            self.peer_queue_capacity,
            self.reconnect_period,
            self.keepalive_idle,
            self.keepalive_interval,
            self.keepalive_retries,
            self.close_peer_timeout,
            self.close_grace,
            self.tcp_tls_listen_addr,
            self.wss_listen_addr,
        )
    }
}
