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

//! `Display` impls for the sections this module owns.
//!
//! Sections drawn from [`crate::common`] pick up [`Display`] from
//! [`crate::displays`]; this module only adds the top-level
//! [`ServerConfig`] formatter and the [`MessageBusConfig`] section
//! formatter.

use super::message_bus::MessageBusConfig;
use super::metadata::MetadataConfig;
use super::partition::PartitionConfig;
use super::quic::{QuicCertificateConfig, QuicConfig};
use super::server::ServerConfig;
use super::tcp::{TcpConfig, TcpTlsConfig};
use std::fmt::{Display, Formatter};

impl Display for ServerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ consumer_group: {}, data_maintenance: {}, \
             heartbeat: {}, system: {}, quic: {}, tcp: {}, http: {}, telemetry: {}, \
             metadata: {}, message_bus: {}, partition: {} }}",
            self.consumer_group,
            self.data_maintenance,
            self.heartbeat,
            self.system,
            self.quic,
            self.tcp,
            self.http,
            self.telemetry,
            self.metadata,
            self.message_bus,
            self.partition,
        )
    }
}

impl Display for PartitionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ prepare_queue_depth: {}, offset_reservation_lease: {}, \
             evicted_ring_capacity: {}, evicted_ring_bytes_max: {}, \
             transfer_served_cache_bytes_max: {}, transfer_artifact_bytes_max: {} }}",
            self.prepare_queue_depth,
            self.offset_reservation_lease,
            self.evicted_ring_capacity,
            self.evicted_ring_bytes_max,
            self.transfer_served_cache_bytes_max,
            self.transfer_artifact_bytes_max,
        )
    }
}

impl Display for MetadataConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ prepare_queue_depth: {}, journal_slots: {}, clients_table_max: {} }}",
            self.prepare_queue_depth, self.journal_slots, self.clients_table_max,
        )
    }
}

impl Display for MessageBusConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_batch: {}, max_message_size: {}, peer_queue_capacity: {}, \
             reconnect_period: {}, close_peer_timeout: {}, close_grace: {}, \
             handshake_grace: {} }}",
            self.max_batch,
            self.max_message_size,
            self.peer_queue_capacity,
            self.reconnect_period,
            self.close_peer_timeout,
            self.close_grace,
            self.handshake_grace,
        )
    }
}

impl Display for TcpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, tls: {} }}",
            self.enabled, self.address, self.tls
        )
    }
}

impl Display for TcpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, self_signed: {}, cert_file: {}, key_file: {} }}",
            self.enabled, self.self_signed, self.cert_file, self.key_file
        )
    }
}

impl Display for QuicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_concurrent_bidi_streams: {}, initial_mtu: {}, send_window: {}, receive_window: {}, stream_receive_window: {}, keep_alive_interval: {}, max_idle_timeout: {}, certificate: {} }}",
            self.enabled,
            self.address,
            self.max_concurrent_bidi_streams,
            self.initial_mtu,
            self.send_window,
            self.receive_window,
            self.stream_receive_window,
            self.keep_alive_interval,
            self.max_idle_timeout,
            self.certificate
        )
    }
}

impl Display for QuicCertificateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ self_signed: {}, cert_file: {}, key_file: {} }}",
            self.self_signed, self.cert_file, self.key_file
        )
    }
}
