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

//! `Default` impls for the sections this module owns (`tcp`, `websocket`,
//! `quic`, `cluster`, `metadata`, `partition`, `message_bus`), sourced
//! from `core/server/config.toml` via [`SERVER_CONFIG`]. Sections drawn
//! from [`crate::common`] (`http`, `system`, `telemetry`,
//! `consumer_group`, `data_maintenance`, `personal_access_token`,
//! `heartbeat`) delegate to the `Default` impls in
//! [`crate::common::defaults`].

use super::cluster::{
    ClusterAuthConfig, ClusterConfig, ClusterCoordinatorConfig, ClusterNodeConfig,
    ClusterTlsConfig, TransportPorts,
};
use super::message_bus::MessageBusConfig;
use super::metadata::MetadataConfig;
use super::node::NodeConfig;
use super::partition::PartitionConfig;
use super::quic::{QuicCertificateConfig, QuicConfig};
use super::server::ServerConfig;
use super::server::ServerSystemConfig;
use super::tcp::{TcpConfig, TcpTlsConfig};
use super::websocket::{WebSocketConfig, WebSocketTlsConfig};
use crate::common::http::HttpConfig;
use crate::common::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, PersonalAccessTokenConfig,
    TelemetryConfig,
};
use std::num::NonZeroU32;
use std::sync::Arc;

// Same embedded TOML the shared sections read; re-exported so sibling
// modules reach it as `super::defaults::SERVER_CONFIG`.
pub use crate::common::defaults::SERVER_CONFIG;

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            consumer_group: ConsumerGroupConfig::default(),
            data_maintenance: DataMaintenanceConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            node: NodeConfig::default(),
            personal_access_token: PersonalAccessTokenConfig::default(),
            system: Arc::new(ServerSystemConfig::default()),
            quic: QuicConfig::default(),
            tcp: TcpConfig::default(),
            websocket: WebSocketConfig::default(),
            http: HttpConfig::default(),
            telemetry: TelemetryConfig::default(),
            cluster: ClusterConfig::default(),
            metadata: MetadataConfig::default(),
            partition: PartitionConfig::default(),
            message_bus: MessageBusConfig::default(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> ClusterConfig {
        ClusterConfig {
            enabled: SERVER_CONFIG.cluster.enabled,
            name: SERVER_CONFIG.cluster.name.parse().unwrap(),
            heartbeat_timeout: SERVER_CONFIG.cluster.heartbeat_timeout.parse().unwrap(),
            commit_broadcast_interval: SERVER_CONFIG
                .cluster
                .commit_broadcast_interval
                .parse()
                .unwrap(),
            prepare_retransmit_interval: SERVER_CONFIG
                .cluster
                .prepare_retransmit_interval
                .parse()
                .unwrap(),
            view_change_retransmit_interval: SERVER_CONFIG
                .cluster
                .view_change_retransmit_interval
                .parse()
                .unwrap(),
            view_change_status_timeout: SERVER_CONFIG
                .cluster
                .view_change_status_timeout
                .parse()
                .unwrap(),
            superblock_wedged_fatal_timeout: SERVER_CONFIG
                .cluster
                .superblock_wedged_fatal_timeout
                .parse()
                .unwrap(),
            request_start_view_retransmit_interval: SERVER_CONFIG
                .cluster
                .request_start_view_retransmit_interval
                .parse()
                .unwrap(),
            view_probe_attempts_max: SERVER_CONFIG.cluster.view_probe_attempts_max as u32,
            repair_retry_interval: SERVER_CONFIG
                .cluster
                .repair_retry_interval
                .parse()
                .unwrap(),
            repair_gap_debounce_interval: SERVER_CONFIG
                .cluster
                .repair_gap_debounce_interval
                .parse()
                .unwrap(),
            repair_chunk_max: SERVER_CONFIG.cluster.repair_chunk_max as usize,
            nodes: SERVER_CONFIG
                .cluster
                .nodes
                .iter()
                .map(|node| ClusterNodeConfig {
                    name: node.name.parse().unwrap(),
                    ip: node.ip.parse().unwrap(),
                    advertised_address: None,
                    advertised_addresses: Vec::new(),
                    replica_id: u8::try_from(node.replica_id).expect(
                        "static_toml replica_id must fit in u8 (0..=255); \
                         fix core/server/config.toml",
                    ),
                    ports: TransportPorts {
                        tcp: Some(u16::try_from(node.ports.tcp).expect(
                            "static_toml cluster.nodes.ports.tcp must fit in u16 (0..=65535); \
                             fix core/server/config.toml",
                        )),
                        quic: Some(u16::try_from(node.ports.quic).expect(
                            "static_toml cluster.nodes.ports.quic must fit in u16 (0..=65535); \
                             fix core/server/config.toml",
                        )),
                        http: Some(u16::try_from(node.ports.http).expect(
                            "static_toml cluster.nodes.ports.http must fit in u16 (0..=65535); \
                             fix core/server/config.toml",
                        )),
                        websocket: Some(u16::try_from(node.ports.websocket).expect(
                            "static_toml cluster.nodes.ports.websocket must fit in u16 (0..=65535); \
                             fix core/server/config.toml",
                        )),
                        tcp_replica: Some(u16::try_from(node.ports.tcp_replica).expect(
                            "static_toml cluster.nodes.ports.tcp_replica must fit in u16 (0..=65535); \
                             fix core/server/config.toml",
                        )),
                    },
                })
                .collect(),
            auth: ClusterAuthConfig::default(),
            tls: ClusterTlsConfig::default(),
            coordinator: ClusterCoordinatorConfig::default(),
        }
    }
}

impl Default for MetadataConfig {
    fn default() -> MetadataConfig {
        // Read from the embedded TOML so the Default impl and the on-disk
        // schema cannot drift (same pattern as MessageBusConfig below).
        let metadata = &SERVER_CONFIG.metadata;
        MetadataConfig {
            prepare_queue_depth: metadata.prepare_queue_depth as usize,
            journal_slots: metadata.journal_slots as usize,
            clients_table_max: metadata.clients_table_max as usize,
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        // Read from the embedded TOML so the Default impl and the on-disk
        // schema cannot drift (same pattern as MetadataConfig above).
        let partition = &SERVER_CONFIG.partition;
        PartitionConfig {
            prepare_queue_depth: partition.prepare_queue_depth as usize,
            dedup_clients_max: partition.dedup_clients_max as usize,
            consumer_offsets_max: partition.consumer_offsets_max as usize,
            offset_reservation_lease: NonZeroU32::new(partition.offset_reservation_lease as u32)
                .expect("the embedded config.toml carries a nonzero offset_reservation_lease"),
            evicted_ring_capacity: partition.evicted_ring_capacity as usize,
            evicted_ring_bytes_max: partition.evicted_ring_bytes_max.parse().unwrap(),
            transfer_served_cache_bytes_max: partition
                .transfer_served_cache_bytes_max
                .parse()
                .unwrap(),
            transfer_artifact_bytes_max: partition.transfer_artifact_bytes_max.parse().unwrap(),
        }
    }
}

impl Default for QuicConfig {
    fn default() -> QuicConfig {
        QuicConfig {
            enabled: SERVER_CONFIG.quic.enabled,
            address: SERVER_CONFIG.quic.address.parse().unwrap(),
            max_concurrent_bidi_streams: SERVER_CONFIG.quic.max_concurrent_bidi_streams as u64,
            initial_mtu: SERVER_CONFIG.quic.initial_mtu.parse().unwrap(),
            send_window: SERVER_CONFIG.quic.send_window.parse().unwrap(),
            receive_window: SERVER_CONFIG.quic.receive_window.parse().unwrap(),
            stream_receive_window: SERVER_CONFIG.quic.stream_receive_window.parse().unwrap(),
            keep_alive_interval: SERVER_CONFIG.quic.keep_alive_interval.parse().unwrap(),
            max_idle_timeout: SERVER_CONFIG.quic.max_idle_timeout.parse().unwrap(),
            certificate: QuicCertificateConfig::default(),
        }
    }
}

impl Default for QuicCertificateConfig {
    fn default() -> QuicCertificateConfig {
        QuicCertificateConfig {
            self_signed: SERVER_CONFIG.quic.certificate.self_signed,
            cert_file: SERVER_CONFIG.quic.certificate.cert_file.parse().unwrap(),
            key_file: SERVER_CONFIG.quic.certificate.key_file.parse().unwrap(),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> TcpConfig {
        TcpConfig {
            enabled: SERVER_CONFIG.tcp.enabled,
            address: SERVER_CONFIG.tcp.address.parse().unwrap(),
            tls: TcpTlsConfig::default(),
        }
    }
}

impl Default for TcpTlsConfig {
    fn default() -> TcpTlsConfig {
        TcpTlsConfig {
            enabled: SERVER_CONFIG.tcp.tls.enabled,
            self_signed: SERVER_CONFIG.tcp.tls.self_signed,
            cert_file: SERVER_CONFIG.tcp.tls.cert_file.parse().unwrap(),
            key_file: SERVER_CONFIG.tcp.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> WebSocketConfig {
        // The size knobs are optional in the schema (commented-out by
        // default), so they map to `None` here when absent; every other
        // field comes from the embedded TOML so the Default impl and
        // the on-disk schema cannot drift.
        WebSocketConfig {
            enabled: SERVER_CONFIG.websocket.enabled,
            address: SERVER_CONFIG.websocket.address.parse().unwrap(),
            read_buffer_size: None,
            write_buffer_size: None,
            max_write_buffer_size: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: SERVER_CONFIG.websocket.accept_unmasked_frames,
            tls: WebSocketTlsConfig::default(),
        }
    }
}

impl Default for WebSocketTlsConfig {
    fn default() -> WebSocketTlsConfig {
        WebSocketTlsConfig {
            enabled: SERVER_CONFIG.websocket.tls.enabled,
            self_signed: SERVER_CONFIG.websocket.tls.self_signed,
            cert_file: SERVER_CONFIG.websocket.tls.cert_file.parse().unwrap(),
            key_file: SERVER_CONFIG.websocket.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for MessageBusConfig {
    fn default() -> MessageBusConfig {
        // Read every field from the embedded TOML so the Default impl
        // and the on-disk schema cannot drift. Sibling impls in this
        // file follow the same pattern.
        let bus = &SERVER_CONFIG.message_bus;
        MessageBusConfig {
            max_batch: bus.max_batch as usize,
            max_message_size: bus.max_message_size.parse().unwrap(),
            peer_queue_capacity: bus.peer_queue_capacity as usize,
            reconnect_period: bus.reconnect_period.parse().unwrap(),
            close_peer_timeout: bus.close_peer_timeout.parse().unwrap(),
            close_grace: bus.close_grace.parse().unwrap(),
            handshake_grace: bus.handshake_grace.parse().unwrap(),
        }
    }
}
