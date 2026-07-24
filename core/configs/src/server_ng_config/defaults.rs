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

//! `Default` impls for the server-ng config surface.
//!
//! Sections that fork (`tcp`, `websocket`, `quic`, `cluster`,
//! `message_bus`) have their own `Default` impls here, sourced from
//! `core/server-ng/config.toml` via [`SERVER_NG_CONFIG`]. Sections that
//! still reuse legacy types (`http`, `system`, `telemetry`,
//! `consumer_group`, `data_maintenance`, `message_saver`,
//! `personal_access_token`, `heartbeat`) delegate to the legacy
//! `Default` impls; overrides land at the consumer level once
//! [`super::server_ng::ServerNgConfig::load`] is wired into server-ng's
//! bootstrap.

use super::cluster::{
    ClusterAuthConfig, ClusterConfig, ClusterNodeConfig, ClusterTlsConfig, TransportPorts,
};
use super::http_admission::HttpAdmissionConfig;
use super::message_bus::MessageBusConfig;
use super::metadata::MetadataConfig;
use super::partition::PartitionConfig;
use super::quic::{QuicCertificateConfig, QuicConfig, QuicSocketConfig};
use super::server_ng::NgSystemConfig;
use super::server_ng::{ExtraConfig, ServerNgConfig};
use super::tcp::{TcpConfig, TcpSocketConfig, TcpTlsConfig};
use super::websocket::{WebSocketConfig, WebSocketTlsConfig};
use crate::server_config::http::HttpConfig;
use crate::server_config::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MessageSaverConfig,
    PersonalAccessTokenConfig, TelemetryConfig,
};
use std::sync::Arc;

static_toml::static_toml! {
    // static_toml resolves relative to CARGO_MANIFEST_DIR (core/configs/).
    pub static SERVER_NG_CONFIG = include_toml!("../server-ng/config.toml");
}

impl Default for ServerNgConfig {
    fn default() -> ServerNgConfig {
        ServerNgConfig {
            consumer_group: ConsumerGroupConfig::default(),
            data_maintenance: DataMaintenanceConfig::default(),
            extra: ExtraConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            message_saver: MessageSaverConfig::default(),
            personal_access_token: PersonalAccessTokenConfig::default(),
            system: Arc::new(NgSystemConfig::default()),
            quic: QuicConfig::default(),
            tcp: TcpConfig::default(),
            websocket: WebSocketConfig::default(),
            http: HttpConfig::default(),
            http_admission: HttpAdmissionConfig::default(),
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
            enabled: SERVER_NG_CONFIG.cluster.enabled,
            name: SERVER_NG_CONFIG.cluster.name.parse().unwrap(),
            heartbeat_timeout: SERVER_NG_CONFIG.cluster.heartbeat_timeout.parse().unwrap(),
            commit_broadcast_interval: SERVER_NG_CONFIG
                .cluster
                .commit_broadcast_interval
                .parse()
                .unwrap(),
            prepare_retransmit_interval: SERVER_NG_CONFIG
                .cluster
                .prepare_retransmit_interval
                .parse()
                .unwrap(),
            view_change_retransmit_interval: SERVER_NG_CONFIG
                .cluster
                .view_change_retransmit_interval
                .parse()
                .unwrap(),
            view_change_status_timeout: SERVER_NG_CONFIG
                .cluster
                .view_change_status_timeout
                .parse()
                .unwrap(),
            request_start_view_retransmit_interval: SERVER_NG_CONFIG
                .cluster
                .request_start_view_retransmit_interval
                .parse()
                .unwrap(),
            view_probe_attempts_max: SERVER_NG_CONFIG.cluster.view_probe_attempts_max as u32,
            nodes: SERVER_NG_CONFIG
                .cluster
                .nodes
                .iter()
                .map(|node| ClusterNodeConfig {
                    name: node.name.parse().unwrap(),
                    ip: node.ip.parse().unwrap(),
                    replica_id: u8::try_from(node.replica_id).expect(
                        "static_toml replica_id must fit in u8 (0..=255); \
                         fix core/server-ng/config.toml",
                    ),
                    ports: TransportPorts {
                        tcp: Some(u16::try_from(node.ports.tcp).expect(
                            "static_toml cluster.nodes.ports.tcp must fit in u16 (0..=65535); \
                             fix core/server-ng/config.toml",
                        )),
                        quic: Some(u16::try_from(node.ports.quic).expect(
                            "static_toml cluster.nodes.ports.quic must fit in u16 (0..=65535); \
                             fix core/server-ng/config.toml",
                        )),
                        http: Some(u16::try_from(node.ports.http).expect(
                            "static_toml cluster.nodes.ports.http must fit in u16 (0..=65535); \
                             fix core/server-ng/config.toml",
                        )),
                        websocket: Some(u16::try_from(node.ports.websocket).expect(
                            "static_toml cluster.nodes.ports.websocket must fit in u16 (0..=65535); \
                             fix core/server-ng/config.toml",
                        )),
                        tcp_replica: Some(u16::try_from(node.ports.tcp_replica).expect(
                            "static_toml cluster.nodes.ports.tcp_replica must fit in u16 (0..=65535); \
                             fix core/server-ng/config.toml",
                        )),
                    },
                })
                .collect(),
            auth: ClusterAuthConfig::default(),
            tls: ClusterTlsConfig::default(),
        }
    }
}

impl Default for MetadataConfig {
    fn default() -> MetadataConfig {
        // Read from the embedded TOML so the Default impl and the on-disk
        // schema cannot drift (same pattern as MessageBusConfig below).
        let metadata = &SERVER_NG_CONFIG.metadata;
        MetadataConfig {
            prepare_queue_depth: metadata.prepare_queue_depth as usize,
            journal_slots: metadata.journal_slots as usize,
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        // Read from the embedded TOML so the Default impl and the on-disk
        // schema cannot drift (same pattern as MetadataConfig above).
        let partition = &SERVER_NG_CONFIG.partition;
        PartitionConfig {
            prepare_queue_depth: partition.prepare_queue_depth as usize,
        }
    }
}

impl Default for HttpAdmissionConfig {
    fn default() -> HttpAdmissionConfig {
        // Read from the embedded TOML so the Default impl and the on-disk
        // schema cannot drift; the lockstep test pins these to the canonical
        // caps.
        let http_admission = &SERVER_NG_CONFIG.http_admission;
        HttpAdmissionConfig {
            max_in_flight_writes: http_admission.max_in_flight_writes as u32,
            max_in_flight_writes_per_session: http_admission.max_in_flight_writes_per_session
                as u32,
        }
    }
}

impl Default for QuicConfig {
    fn default() -> QuicConfig {
        QuicConfig {
            enabled: SERVER_NG_CONFIG.quic.enabled,
            address: SERVER_NG_CONFIG.quic.address.parse().unwrap(),
            max_concurrent_bidi_streams: SERVER_NG_CONFIG.quic.max_concurrent_bidi_streams as u64,
            datagram_send_buffer_size: SERVER_NG_CONFIG
                .quic
                .datagram_send_buffer_size
                .parse()
                .unwrap(),
            initial_mtu: SERVER_NG_CONFIG.quic.initial_mtu.parse().unwrap(),
            send_window: SERVER_NG_CONFIG.quic.send_window.parse().unwrap(),
            receive_window: SERVER_NG_CONFIG.quic.receive_window.parse().unwrap(),
            keep_alive_interval: SERVER_NG_CONFIG.quic.keep_alive_interval.parse().unwrap(),
            max_idle_timeout: SERVER_NG_CONFIG.quic.max_idle_timeout.parse().unwrap(),
            certificate: QuicCertificateConfig::default(),
            socket: QuicSocketConfig::default(),
        }
    }
}

impl Default for QuicSocketConfig {
    fn default() -> QuicSocketConfig {
        QuicSocketConfig {
            override_defaults: SERVER_NG_CONFIG.quic.socket.override_defaults,
            recv_buffer_size: SERVER_NG_CONFIG
                .quic
                .socket
                .recv_buffer_size
                .parse()
                .unwrap(),
            send_buffer_size: SERVER_NG_CONFIG
                .quic
                .socket
                .send_buffer_size
                .parse()
                .unwrap(),
            keepalive: SERVER_NG_CONFIG.quic.socket.keepalive,
        }
    }
}

impl Default for QuicCertificateConfig {
    fn default() -> QuicCertificateConfig {
        QuicCertificateConfig {
            self_signed: SERVER_NG_CONFIG.quic.certificate.self_signed,
            cert_file: SERVER_NG_CONFIG.quic.certificate.cert_file.parse().unwrap(),
            key_file: SERVER_NG_CONFIG.quic.certificate.key_file.parse().unwrap(),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> TcpConfig {
        TcpConfig {
            enabled: SERVER_NG_CONFIG.tcp.enabled,
            address: SERVER_NG_CONFIG.tcp.address.parse().unwrap(),
            ipv6: SERVER_NG_CONFIG.tcp.ipv_6,
            tls: TcpTlsConfig::default(),
            socket: TcpSocketConfig::default(),
            socket_migration: SERVER_NG_CONFIG.tcp.socket_migration,
        }
    }
}

impl Default for TcpTlsConfig {
    fn default() -> TcpTlsConfig {
        TcpTlsConfig {
            enabled: SERVER_NG_CONFIG.tcp.tls.enabled,
            self_signed: SERVER_NG_CONFIG.tcp.tls.self_signed,
            cert_file: SERVER_NG_CONFIG.tcp.tls.cert_file.parse().unwrap(),
            key_file: SERVER_NG_CONFIG.tcp.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for TcpSocketConfig {
    fn default() -> TcpSocketConfig {
        TcpSocketConfig {
            override_defaults: SERVER_NG_CONFIG.tcp.socket.override_defaults,
            recv_buffer_size: SERVER_NG_CONFIG
                .tcp
                .socket
                .recv_buffer_size
                .parse()
                .unwrap(),
            send_buffer_size: SERVER_NG_CONFIG
                .tcp
                .socket
                .send_buffer_size
                .parse()
                .unwrap(),
            keepalive: SERVER_NG_CONFIG.tcp.socket.keepalive,
            nodelay: SERVER_NG_CONFIG.tcp.socket.nodelay,
            linger: SERVER_NG_CONFIG.tcp.socket.linger.parse().unwrap(),
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> WebSocketConfig {
        WebSocketConfig {
            enabled: SERVER_NG_CONFIG.websocket.enabled,
            address: SERVER_NG_CONFIG.websocket.address.parse().unwrap(),
            read_buffer_size: None,
            write_buffer_size: None,
            max_write_buffer_size: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: false,
            tls: WebSocketTlsConfig::default(),
        }
    }
}

impl Default for WebSocketTlsConfig {
    fn default() -> WebSocketTlsConfig {
        WebSocketTlsConfig {
            enabled: SERVER_NG_CONFIG.websocket.tls.enabled,
            self_signed: SERVER_NG_CONFIG.websocket.tls.self_signed,
            cert_file: SERVER_NG_CONFIG.websocket.tls.cert_file.parse().unwrap(),
            key_file: SERVER_NG_CONFIG.websocket.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for MessageBusConfig {
    fn default() -> MessageBusConfig {
        // Read every field from the embedded TOML so the Default impl
        // and the on-disk schema cannot drift. Sibling impls in this
        // file follow the same pattern. The `ws_*_size` knobs are
        // optional in the schema (commented-out by default), so they
        // map to `None` here when absent.
        let bus = &SERVER_NG_CONFIG.message_bus;
        MessageBusConfig {
            max_batch: bus.max_batch as usize,
            max_message_size: bus.max_message_size.parse().unwrap(),
            peer_queue_capacity: bus.peer_queue_capacity as usize,
            reconnect_period: bus.reconnect_period.parse().unwrap(),
            close_peer_timeout: bus.close_peer_timeout.parse().unwrap(),
            close_grace: bus.close_grace.parse().unwrap(),
            handshake_grace: bus.handshake_grace.parse().unwrap(),
            ws_max_message_size: None,
            ws_max_frame_size: None,
            ws_write_buffer_size: None,
            ws_accept_unmasked_frames: bus.ws_accept_unmasked_frames,
        }
    }
}
