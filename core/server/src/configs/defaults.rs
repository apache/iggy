/* Licensed to the Apache Software Foundation (ASF) under one
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

use super::sharding::ShardingConfig;
use super::system::MemoryPoolConfig;
use super::tcp::TcpSocketConfig;
use crate::configs::cluster::{ClusterConfig, ClusterNodeConfig, NodeConfig};
use crate::configs::http::{
    HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig,
};
use crate::configs::quic::{QuicCertificateConfig, QuicConfig, QuicSocketConfig};
use crate::configs::server::{
    DataMaintenanceConfig, HeartbeatConfig, MessageSaverConfig, MessagesMaintenanceConfig,
    PersonalAccessTokenCleanerConfig, PersonalAccessTokenConfig, ServerConfig, TelemetryConfig,
    TelemetryLogsConfig, TelemetryTracesConfig,
};
use crate::configs::system::{
    BackupConfig, CompatibilityConfig, CompressionConfig, EncryptionConfig, LoggingConfig,
    MessageDeduplicationConfig, PartitionConfig, RecoveryConfig, RuntimeConfig, SegmentConfig,
    StateConfig, StreamConfig, SystemConfig, TopicConfig,
};
use crate::configs::tcp::{TcpConfig, TcpTlsConfig};
use crate::configs::websocket::{WebSocketConfig, WebSocketTlsConfig};
use iggy_common::IggyByteSize;
use iggy_common::IggyDuration;
use std::sync::Arc;
use std::time::Duration;

static_toml::static_toml! {
    // static_toml crate always starts from CARGO_MANIFEST_DIR (in this case iggy-server root directory)
    pub static SERVER_CONFIG = include_toml!("../configs/server.toml");
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
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
        }
    }
}

impl Default for MessagesMaintenanceConfig {
    fn default() -> MessagesMaintenanceConfig {
        MessagesMaintenanceConfig {
            cleaner_enabled: SERVER_CONFIG.data_maintenance.messages.cleaner_enabled,
            interval: SERVER_CONFIG
                .data_maintenance
                .messages
                .interval
                .parse()
                .unwrap(),
        }
    }
}

impl Default for QuicConfig {
    fn default() -> QuicConfig {
        QuicConfig {
            enabled: SERVER_CONFIG.quic.enabled,
            address: SERVER_CONFIG.quic.address.parse().unwrap(),
            max_concurrent_bidi_streams: SERVER_CONFIG.quic.max_concurrent_bidi_streams as u64,
            datagram_send_buffer_size: SERVER_CONFIG
                .quic
                .datagram_send_buffer_size
                .parse()
                .unwrap(),
            initial_mtu: SERVER_CONFIG.quic.initial_mtu.parse().unwrap(),
            send_window: SERVER_CONFIG.quic.send_window.parse().unwrap(),
            receive_window: SERVER_CONFIG.quic.receive_window.parse().unwrap(),
            keep_alive_interval: SERVER_CONFIG.quic.keep_alive_interval.parse().unwrap(),
            max_idle_timeout: SERVER_CONFIG.quic.max_idle_timeout.parse().unwrap(),
            certificate: QuicCertificateConfig::default(),
            socket: QuicSocketConfig::default(),
        }
    }
}

impl Default for QuicSocketConfig {
    fn default() -> QuicSocketConfig {
        QuicSocketConfig {
            override_defaults: SERVER_CONFIG.quic.socket.override_defaults,
            recv_buffer_size: SERVER_CONFIG.quic.socket.recv_buffer_size.parse().unwrap(),
            send_buffer_size: SERVER_CONFIG.quic.socket.send_buffer_size.parse().unwrap(),
            keepalive: SERVER_CONFIG.quic.socket.keepalive,
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
            ipv6: SERVER_CONFIG.tcp.ipv_6,
            tls: TcpTlsConfig::default(),
            socket: TcpSocketConfig::default(),
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

impl Default for TcpSocketConfig {
    fn default() -> TcpSocketConfig {
        TcpSocketConfig {
            override_defaults: false,
            recv_buffer_size: IggyByteSize::from(100_000_u64),
            send_buffer_size: IggyByteSize::from(100_000_u64),
            keepalive: false,
            nodelay: false,
            linger: IggyDuration::new(Duration::new(0, 0)),
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> WebSocketConfig {
        WebSocketConfig {
            enabled: SERVER_CONFIG.websocket.enabled,
            address: SERVER_CONFIG.websocket.address.parse().unwrap(),
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
            enabled: SERVER_CONFIG.websocket.tls.enabled,
            self_signed: SERVER_CONFIG.websocket.tls.self_signed,
            cert_file: SERVER_CONFIG.websocket.tls.cert_file.parse().unwrap(),
            key_file: SERVER_CONFIG.websocket.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> HttpConfig {
        HttpConfig {
            enabled: SERVER_CONFIG.http.enabled,
            address: SERVER_CONFIG.http.address.parse().unwrap(),
            max_request_size: SERVER_CONFIG.http.max_request_size.parse().unwrap(),
            cors: HttpCorsConfig::default(),
            jwt: HttpJwtConfig::default(),
            metrics: HttpMetricsConfig::default(),
            tls: HttpTlsConfig::default(),
        }
    }
}

impl Default for HttpCorsConfig {
    fn default() -> HttpCorsConfig {
        HttpCorsConfig {
            enabled: SERVER_CONFIG.http.cors.enabled,
            allowed_methods: SERVER_CONFIG
                .http
                .cors
                .allowed_methods
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            allowed_origins: SERVER_CONFIG
                .http
                .cors
                .allowed_origins
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            allowed_headers: SERVER_CONFIG
                .http
                .cors
                .allowed_headers
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            exposed_headers: SERVER_CONFIG
                .http
                .cors
                .exposed_headers
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            allow_credentials: SERVER_CONFIG.http.cors.allow_credentials,
            allow_private_network: SERVER_CONFIG.http.cors.allow_private_network,
        }
    }
}

impl Default for HttpJwtConfig {
    fn default() -> HttpJwtConfig {
        HttpJwtConfig {
            algorithm: SERVER_CONFIG.http.jwt.algorithm.parse().unwrap(),
            issuer: SERVER_CONFIG.http.jwt.issuer.parse().unwrap(),
            audience: SERVER_CONFIG.http.jwt.audience.parse().unwrap(),
            valid_issuers: SERVER_CONFIG
                .http
                .jwt
                .valid_issuers
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            valid_audiences: SERVER_CONFIG
                .http
                .jwt
                .valid_audiences
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            access_token_expiry: SERVER_CONFIG.http.jwt.access_token_expiry.parse().unwrap(),
            clock_skew: SERVER_CONFIG.http.jwt.clock_skew.parse().unwrap(),
            not_before: SERVER_CONFIG.http.jwt.not_before.parse().unwrap(),
            encoding_secret: SERVER_CONFIG.http.jwt.encoding_secret.parse().unwrap(),
            decoding_secret: SERVER_CONFIG.http.jwt.decoding_secret.parse().unwrap(),
            use_base64_secret: SERVER_CONFIG.http.jwt.use_base_64_secret,
        }
    }
}

impl Default for HttpMetricsConfig {
    fn default() -> HttpMetricsConfig {
        HttpMetricsConfig {
            enabled: SERVER_CONFIG.http.metrics.enabled,
            endpoint: SERVER_CONFIG.http.metrics.endpoint.parse().unwrap(),
        }
    }
}

impl Default for HttpTlsConfig {
    fn default() -> HttpTlsConfig {
        HttpTlsConfig {
            enabled: SERVER_CONFIG.http.tls.enabled,
            cert_file: SERVER_CONFIG.http.tls.cert_file.parse().unwrap(),
            key_file: SERVER_CONFIG.http.tls.key_file.parse().unwrap(),
        }
    }
}

impl Default for MessageSaverConfig {
    fn default() -> MessageSaverConfig {
        MessageSaverConfig {
            enabled: SERVER_CONFIG.message_saver.enabled,
            enforce_fsync: SERVER_CONFIG.message_saver.enforce_fsync,
            interval: SERVER_CONFIG.message_saver.interval.parse().unwrap(),
        }
    }
}

impl Default for PersonalAccessTokenConfig {
    fn default() -> PersonalAccessTokenConfig {
        PersonalAccessTokenConfig {
            max_tokens_per_user: SERVER_CONFIG.personal_access_token.max_tokens_per_user as u32,
            cleaner: PersonalAccessTokenCleanerConfig::default(),
        }
    }
}

impl Default for PersonalAccessTokenCleanerConfig {
    fn default() -> PersonalAccessTokenCleanerConfig {
        PersonalAccessTokenCleanerConfig {
            enabled: SERVER_CONFIG.personal_access_token.cleaner.enabled,
            interval: SERVER_CONFIG
                .personal_access_token
                .cleaner
                .interval
                .parse()
                .unwrap(),
        }
    }
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            path: SERVER_CONFIG.system.path.parse().unwrap(),
            backup: BackupConfig::default(),
            runtime: RuntimeConfig::default(),
            logging: LoggingConfig::default(),
            stream: StreamConfig::default(),
            encryption: EncryptionConfig::default(),
            topic: TopicConfig::default(),
            partition: PartitionConfig::default(),
            segment: SegmentConfig::default(),
            state: StateConfig::default(),
            compression: CompressionConfig::default(),
            message_deduplication: MessageDeduplicationConfig::default(),
            recovery: RecoveryConfig::default(),
            memory_pool: MemoryPoolConfig::default(),
            sharding: ShardingConfig::default(),
        }
    }
}

impl Default for BackupConfig {
    fn default() -> BackupConfig {
        BackupConfig {
            path: SERVER_CONFIG.system.backup.path.parse().unwrap(),
            compatibility: CompatibilityConfig::default(),
        }
    }
}

impl Default for CompatibilityConfig {
    fn default() -> Self {
        CompatibilityConfig {
            path: SERVER_CONFIG
                .system
                .backup
                .compatibility
                .path
                .parse()
                .unwrap(),
        }
    }
}

impl Default for HeartbeatConfig {
    fn default() -> HeartbeatConfig {
        HeartbeatConfig {
            enabled: SERVER_CONFIG.heartbeat.enabled,
            interval: SERVER_CONFIG.heartbeat.interval.parse().unwrap(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> RuntimeConfig {
        RuntimeConfig {
            path: SERVER_CONFIG.system.runtime.path.parse().unwrap(),
        }
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            allow_override: SERVER_CONFIG.system.compression.allow_override,
            default_algorithm: SERVER_CONFIG
                .system
                .compression
                .default_algorithm
                .parse()
                .unwrap(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> LoggingConfig {
        LoggingConfig {
            path: SERVER_CONFIG.system.logging.path.parse().unwrap(),
            level: SERVER_CONFIG.system.logging.level.parse().unwrap(),
            max_size: SERVER_CONFIG.system.logging.max_size.parse().unwrap(),
            retention: SERVER_CONFIG.system.logging.retention.parse().unwrap(),
            sysinfo_print_interval: SERVER_CONFIG
                .system
                .logging
                .sysinfo_print_interval
                .parse()
                .unwrap(),
        }
    }
}

impl Default for EncryptionConfig {
    fn default() -> EncryptionConfig {
        EncryptionConfig {
            enabled: SERVER_CONFIG.system.encryption.enabled,
            key: SERVER_CONFIG.system.encryption.key.parse().unwrap(),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            path: SERVER_CONFIG.system.stream.path.parse().unwrap(),
        }
    }
}

impl Default for TopicConfig {
    fn default() -> TopicConfig {
        TopicConfig {
            path: SERVER_CONFIG.system.topic.path.parse().unwrap(),
            max_size: SERVER_CONFIG.system.topic.max_size.parse().unwrap(),
            delete_oldest_segments: SERVER_CONFIG.system.topic.delete_oldest_segments,
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        PartitionConfig {
            path: SERVER_CONFIG.system.partition.path.parse().unwrap(),
            size_of_messages_required_to_save: SERVER_CONFIG
                .system
                .partition
                .size_of_messages_required_to_save
                .parse()
                .unwrap(),
            messages_required_to_save: SERVER_CONFIG.system.partition.messages_required_to_save
                as u32,
            enforce_fsync: SERVER_CONFIG.system.partition.enforce_fsync,
            validate_checksum: SERVER_CONFIG.system.partition.validate_checksum,
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> SegmentConfig {
        SegmentConfig {
            size: SERVER_CONFIG.system.segment.size.parse().unwrap(),
            cache_indexes: SERVER_CONFIG.system.segment.cache_indexes.parse().unwrap(),
            message_expiry: SERVER_CONFIG.system.segment.message_expiry.parse().unwrap(),
            archive_expired: SERVER_CONFIG.system.segment.archive_expired,
        }
    }
}

impl Default for StateConfig {
    fn default() -> StateConfig {
        StateConfig {
            enforce_fsync: SERVER_CONFIG.system.state.enforce_fsync,
            max_file_operation_retries: SERVER_CONFIG.system.state.max_file_operation_retries
                as u32,
            retry_delay: SERVER_CONFIG.system.state.retry_delay.parse().unwrap(),
        }
    }
}

impl Default for MessageDeduplicationConfig {
    fn default() -> MessageDeduplicationConfig {
        MessageDeduplicationConfig {
            enabled: SERVER_CONFIG.system.message_deduplication.enabled,
            max_entries: SERVER_CONFIG.system.message_deduplication.max_entries as u64,
            expiry: SERVER_CONFIG
                .system
                .message_deduplication
                .expiry
                .parse()
                .unwrap(),
        }
    }
}

impl Default for RecoveryConfig {
    fn default() -> RecoveryConfig {
        RecoveryConfig {
            recreate_missing_state: SERVER_CONFIG.system.recovery.recreate_missing_state,
        }
    }
}

impl Default for MemoryPoolConfig {
    fn default() -> MemoryPoolConfig {
        MemoryPoolConfig {
            enabled: SERVER_CONFIG.system.memory_pool.enabled,
            size: SERVER_CONFIG.system.memory_pool.size.parse().unwrap(),
            bucket_capacity: SERVER_CONFIG.system.memory_pool.bucket_capacity as u32,
        }
    }
}

impl Default for TelemetryConfig {
    fn default() -> TelemetryConfig {
        TelemetryConfig {
            enabled: SERVER_CONFIG.telemetry.enabled,
            service_name: SERVER_CONFIG.telemetry.service_name.parse().unwrap(),
            logs: TelemetryLogsConfig::default(),
            traces: TelemetryTracesConfig::default(),
        }
    }
}

impl Default for TelemetryLogsConfig {
    fn default() -> TelemetryLogsConfig {
        TelemetryLogsConfig {
            transport: SERVER_CONFIG.telemetry.logs.transport.parse().unwrap(),
            endpoint: SERVER_CONFIG.telemetry.logs.endpoint.parse().unwrap(),
        }
    }
}

impl Default for TelemetryTracesConfig {
    fn default() -> TelemetryTracesConfig {
        TelemetryTracesConfig {
            transport: SERVER_CONFIG.telemetry.traces.transport.parse().unwrap(),
            endpoint: SERVER_CONFIG.telemetry.traces.endpoint.parse().unwrap(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> ClusterConfig {
        ClusterConfig {
            enabled: SERVER_CONFIG.cluster.enabled,
            id: SERVER_CONFIG.cluster.id as u32,
            name: SERVER_CONFIG.cluster.name.parse().unwrap(),
            transport: SERVER_CONFIG.cluster.transport.parse().unwrap(),
            node: NodeConfig::default(),
            nodes: vec![
                ClusterNodeConfig {
                    id: SERVER_CONFIG.cluster.nodes[0].id as u32,
                    name: SERVER_CONFIG.cluster.nodes[0].name.parse().unwrap(),
                    address: SERVER_CONFIG.cluster.nodes[0].address.parse().unwrap(),
                },
                ClusterNodeConfig {
                    id: SERVER_CONFIG.cluster.nodes[1].id as u32,
                    name: SERVER_CONFIG.cluster.nodes[1].name.parse().unwrap(),
                    address: SERVER_CONFIG.cluster.nodes[1].address.parse().unwrap(),
                },
            ],
        }
    }
}

impl Default for NodeConfig {
    fn default() -> NodeConfig {
        NodeConfig {
            id: SERVER_CONFIG.cluster.node.id as u32,
        }
    }
}
