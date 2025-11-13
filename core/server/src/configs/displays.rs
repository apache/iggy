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

use crate::configs::quic::{QuicCertificateConfig, QuicConfig};
use crate::configs::server::{
    DataMaintenanceConfig, HeartbeatConfig, MessagesMaintenanceConfig, TelemetryConfig,
    TelemetryLogsConfig, TelemetryTracesConfig,
};
use crate::configs::system::MessageDeduplicationConfig;
use crate::configs::{
    http::{HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig},
    server::{MessageSaverConfig, ServerConfig},
    system::{
        CompressionConfig, EncryptionConfig, LoggingConfig, PartitionConfig, SegmentConfig,
        StateConfig, StreamConfig, SystemConfig, TopicConfig,
    },
    tcp::{TcpConfig, TcpSocketConfig, TcpTlsConfig},
};
use std::fmt::{Display, Formatter};

impl Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_request_size: {}, cors: {}, jwt: {}, metrics: {}, tls: {} }}",
            self.enabled,
            self.address,
            self.max_request_size,
            self.cors,
            self.jwt,
            self.metrics,
            self.tls
        )
    }
}

impl Display for HttpCorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, allowed_methods: {:?}, allowed_origins: {:?}, allowed_headers: {:?}, exposed_headers: {:?}, allow_credentials: {}, allow_private_network: {} }}",
            self.enabled,
            self.allowed_methods,
            self.allowed_origins,
            self.allowed_headers,
            self.exposed_headers,
            self.allow_credentials,
            self.allow_private_network
        )
    }
}

impl Display for HttpJwtConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ algorithm: {}, audience: {}, access_token_expiry: {}, use_base64_secret: {} }}",
            self.algorithm, self.audience, self.access_token_expiry, self.use_base64_secret
        )
    }
}

impl Display for HttpMetricsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, endpoint: {} }}",
            self.enabled, self.endpoint
        )
    }
}

impl Display for HttpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, cert_file: {}, key_file: {} }}",
            self.enabled, self.cert_file, self.key_file
        )
    }
}

impl Display for QuicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_concurrent_bidi_streams: {}, datagram_send_buffer_size: {}, initial_mtu: {}, send_window: {}, receive_window: {}, keep_alive_interval: {}, max_idle_timeout: {}, certificate: {} }}",
            self.enabled,
            self.address,
            self.max_concurrent_bidi_streams,
            self.datagram_send_buffer_size,
            self.initial_mtu,
            self.send_window,
            self.receive_window,
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

impl Display for CompressionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ allowed_override: {}, default_algorithm: {} }}",
            self.allow_override, self.default_algorithm
        )
    }
}

impl Display for DataMaintenanceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ messages: {} }}", self.messages)
    }
}

impl Display for MessagesMaintenanceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ cleaner_enabled: {}, interval: {} }}",
            self.cleaner_enabled, self.interval
        )
    }
}

impl Display for ServerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ data_maintenance: {}, message_saver: {}, heartbeat: {}, system: {}, quic: {}, tcp: {}, http: {}, telemetry: {} }}",
            self.data_maintenance,
            self.message_saver,
            self.heartbeat,
            self.system,
            self.quic,
            self.tcp,
            self.http,
            self.telemetry
        )
    }
}

impl Display for MessageSaverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, enforce_fsync: {}, interval: {} }}",
            self.enabled, self.enforce_fsync, self.interval
        )
    }
}

impl Display for HeartbeatConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, interval: {} }}",
            self.enabled, self.interval
        )
    }
}

impl Display for EncryptionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ enabled: {} }}", self.enabled)
    }
}

impl Display for StreamConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for TopicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, max_size: {}, delete_oldest_segments: {} }}",
            self.path, self.max_size, self.delete_oldest_segments
        )
    }
}

impl Display for PartitionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, messages_required_to_save: {}, size_of_messages_required_to_save: {}, enforce_fsync: {}, validate_checksum: {} }}",
            self.path,
            self.messages_required_to_save,
            self.size_of_messages_required_to_save,
            self.enforce_fsync,
            self.validate_checksum
        )
    }
}

impl Display for MessageDeduplicationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, max_entries: {:?}, expiry: {:?} }}",
            self.enabled, self.max_entries, self.expiry
        )
    }
}

impl Display for SegmentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ size_bytes: {}, cache_indexes: {}, message_expiry: {}, archive_expired: {} }}",
            self.size, self.cache_indexes, self.message_expiry, self.archive_expired,
        )
    }
}

impl Display for LoggingConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, level: {}, max_size: {}, retention: {} }}",
            self.path,
            self.level,
            self.max_size.as_human_string_with_zero_as_unlimited(),
            self.retention
        )
    }
}

impl Display for TcpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, ipv6: {}, tls: {}, socket: {} }}",
            self.enabled, self.address, self.ipv6, self.tls, self.socket,
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

impl Display for TcpSocketConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ override defaults: {}, recv buffer size: {}, send buffer size {}, keepalive: {}, nodelay: {}, linger: {} }}",
            self.override_defaults,
            self.recv_buffer_size,
            self.send_buffer_size,
            self.keepalive,
            self.nodelay,
            self.linger,
        )
    }
}

impl Display for TelemetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, service_name: {}, logs: {}, traces: {} }}",
            self.enabled, self.service_name, self.logs, self.traces
        )
    }
}

impl Display for TelemetryLogsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

impl Display for StateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enforce_fsync: {}, max_file_operation_retries: {}, retry_delay: {} }}",
            self.enforce_fsync, self.max_file_operation_retries, self.retry_delay,
        )
    }
}

impl Display for TelemetryTracesConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

impl Display for SystemConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, logging: {}, stream: {}, topic: {}, partition: {}, segment: {}, encryption: {}, state: {} }}",
            self.path,
            self.logging,
            self.stream,
            self.topic,
            self.partition,
            self.segment,
            self.encryption,
            self.state,
        )
    }
}
