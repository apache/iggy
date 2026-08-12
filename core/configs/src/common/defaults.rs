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

use super::http::{HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig};
use super::server::{
    ConsumerGroupConfig, HeartbeatConfig, MemoryPoolConfig, MessageSaverConfig,
    MessagesMaintenanceConfig, PersonalAccessTokenCleanerConfig, PersonalAccessTokenConfig,
    TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig,
};
use super::system::{
    BackupConfig, CompatibilityConfig, CompressionConfig, EncryptionConfig, LoggingConfig,
    MessageDeduplicationConfig, PartitionConfig, RecoveryConfig, RuntimeConfig, SegmentConfig,
    StateConfig, StreamConfig, SystemConfig, TopicConfig,
};
use configs::ConfigEnvMappings;

static_toml::static_toml! {
    // static_toml resolves relative to CARGO_MANIFEST_DIR (core/configs/).
    pub static SERVER_CONFIG = include_toml!("../server/config.toml");
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

impl Default for HttpConfig {
    fn default() -> HttpConfig {
        HttpConfig {
            enabled: SERVER_CONFIG.http.enabled,
            address: SERVER_CONFIG.http.address.parse().unwrap(),
            max_request_size: SERVER_CONFIG.http.max_request_size.parse().unwrap(),
            web_ui: SERVER_CONFIG.http.web_ui,
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
            trusted_issuers: None,
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

impl<S: ConfigEnvMappings + Default> Default for SystemConfig<S> {
    fn default() -> Self {
        Self {
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
            sharding: S::default(),
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

impl Default for ConsumerGroupConfig {
    fn default() -> ConsumerGroupConfig {
        ConsumerGroupConfig {
            rebalancing_timeout: SERVER_CONFIG
                .consumer_group
                .rebalancing_timeout
                .parse()
                .unwrap(),
            rebalancing_check_interval: SERVER_CONFIG
                .consumer_group
                .rebalancing_check_interval
                .parse()
                .unwrap(),
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
            file_enabled: SERVER_CONFIG.system.logging.file_enabled,
            max_file_size: SERVER_CONFIG.system.logging.max_file_size.parse().unwrap(),
            max_total_size: SERVER_CONFIG.system.logging.max_total_size.parse().unwrap(),
            rotation_check_interval: SERVER_CONFIG
                .system
                .logging
                .rotation_check_interval
                .parse()
                .unwrap(),
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
            message_expiry: SERVER_CONFIG.system.topic.message_expiry.parse().unwrap(),
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
            preallocate: SERVER_CONFIG.system.segment.preallocate,
            cache_indexes: SERVER_CONFIG.system.segment.cache_indexes.parse().unwrap(),
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
        Self {
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
