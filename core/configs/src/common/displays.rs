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

use super::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MessagesMaintenanceConfig,
    TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig,
};
use super::{
    http::{HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig},
    system::{
        EncryptionConfig, LoggingConfig, PartitionConfig, SegmentConfig, StreamConfig,
        SystemConfig, TopicConfig,
    },
};
use configs::ConfigEnvMappings;
use std::fmt::{Display, Formatter};

impl Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_request_size: {}, web_ui: {}, cors: {}, jwt: {}, metrics: {}, tls: {} }}",
            self.enabled,
            self.address,
            self.max_request_size,
            self.web_ui,
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

impl Display for ConsumerGroupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ rebalancing_timeout: {} }}", self.rebalancing_timeout)
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
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for PartitionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, validate_checksum: {} }}",
            self.path, self.validate_checksum
        )
    }
}

impl Display for SegmentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ archive_expired: {} }}", self.archive_expired,)
    }
}

impl Display for LoggingConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, level: {}, file_enabled: {}, max_file_size: {}, max_total_size: {}, rotation_check_interval: {}, retention: {} }}",
            self.path,
            self.level,
            self.file_enabled,
            self.max_file_size.as_human_string_with_zero_as_unlimited(),
            self.max_total_size.as_human_string_with_zero_as_unlimited(),
            self.rotation_check_interval,
            self.retention
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

impl Display for TelemetryTracesConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

impl<S: ConfigEnvMappings> Display for SystemConfig<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, logging: {}, stream: {}, topic: {}, partition: {}, segment: {}, encryption: {} }}",
            self.path,
            self.logging,
            self.stream,
            self.topic,
            self.partition,
            self.segment,
            self.encryption,
        )
    }
}
