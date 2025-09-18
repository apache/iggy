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

use iggy_connector_sdk::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkSourceConfig {
    /// Flink cluster URL
    pub flink_cluster_url: String,

    /// Source type (kafka, kinesis, rabbitmq, custom)
    pub source_type: SourceType,

    /// Source identifier (topic, stream, queue name, etc.)
    pub source_identifier: String,

    /// Pattern for matching multiple sources (e.g., "events-*")
    pub source_pattern: Option<String>,

    /// Starting position for consumption
    #[serde(default = "default_start_position")]
    pub start_position: StartPosition,

    /// Output schema for messages
    #[serde(default = "default_schema")]
    pub output_schema: Schema,

    /// Batch size for fetching messages
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Poll interval in milliseconds
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,

    /// Enable background fetching
    #[serde(default = "default_background_fetch")]
    pub enable_background_fetch: bool,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub connection_timeout_secs: u64,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Watermark strategy for event time processing
    pub watermark_strategy: Option<WatermarkStrategy>,

    /// Parallelism for the source operator
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,

    /// Authentication configuration
    pub auth: Option<AuthConfig>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// Schema registry configuration for Avro/Protobuf
    pub schema_registry: Option<SchemaRegistryConfig>,

    /// Custom properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Kafka,
    Kinesis,
    RabbitMQ,
    Pulsar,
    FileSystem,
    Socket,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    Earliest,
    Latest,
    Timestamp(u64),
    Offset(u64),
    GroupOffsets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkStrategy {
    pub strategy_type: WatermarkType,
    pub max_out_of_orderness_ms: u64,
    pub idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkType {
    BoundedOutOfOrderness,
    AscendingTimestamps,
    CustomTimestampAssigner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub auth_type: AuthType,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub sasl_mechanism: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    None,
    Basic,
    Bearer,
    SaslPlain,
    SaslScram,
    Certificate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_path: Option<String>,
    pub verify_hostname: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub auth: Option<AuthConfig>,
    pub cache_size: Option<usize>,
}

fn default_start_position() -> StartPosition {
    StartPosition::Latest
}

fn default_schema() -> Schema {
    Schema::Json
}

fn default_batch_size() -> usize {
    100
}

fn default_poll_interval() -> u64 {
    1000
}

fn default_background_fetch() -> bool {
    false
}

fn default_timeout() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    3
}

fn default_parallelism() -> u32 {
    1
}
