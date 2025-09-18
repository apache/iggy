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

use iggy_connector_sdk::{transforms::TransformType, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkSinkConfig {
    /// Flink cluster URL (e.g., "http://localhost:8081")
    pub flink_cluster_url: String,

    /// Name of the Flink job
    pub job_name: String,

    /// Optional specific job ID to send data to
    pub job_id: Option<String>,

    /// Optional JAR path for job submission
    pub jar_path: Option<String>,

    /// Entry class for JAR submission
    pub entry_class: Option<String>,

    /// Program arguments for JAR submission
    pub program_args: Option<String>,

    /// Parallelism for the sink operator
    #[serde(default = "default_parallelism")]
    pub parallelism: Option<u32>,

    /// Batch size before sending to Flink
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Auto-flush interval in milliseconds (0 to disable)
    #[serde(default = "default_flush_interval")]
    pub auto_flush_interval_ms: u64,

    /// Enable Flink checkpointing
    #[serde(default = "default_checkpointing")]
    pub enable_checkpointing: bool,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_secs: u64,

    /// Sink type (kafka, jdbc, elasticsearch, etc.)
    pub sink_type: SinkType,

    /// Target topic/table/index based on sink type
    pub target: String,

    /// Skip errors instead of failing the entire batch
    #[serde(default)]
    pub skip_errors: bool,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub connection_timeout_secs: u64,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Retry delay in milliseconds
    #[serde(default = "default_retry_delay")]
    pub retry_delay_ms: u64,

    /// Enable exactly-once semantics
    #[serde(default)]
    pub exactly_once: bool,

    /// Optional schema registry URL for Avro/Protobuf
    pub schema_registry_url: Option<String>,

    /// Authentication configuration
    pub auth: Option<AuthConfig>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// Custom properties for specific sink types
    #[serde(default)]
    pub properties: HashMap<String, String>,

    /// Transforms to apply to messages before sending to Flink
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,

    /// Input schema format (for decoding if different from native)
    pub input_schema: Option<Schema>,

    /// Output schema format (for encoding before sending to Flink)
    pub output_schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkType {
    Kafka,
    Jdbc,
    Elasticsearch,
    Cassandra,
    Redis,
    Hbase,
    MongoDB,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub auth_type: AuthType,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    None,
    Basic,
    Bearer,
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
pub struct TransformConfig {
    pub r#type: TransformType,
    pub config: serde_json::Value,
}

fn default_parallelism() -> Option<u32> {
    Some(4)
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval() -> u64 {
    5000
}

fn default_checkpointing() -> bool {
    true
}

fn default_checkpoint_interval() -> u64 {
    60
}

fn default_timeout() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_delay() -> u64 {
    1000
}
