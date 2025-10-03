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

use crate::utils::{byte_size::IggyByteSize, duration::IggyDuration, timestamp::IggyTimestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// `Stats` represents the statistics and details of the server and running process.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    /// The unique identifier of the process.
    pub process_id: u32,
    /// The CPU usage of the process.
    pub cpu_usage: f32,
    /// the total CPU usage of the system.
    pub total_cpu_usage: f32,
    /// The memory usage of the process.
    pub memory_usage: IggyByteSize,
    /// The total memory of the system.
    pub total_memory: IggyByteSize,
    /// The available memory of the system.
    pub available_memory: IggyByteSize,
    /// The run time of the process.
    pub run_time: IggyDuration,
    /// The start time of the process.
    pub start_time: IggyTimestamp,
    /// The total number of bytes read.
    pub read_bytes: IggyByteSize,
    /// The total number of bytes written.
    pub written_bytes: IggyByteSize,
    /// The total size of the messages in bytes.
    pub messages_size_bytes: IggyByteSize,
    /// The total number of streams.
    pub streams_count: u32,
    /// The total number of topics.
    pub topics_count: u32,
    /// The total number of partitions.
    pub partitions_count: u32,
    /// The total number of segments.
    pub segments_count: u32,
    /// The total number of messages.
    pub messages_count: u64,
    /// The total number of connected clients.
    pub clients_count: u32,
    /// The total number of consumer groups.
    pub consumer_groups_count: u32,
    /// The name of the host.
    pub hostname: String,
    /// The details of the operating system.
    pub os_name: String,
    /// The version of the operating system.
    pub os_version: String,
    /// The version of the kernel.
    pub kernel_version: String,
    /// The version of the Iggy server.
    pub iggy_server_version: String,
    /// The semantic version of the Iggy server in the numeric format e.g. 1.2.3 -> 100200300 (major * 1000000 + minor * 1000 + patch).
    pub iggy_server_semver: Option<u32>,
    /// Cache metrics per partition
    #[serde(with = "cache_metrics_serializer")]
    pub cache_metrics: HashMap<CacheMetricsKey, CacheMetrics>,
}

/// Key for identifying a specific partition's cache metrics
#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct CacheMetricsKey {
    /// Stream ID
    pub stream_id: u32,
    /// Topic ID
    pub topic_id: u32,
    /// Partition ID
    pub partition_id: u32,
}

impl CacheMetricsKey {
    pub fn to_string_key(&self) -> String {
        format!("{}-{}-{}", self.stream_id, self.topic_id, self.partition_id)
    }
}

/// Cache metrics for a specific partition
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CacheMetrics {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Hit ratio (hits / (hits + misses))
    pub hit_ratio: f32,
}

mod cache_metrics_serializer {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S>(
        metrics: &HashMap<CacheMetricsKey, CacheMetrics>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let string_map: HashMap<String, &CacheMetrics> = metrics
            .iter()
            .map(|(k, v)| (k.to_string_key(), v))
            .collect();
        string_map.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<CacheMetricsKey, CacheMetrics>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string_map: HashMap<String, CacheMetrics> = HashMap::deserialize(deserializer)?;
        let mut result = HashMap::new();
        for (key_str, value) in string_map {
            let parts: Vec<&str> = key_str.split('-').collect();
            if parts.len() != 3 {
                continue;
            }
            if let (Ok(stream_id), Ok(topic_id), Ok(partition_id)) = (
                parts[0].parse::<u32>(),
                parts[1].parse::<u32>(),
                parts[2].parse::<u32>(),
            ) {
                let key = CacheMetricsKey {
                    stream_id,
                    topic_id,
                    partition_id,
                };
                result.insert(key, value);
            }
        }
        Ok(result)
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            process_id: 0,
            cpu_usage: 0.0,
            total_cpu_usage: 0.0,
            memory_usage: 0_u64.into(),
            total_memory: 0_u64.into(),
            available_memory: 0_u64.into(),
            run_time: 0.into(),
            start_time: 0.into(),
            read_bytes: 0_u64.into(),
            written_bytes: 0_u64.into(),
            messages_size_bytes: 0_u64.into(),
            streams_count: 0,
            topics_count: 0,
            partitions_count: 0,
            segments_count: 0,
            messages_count: 0,
            clients_count: 0,
            consumer_groups_count: 0,
            hostname: "unknown_hostname".to_string(),
            os_name: "unknown_os_name".to_string(),
            os_version: "unknown_os_version".to_string(),
            kernel_version: "unknown_kernel_version".to_string(),
            iggy_server_version: "unknown_iggy_version".to_string(),
            iggy_server_semver: None,
            cache_metrics: HashMap::new(),
        }
    }
}
