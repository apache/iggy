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

use super::cache_indexes::CacheIndexesConfig;
use iggy_common::IggyByteSize;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::{CompressionAlgorithm, IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemConfig {
    pub path: String,
    pub backup: BackupConfig,
    pub state: StateConfig,
    pub runtime: RuntimeConfig,
    pub logging: LoggingConfig,
    pub stream: StreamConfig,
    pub topic: TopicConfig,
    pub partition: PartitionConfig,
    pub segment: SegmentConfig,
    pub encryption: EncryptionConfig,
    pub compression: CompressionConfig,
    pub message_deduplication: MessageDeduplicationConfig,
    pub recovery: RecoveryConfig,
    pub memory_pool: MemoryPoolConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BackupConfig {
    pub path: String,
    pub compatibility: CompatibilityConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompatibilityConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompressionConfig {
    pub allow_override: bool,
    pub default_algorithm: CompressionAlgorithm,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub path: String,
    pub level: String,
    pub max_size: IggyByteSize,
    #[serde_as(as = "DisplayFromStr")]
    pub retention: IggyDuration,
    #[serde_as(as = "DisplayFromStr")]
    pub sysinfo_print_interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub key: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub path: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub path: String,
    #[serde_as(as = "DisplayFromStr")]
    pub max_size: MaxTopicSize,
    pub delete_oldest_segments: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionConfig {
    pub path: String,
    pub messages_required_to_save: u32,
    pub size_of_messages_required_to_save: IggyByteSize,
    pub enforce_fsync: bool,
    pub validate_checksum: bool,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct MessageDeduplicationConfig {
    pub enabled: bool,
    pub max_entries: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub expiry: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RecoveryConfig {
    pub recreate_missing_state: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MemoryPoolConfig {
    pub enabled: bool,
    pub size: IggyByteSize,
    pub bucket_capacity: u32,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub size: IggyByteSize,
    pub cache_indexes: CacheIndexesConfig,
    #[serde_as(as = "DisplayFromStr")]
    pub message_expiry: IggyExpiry,
    pub archive_expired: bool,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct StateConfig {
    pub enforce_fsync: bool,
    pub max_file_operation_retries: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub retry_delay: IggyDuration,
}

impl SystemConfig {
    pub fn get_system_path(&self) -> String {
        self.path.to_string()
    }

    pub fn get_state_path(&self) -> String {
        format!("{}/state", self.get_system_path())
    }

    pub fn get_state_messages_file_path(&self) -> String {
        format!("{}/log", self.get_state_path())
    }

    pub fn get_state_info_path(&self) -> String {
        format!("{}/info", self.get_state_path())
    }
    pub fn get_state_tokens_path(&self) -> String {
        format!("{}/tokens", self.get_state_path())
    }

    pub fn get_backup_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.backup.path)
    }

    pub fn get_compatibility_backup_path(&self) -> String {
        format!(
            "{}/{}",
            self.get_backup_path(),
            self.backup.compatibility.path
        )
    }

    pub fn get_runtime_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.runtime.path)
    }

    pub fn get_streams_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.stream.path)
    }

    pub fn get_stream_path(&self, stream_id: u32) -> String {
        format!("{}/{}", self.get_streams_path(), stream_id)
    }

    pub fn get_topics_path(&self, stream_id: u32) -> String {
        format!("{}/{}", self.get_stream_path(stream_id), self.topic.path)
    }

    pub fn get_topic_path(&self, stream_id: u32, topic_id: u32) -> String {
        format!("{}/{}", self.get_topics_path(stream_id), topic_id)
    }

    pub fn get_partitions_path(&self, stream_id: u32, topic_id: u32) -> String {
        format!(
            "{}/{}",
            self.get_topic_path(stream_id, topic_id),
            self.partition.path
        )
    }

    pub fn get_partition_path(&self, stream_id: u32, topic_id: u32, partition_id: u32) -> String {
        format!(
            "{}/{}",
            self.get_partitions_path(stream_id, topic_id),
            partition_id
        )
    }

    pub fn get_offsets_path(&self, stream_id: u32, topic_id: u32, partition_id: u32) -> String {
        format!(
            "{}/offsets",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_offsets_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> String {
        format!(
            "{}/consumers",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_group_offsets_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> String {
        format!(
            "{}/groups",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_segment_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
    ) -> String {
        format!(
            "{}/{:0>20}",
            self.get_partition_path(stream_id, topic_id, partition_id),
            start_offset
        )
    }
}
