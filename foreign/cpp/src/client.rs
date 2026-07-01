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

use crate::{RUNTIME, ffi};
use iggy::prelude::{
    Client as IggyConnectionClient, CompressionAlgorithm as RustCompressionAlgorithm, Consumer,
    ConsumerGroupClient, Identifier as RustIdentifier, IggyClient as RustIggyClient,
    IggyClientBuilder as RustIggyClientBuilder, IggyError, IggyExpiry as RustIggyExpiry,
    IggyMessage, IggyTimestamp, MaxTopicSize as RustMaxTopicSize, MessageClient, PartitionClient,
    Partitioning, PollingStrategy, SnapshotCompression as RustSnapshotCompression, StreamClient,
    SystemClient as RustSystemClient, SystemSnapshotType as RustSystemSnapshotType, TopicClient,
    UserClient,
};
use iggy_common::{
    CacheMetrics as RustCacheMetrics, CacheMetricsKey as RustCacheMetricsKey,
    ClientInfo as RustClientInfo, ClientInfoDetails as RustClientInfoDetails, Stats as RustStats,
};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

/// Sentinel value passed from C++ to mean "no partition specified" — the server picks the
/// partition based on the consumer/strategy. Cxx FFI does not support `Option<u32>`, so we
/// reserve `u32::MAX` as the sentinel for `partition_id`.
const ANY_PARTITION_ID: u32 = u32::MAX;
impl From<RustClientInfo> for ffi::ClientInfo {
    fn from(client: RustClientInfo) -> Self {
        let has_user_id = client.user_id.is_some();
        ffi::ClientInfo {
            client_id: client.client_id,
            has_user_id,
            user_id: client.user_id.unwrap_or(u32::MAX),
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
        }
    }
}

impl From<RustClientInfoDetails> for ffi::ClientInfoDetails {
    fn from(client: RustClientInfoDetails) -> Self {
        let has_user_id = client.user_id.is_some();
        ffi::ClientInfoDetails {
            client_id: client.client_id,
            has_user_id,
            user_id: client.user_id.unwrap_or(u32::MAX),
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups: client
                .consumer_groups
                .into_iter()
                .map(ffi::ConsumerGroupInfo::from)
                .collect(),
        }
    }
}

impl TryFrom<Option<RustClientInfoDetails>> for ffi::ClientInfoDetails {
    type Error = String;

    fn try_from(client: Option<RustClientInfoDetails>) -> Result<Self, Self::Error> {
        match client {
            Some(client) => Ok(ffi::ClientInfoDetails::from(client)),
            None => Err("client not found".to_string()),
        }
    }
}

impl From<(RustCacheMetricsKey, RustCacheMetrics)> for ffi::CacheMetricEntry {
    fn from((key, metrics): (RustCacheMetricsKey, RustCacheMetrics)) -> Self {
        ffi::CacheMetricEntry {
            stream_id: key.stream_id,
            topic_id: key.topic_id,
            partition_id: key.partition_id,
            hits: metrics.hits,
            misses: metrics.misses,
            hit_ratio: metrics.hit_ratio,
        }
    }
}

impl From<RustStats> for ffi::Stats {
    fn from(stats: RustStats) -> Self {
        let has_server_semver = stats.iggy_server_semver.is_some();
        ffi::Stats {
            process_id: stats.process_id,
            cpu_usage: stats.cpu_usage,
            total_cpu_usage: stats.total_cpu_usage,
            memory_usage: stats.memory_usage.as_bytes_u64(),
            total_memory: stats.total_memory.as_bytes_u64(),
            available_memory: stats.available_memory.as_bytes_u64(),
            run_time_micros: stats.run_time.as_micros(),
            start_time_epoch_micros: stats.start_time.as_micros(),
            read_bytes: stats.read_bytes.as_bytes_u64(),
            written_bytes: stats.written_bytes.as_bytes_u64(),
            messages_size_bytes: stats.messages_size_bytes.as_bytes_u64(),
            streams_count: stats.streams_count,
            topics_count: stats.topics_count,
            partitions_count: stats.partitions_count,
            segments_count: stats.segments_count,
            messages_count: stats.messages_count,
            clients_count: stats.clients_count,
            consumer_groups_count: stats.consumer_groups_count,
            hostname: stats.hostname,
            os_name: stats.os_name,
            os_version: stats.os_version,
            kernel_version: stats.kernel_version,
            iggy_server_version: stats.iggy_server_version,
            has_server_semver,
            iggy_server_semver: stats.iggy_server_semver.unwrap_or(0),
            cache_metrics: stats
                .cache_metrics
                .into_iter()
                .map(ffi::CacheMetricEntry::from)
                .collect(),
            threads_count: stats.threads_count,
            free_disk_space: stats.free_disk_space.as_bytes_u64(),
            total_disk_space: stats.total_disk_space.as_bytes_u64(),
        }
    }
}

pub struct Client {
    pub inner: Arc<RustIggyClient>,
}

/// Creates a new client connection and returns a raw pointer to the underlying [`Client`].
///
/// # Ownership
///
/// The returned `*mut Client` is owned by the caller (the C++ side). The caller is responsible
/// for calling [`delete_connection`] exactly once to release the resources. Failing to do so
/// leaks the underlying tokio runtime resources and the open network connection.
///
/// # Safety
///
/// - Passing the pointer to [`delete_connection`] more than once is undefined behaviour
///   (double-free).
/// - Using the pointer after [`delete_connection`] has been called is undefined behaviour
///   (use-after-free).
/// - This function does not provide synchronisation. The pointer must not be used concurrently
///   from multiple threads unless the caller serialises access externally.
pub fn new_connection(connection_string: String) -> Result<*mut Client, String> {
    let connection_str = connection_string.as_str();
    let client = match connection_str {
        "" => RustIggyClientBuilder::new()
            .with_tcp()
            .build()
            .map_err(|error| format!("Could not build default connection: {error}"))?,
        s if s.starts_with("iggy://") || s.starts_with("iggy+") => {
            RustIggyClient::from_connection_string(s)
                .map_err(|error| format!("Could not parse connection string '{s}': {error}"))?
        }
        s => RustIggyClientBuilder::new()
            .with_tcp()
            .with_server_address(connection_string.clone())
            .build()
            .map_err(|error| format!("Could not build connection for address '{s}': {error}"))?,
    };

    Ok(Box::into_raw(Box::new(Client {
        inner: Arc::new(client),
    })))
}

impl Client {
    pub fn login_user(&self, username: String, password: String) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .login_user(&username, &password)
                .await
                .map_err(|error| format!("Could not login user '{username}': {error}"))?;
            Ok(())
        })
    }

    pub fn connect(&self) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .connect()
                .await
                .map_err(|error| format!("Could not connect: {error}"))?;
            Ok(())
        })
    }

    pub fn get_streams(&self) -> Result<Vec<ffi::Stream>, String> {
        RUNTIME.block_on(async {
            let streams = self
                .inner
                .get_streams()
                .await
                .map_err(|error| format!("Could not get streams: {error}"))?;
            Ok(streams.into_iter().map(ffi::Stream::from).collect())
        })
    }

    pub fn create_stream(&self, stream_name: String) -> Result<ffi::StreamDetails, String> {
        RUNTIME.block_on(async {
            let stream_details = self
                .inner
                .create_stream(&stream_name)
                .await
                .map_err(|error| format!("Could not create stream '{stream_name}': {error}"))?;
            Ok(ffi::StreamDetails::from(stream_details))
        })
    }

    pub fn get_stream(&self, stream_id: ffi::Identifier) -> Result<ffi::StreamDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not get stream: {error}"))?;

        RUNTIME.block_on(async {
            let stream_details = self
                .inner
                .get_stream(&rust_stream_id)
                .await
                .map_err(|error| format!("Could not get stream '{rust_stream_id}': {error}"))?;
            let stream_details =
                stream_details.ok_or_else(|| format!("Stream '{rust_stream_id}' was not found"))?;
            Ok(ffi::StreamDetails::from(stream_details))
        })
    }

    pub fn delete_stream(&self, stream_id: ffi::Identifier) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not delete stream: {error}"))?;

        RUNTIME.block_on(async {
            self.inner
                .delete_stream(&rust_stream_id)
                .await
                .map_err(|error| format!("Could not delete stream '{rust_stream_id}': {error}"))?;
            Ok(())
        })
    }

    pub fn purge_stream(&self, stream_id: ffi::Identifier) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not purge stream: {error}"))?;

        RUNTIME.block_on(async {
            self.inner
                .purge_stream(&rust_stream_id)
                .await
                .map_err(|error| format!("Could not purge stream '{rust_stream_id}': {error}"))?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn send_messages(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partitioning_kind: String,
        partitioning_value: Vec<u8>,
        messages: Vec<ffi::IggyMessageToSend>,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not send messages: {error}"))?;
        let rust_topic_id = RustIdentifier::try_from(topic_id)
            .map_err(|error| format!("Could not send messages: {error}"))?;

        let partitioning = match partitioning_kind.as_str() {
            "balanced" => Partitioning::balanced(),
            "partition_id" => {
                if partitioning_value.len() != 4 {
                    return Err(format!(
                        "Could not send messages: partition_id requires exactly 4 bytes, got {}",
                        partitioning_value.len()
                    ));
                }
                let id =
                    u32::from_le_bytes(partitioning_value.as_slice().try_into().map_err(|_| {
                        "Could not send messages: invalid partition_id value".to_string()
                    })?);
                Partitioning::partition_id(id)
            }
            "messages_key" => {
                if partitioning_value.is_empty() {
                    return Err(
                        "Could not send messages: messages_key requires a non-empty value"
                            .to_string(),
                    );
                }
                Partitioning::messages_key(&partitioning_value).map_err(|error| {
                    format!("Could not send messages: invalid messages key: {error}")
                })?
            }
            _ => {
                return Err(format!(
                    "Could not send messages: invalid partitioning kind: {partitioning_kind}"
                ));
            }
        };

        let mut iggy_messages: Vec<IggyMessage> = messages
            .into_iter()
            .map(IggyMessage::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        RUNTIME.block_on(async {
            self.inner
                .send_messages(
                    &rust_stream_id,
                    &rust_topic_id,
                    &partitioning,
                    &mut iggy_messages,
                )
                .await
                .map_err(|error| format!("Could not send messages: {error}"))?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn poll_messages(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partition_id: u32,
        consumer_kind: String,
        consumer_id: ffi::Identifier,
        polling_strategy_kind: String,
        polling_strategy_value: u64,
        count: u32,
        auto_commit: bool,
    ) -> Result<ffi::PolledMessages, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not poll messages: {error}"))?;
        let rust_topic_id = RustIdentifier::try_from(topic_id)
            .map_err(|error| format!("Could not poll messages: {error}"))?;
        let rust_consumer_id = RustIdentifier::try_from(consumer_id)
            .map_err(|error| format!("Could not poll messages: {error}"))?;

        let consumer = match consumer_kind.as_str() {
            "consumer" => Consumer::new(rust_consumer_id),
            "consumer_group" => Consumer::group(rust_consumer_id),
            _ => {
                return Err(format!(
                    "Could not poll messages: invalid consumer kind: {consumer_kind}"
                ));
            }
        };

        let strategy = match polling_strategy_kind.as_str() {
            "offset" => PollingStrategy::offset(polling_strategy_value),
            "timestamp" => PollingStrategy::timestamp(IggyTimestamp::from(polling_strategy_value)),
            "first" => PollingStrategy::first(),
            "last" => PollingStrategy::last(),
            "next" => PollingStrategy::next(),
            _ => {
                return Err(format!(
                    "Could not poll messages: invalid polling strategy: {polling_strategy_kind}"
                ));
            }
        };

        let opt_partition = if partition_id == ANY_PARTITION_ID {
            None
        } else {
            Some(partition_id)
        };

        RUNTIME.block_on(async {
            let polled = self
                .inner
                .poll_messages(
                    &rust_stream_id,
                    &rust_topic_id,
                    opt_partition,
                    &consumer,
                    &strategy,
                    count,
                    auto_commit,
                )
                .await
                .map_err(|error| format!("Could not poll messages: {error}"))?;
            Ok(ffi::PolledMessages::from(polled))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_name: String,
        partitions_count: u32,
        compression_algorithm: String,
        replication_factor: u8,
        message_expiry_kind: String,
        message_expiry_value: u64,
        max_topic_size: String,
    ) -> Result<ffi::TopicDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not create topic '{topic_name}': {error}"))?;
        let rust_compression_algorithm = match compression_algorithm.to_lowercase().as_str() {
            "" | "none" => RustCompressionAlgorithm::None,
            _ => RustCompressionAlgorithm::from_str(&compression_algorithm).map_err(|error| {
                format!(
                    "Could not create topic '{topic_name}': invalid compression algorithm '{compression_algorithm}': {error}"
                )
            })?,
        };
        let rust_replication_factor = Some(replication_factor.max(1));
        let rust_message_expiry = match message_expiry_kind.as_str() {
            "" | "server_default" | "default" => RustIggyExpiry::ServerDefault,
            "never_expire" => RustIggyExpiry::NeverExpire,
            "duration" => RustIggyExpiry::ExpireDuration(iggy::prelude::IggyDuration::from(
                message_expiry_value,
            )),
            _ => {
                return Err(format!(
                    "Could not create topic '{topic_name}': invalid message expiry kind '{message_expiry_kind}'"
                ));
            }
        };
        let rust_max_topic_size = match max_topic_size.as_str() {
            "" | "server_default" | "0" => RustMaxTopicSize::ServerDefault,
            _ => RustMaxTopicSize::from_str(&max_topic_size).map_err(|error| {
                format!(
                    "Could not create topic '{topic_name}': invalid max topic size '{max_topic_size}': {error}"
                )
            })?,
        };

        RUNTIME.block_on(async {
            let topic_details = self
                .inner
                .create_topic(
                    &rust_stream_id,
                    &topic_name,
                    partitions_count,
                    rust_compression_algorithm,
                    rust_replication_factor,
                    rust_message_expiry,
                    rust_max_topic_size,
                )
                .await
                .map_err(|error| {
                    format!(
                        "Could not create topic '{topic_name}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(ffi::TopicDetails::from(topic_details))
        })
    }

    pub fn get_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
    ) -> Result<ffi::TopicDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not get topic: invalid stream identifier: {error}"))?;
        let rust_topic_id = RustIdentifier::try_from(topic_id)
            .map_err(|error| format!("Could not get topic: invalid topic identifier: {error}"))?;

        RUNTIME.block_on(async {
            let topic_details = self
                .inner
                .get_topic(&rust_stream_id, &rust_topic_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not get topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            let topic_details = topic_details.ok_or_else(|| {
                format!(
                    "Topic '{rust_topic_id}' was not found on stream '{rust_stream_id}'"
                )
            })?;
            Ok(ffi::TopicDetails::from(topic_details))
        })
    }

    pub fn get_topics(&self, stream_id: ffi::Identifier) -> Result<Vec<ffi::Topic>, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not get topics: invalid stream identifier: {error}"))?;

        RUNTIME.block_on(async {
            let topics = self
                .inner
                .get_topics(&rust_stream_id)
                .await
                .map_err(|error| {
                    format!("Could not get topics on stream '{rust_stream_id}': {error}")
                })?;
            Ok(topics.into_iter().map(ffi::Topic::from).collect())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        topic_name: String,
        compression_algorithm: String,
        replication_factor: u8,
        message_expiry_kind: String,
        message_expiry_value: u64,
        max_topic_size: String,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not update topic '{topic_name}': {error}"))?;
        let rust_topic_id = RustIdentifier::try_from(topic_id)
            .map_err(|error| format!("Could not update topic '{topic_name}': {error}"))?;
        let rust_compression_algorithm = match compression_algorithm.to_lowercase().as_str() {
            "" | "none" => RustCompressionAlgorithm::None,
            _ => RustCompressionAlgorithm::from_str(&compression_algorithm).map_err(|error| {
                format!(
                    "Could not update topic '{topic_name}': invalid compression algorithm '{compression_algorithm}': {error}"
                )
            })?,
        };
        let rust_replication_factor = Some(replication_factor.max(1));
        let rust_message_expiry = match message_expiry_kind.as_str() {
            "" | "server_default" | "default" => RustIggyExpiry::ServerDefault,
            "never_expire" => RustIggyExpiry::NeverExpire,
            "duration" => RustIggyExpiry::ExpireDuration(iggy::prelude::IggyDuration::from(
                message_expiry_value,
            )),
            _ => {
                return Err(format!(
                    "Could not update topic '{topic_name}': invalid message expiry kind '{message_expiry_kind}'"
                ));
            }
        };
        let rust_max_topic_size = match max_topic_size.as_str() {
            "" | "server_default" | "0" => RustMaxTopicSize::ServerDefault,
            _ => RustMaxTopicSize::from_str(&max_topic_size).map_err(|error| {
                format!(
                    "Could not update topic '{topic_name}': invalid max topic size '{max_topic_size}': {error}"
                )
            })?,
        };

        RUNTIME.block_on(async {
            self.inner
                .update_topic(
                    &rust_stream_id,
                    &rust_topic_id,
                    &topic_name,
                    rust_compression_algorithm,
                    rust_replication_factor,
                    rust_message_expiry,
                    rust_max_topic_size,
                )
                .await
                .map_err(|error| {
                    format!(
                        "Could not update topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn delete_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not delete topic: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not delete topic: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .delete_topic(&rust_stream_id, &rust_topic_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not delete topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn purge_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not purge topic: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id)
            .map_err(|error| format!("Could not purge topic: invalid topic identifier: {error}"))?;

        RUNTIME.block_on(async {
            self.inner
                .purge_topic(&rust_stream_id, &rust_topic_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not purge topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn create_partitions(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partitions_count: u32,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not create partitions: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not create partitions: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .create_partitions(&rust_stream_id, &rust_topic_id, partitions_count)
                .await
                .map_err(|error| {
                    format!(
                        "Could not create {partitions_count} partitions for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn delete_partitions(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partitions_count: u32,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not delete partitions: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not delete partitions: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .delete_partitions(&rust_stream_id, &rust_topic_id, partitions_count)
                .await
                .map_err(|error| {
                    format!(
                        "Could not delete {partitions_count} partitions for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn create_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        name: String,
    ) -> Result<ffi::ConsumerGroupDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not create consumer group '{name}': invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not create consumer group '{name}': invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            let group = self
                .inner
                .create_consumer_group(&rust_stream_id, &rust_topic_id, &name)
                .await
                .map_err(|error| {
                    format!(
                        "Could not create consumer group '{name}' for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(ffi::ConsumerGroupDetails::from(group))
        })
    }

    pub fn get_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<ffi::ConsumerGroupDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not get consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not get consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not get consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            let group = self
                .inner
                .get_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not get consumer group '{rust_group_id}' for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            let group = group.ok_or_else(|| {
                format!(
                    "Consumer group '{rust_group_id}' was not found for topic '{rust_topic_id}' on stream '{rust_stream_id}'"
                )
            })?;
            Ok(ffi::ConsumerGroupDetails::from(group))
        })
    }

    pub fn get_consumer_groups(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
    ) -> Result<Vec<ffi::ConsumerGroup>, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not get consumer groups: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not get consumer groups: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            let groups = self
                .inner
                .get_consumer_groups(&rust_stream_id, &rust_topic_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not get consumer groups for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(groups.into_iter().map(ffi::ConsumerGroup::from).collect())
        })
    }

    pub fn delete_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not delete consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not delete consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not delete consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .delete_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not delete consumer group '{rust_group_id}' for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn join_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not join consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not join consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not join consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .join_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not join consumer group '{rust_group_id}' for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn get_stats(&self) -> Result<ffi::Stats, String> {
        RUNTIME.block_on(async {
            let stats = self
                .inner
                .get_stats()
                .await
                .map_err(|error| format!("Could not get stats: {error}"))?;
            Ok(ffi::Stats::from(stats))
        })
    }

    pub fn get_me(&self) -> Result<ffi::ClientInfoDetails, String> {
        RUNTIME.block_on(async {
            let client = self
                .inner
                .get_me()
                .await
                .map_err(|error| format!("Could not get current client info: {error}"))?;
            Ok(ffi::ClientInfoDetails::from(client))
        })
    }

    pub fn get_client(&self, client_id: u32) -> Result<ffi::ClientInfoDetails, String> {
        RUNTIME.block_on(async {
            let client = self
                .inner
                .get_client(client_id)
                .await
                .map_err(|error| format!("Could not get client '{client_id}': {error}"))?;
            ffi::ClientInfoDetails::try_from(client)
                .map_err(|error| format!("Could not get client '{client_id}': {error}"))
        })
    }

    pub fn get_clients(&self) -> Result<Vec<ffi::ClientInfo>, String> {
        RUNTIME.block_on(async {
            let clients = self
                .inner
                .get_clients()
                .await
                .map_err(|error| format!("Could not get clients: {error}"))?;
            Ok(clients.into_iter().map(ffi::ClientInfo::from).collect())
        })
    }

    pub fn ping(&self) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .ping()
                .await
                .map_err(|error| format!("Could not ping server: {error}"))?;
            Ok(())
        })
    }

    pub fn leave_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not leave consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not leave consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not leave consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .leave_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not leave consumer group '{rust_group_id}' for topic '{rust_topic_id}' on stream '{rust_stream_id}': {error}"
                    )
                })?;
            Ok(())
        })
    }

    pub fn heartbeat_interval(&self) -> u64 {
        // The upstream client exposes this config-derived value via an async API,
        // so the synchronous C++ wrapper reads it by blocking on the runtime.
        RUNTIME.block_on(async { self.inner.heartbeat_interval().await.as_micros() })
    }

    pub fn snapshot(
        &self,
        snapshot_compression: String,
        snapshot_types: Vec<String>,
    ) -> Result<Vec<u8>, String> {
        let rust_compression = match snapshot_compression.trim() {
            "" => {
                return Err(
                    "Could not capture snapshot: snapshot_compression must not be empty"
                        .to_string(),
                );
            }
            value => RustSnapshotCompression::from_str(value).map_err(|error| {
                format!("Could not capture snapshot: invalid compression '{value}': {error}")
            })?,
        };
        if snapshot_types.is_empty() {
            return Err("Could not capture snapshot: snapshot_types must not be empty".to_string());
        }

        let mut seen_snapshot_types = HashSet::new();
        let rust_snapshot_types = snapshot_types
            .into_iter()
            .filter(|snapshot_type| seen_snapshot_types.insert(snapshot_type.clone()))
            .map(|snapshot_type| {
                RustSystemSnapshotType::from_str(&snapshot_type).map_err(|error| {
                    format!(
                        "Could not capture snapshot: invalid snapshot type '{snapshot_type}': {error}"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        RUNTIME.block_on(async {
            let snapshot = self
                .inner
                .snapshot(rust_compression, rust_snapshot_types)
                .await
                .map_err(|error| format!("Could not capture snapshot: {error}"))?;
            let iggy_common::Snapshot(bytes) = snapshot;
            Ok(bytes)
        })
    }
}

pub unsafe fn delete_connection(client: *mut Client) -> Result<(), String> {
    if client.is_null() {
        return Ok(());
    }

    // `Box::from_raw` below runs unconditionally, so the client is always released regardless
    // of `logout_result`. The result is only used to surface a logout error to the caller — there
    // is no leak path here.
    let logout_result = RUNTIME.block_on(async { unsafe { &*client }.inner.logout_user().await });

    unsafe {
        drop(Box::from_raw(client));
    }

    match logout_result {
        Ok(()) | Err(IggyError::Unauthenticated | IggyError::Disconnected) => Ok(()),
        Err(error) => Err(format!(
            "Could not logout user during deletion of client: {error}"
        )),
    }
}
