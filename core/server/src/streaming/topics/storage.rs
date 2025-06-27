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

use crate::state::system::TopicState;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::TopicStorage;
use crate::streaming::topics::COMPONENT;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use ahash::AHashSet;
use anyhow::Context;
use error_set::ErrContext;
use futures::future::join_all;
use iggy_common::IggyError;
use iggy_common::locking::IggyRwLock;
use iggy_common::locking::IggySharedMutFn;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir_all;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct FileTopicStorage;

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerGroupData {
    id: u32,
    name: String,
}

impl TopicStorage for FileTopicStorage {
    async fn load(&self, topic: &mut Topic, mut state: TopicState) -> Result<(), IggyError> {
        info!("Loading topic {} from disk...", topic);
        if !Path::new(&topic.path).exists() {
            return Err(IggyError::TopicIdNotFound(topic.topic_id, topic.stream_id));
        }

        let message_expiry = Topic::get_message_expiry(state.message_expiry, &topic.config);
        let max_topic_size = Topic::get_max_topic_size(state.max_topic_size, &topic.config)?;
        topic.created_at = state.created_at;
        topic.message_expiry = message_expiry;
        topic.max_topic_size = max_topic_size;
        topic.compression_algorithm = state.compression_algorithm;
        topic.replication_factor = state.replication_factor.unwrap_or(1);

        let mut dir_entries = fs::read_dir(&topic.partitions_path).await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} for topic with ID: {} and path: {}",
                                     topic.topic_id, topic.stream_id, topic.topic_id, &topic.partitions_path))
            .map_err(|_| IggyError::CannotReadPartitions)?;

        let mut unloaded_partitions = Vec::new();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() || metadata.unwrap().is_file() {
                continue;
            }

            let name = dir_entry.file_name().into_string().unwrap();
            let partition_id = name.parse::<u32>();
            if partition_id.is_err() {
                error!("Invalid partition ID file with name: '{}'.", name);
                continue;
            }

            let partition_id = partition_id.unwrap();
            let partition_state = state.partitions.get(&partition_id);
            if partition_state.is_none() {
                let stream_id = topic.stream_id;
                let topic_id = topic.topic_id;
                error!(
                    "Partition with ID: '{partition_id}' for stream with ID: '{stream_id}' and topic with ID: '{topic_id}' was not found in state, but exists on disk and will be removed."
                );
                if let Err(error) = fs::remove_dir_all(&dir_entry.path()).await {
                    error!("Cannot remove partition directory: {error}");
                } else {
                    warn!(
                        "Partition with ID: '{partition_id}' for stream with ID: '{stream_id}' and topic with ID: '{topic_id}' was removed."
                    );
                }
                continue;
            }

            let partition_state = partition_state.unwrap();
            let partition = Partition::create(
                topic.stream_id,
                topic.topic_id,
                partition_id,
                false,
                topic.config.clone(),
                topic.storage.clone(),
                message_expiry,
                topic.messages_count_of_parent_stream.clone(),
                topic.messages_count.clone(),
                topic.size_of_parent_stream.clone(),
                topic.size_bytes.clone(),
                topic.segments_count_of_parent_stream.clone(),
                partition_state.created_at,
            )
            .await;
            unloaded_partitions.push(partition);
        }

        let state_partition_ids = state.partitions.keys().copied().collect::<AHashSet<u32>>();
        let unloaded_partition_ids = unloaded_partitions
            .iter()
            .map(|partition| partition.partition_id)
            .collect::<AHashSet<u32>>();
        let missing_ids = state_partition_ids
            .difference(&unloaded_partition_ids)
            .copied()
            .collect::<AHashSet<u32>>();
        if missing_ids.is_empty() {
            info!(
                "All partitions for topic with ID: '{}' for stream with ID: '{}' found on disk were found in state.",
                topic.topic_id, topic.stream_id
            );
        } else {
            warn!(
                "Partitions with IDs: '{missing_ids:?}' for topic with ID: '{topic_id}' for stream with ID: '{stream_id}' were not found on disk.",
                topic_id = topic.topic_id,
                stream_id = topic.stream_id
            );
            if topic.config.recovery.recreate_missing_state {
                info!(
                    "Recreating missing state in recovery config is enabled, missing partitions will be created for topic with ID: '{}' for stream with ID: '{}'.",
                    topic.topic_id, topic.stream_id
                );

                for partition_id in missing_ids {
                    let partition_state = state.partitions.get(&partition_id).unwrap();
                    let mut partition = Partition::create(
                        topic.stream_id,
                        topic.topic_id,
                        partition_id,
                        true,
                        topic.config.clone(),
                        topic.storage.clone(),
                        message_expiry,
                        topic.messages_count_of_parent_stream.clone(),
                        topic.messages_count.clone(),
                        topic.size_of_parent_stream.clone(),
                        topic.size_bytes.clone(),
                        topic.segments_count_of_parent_stream.clone(),
                        partition_state.created_at,
                    )
                    .await;
                    partition.persist().await.with_error_context(|error| {
                        format!(
                            "{COMPONENT} (error: {error}) - failed to persist partition: {partition}"
                        )
                    })?;
                    partition.segments.clear();
                    unloaded_partitions.push(partition);
                    info!(
                        "Created missing partition with ID: '{partition_id}', for topic with ID: '{}' for stream with ID: '{}'.",
                        topic.topic_id, topic.stream_id
                    );
                }
            } else {
                warn!(
                    "Recreating missing state in recovery config is disabled, missing partitions will not be created for topic with ID: '{}' for stream with ID: '{}'.",
                    topic.topic_id, topic.stream_id
                );
            }
        }

        let stream_id = topic.stream_id;
        let topic_id = topic.topic_id;
        let loaded_partitions = Arc::new(Mutex::new(Vec::new()));
        let mut load_partitions = Vec::new();
        for mut partition in unloaded_partitions {
            let loaded_partitions = loaded_partitions.clone();
            let partition_state = state.partitions.remove(&partition.partition_id).unwrap();
            let load_partition = monoio::spawn(async move {
                match partition.load(partition_state).await {
                    Ok(_) => {
                        loaded_partitions.lock().await.push(partition);
                    }
                    Err(error) => {
                        error!(
                            "Failed to load partition with ID: {} for stream with ID: {stream_id} and topic with ID: {topic_id}. Error: {error}",
                            partition.partition_id
                        );
                    }
                }
            });
            load_partitions.push(load_partition);
        }

        join_all(load_partitions).await;
        for partition in loaded_partitions.lock().await.drain(..) {
            topic
                .partitions
                .insert(partition.partition_id, IggyRwLock::new(partition));
        }

        for consumer_group in state.consumer_groups.into_values() {
            let consumer_group = ConsumerGroup::new(
                topic.topic_id,
                consumer_group.id,
                &consumer_group.name,
                topic.get_partitions_count(),
            );
            topic
                .consumer_groups_ids
                .insert(consumer_group.name.to_owned(), consumer_group.group_id);
            topic
                .consumer_groups
                .borrow_mut()
                .insert(consumer_group.group_id, consumer_group);
        }

        info!("Loaded topic {topic}");

        Ok(())
    }

    async fn save(&self, topic: &Topic) -> Result<(), IggyError> {
        if !Path::new(&topic.path).exists() && create_dir_all(&topic.path).await.is_err() {
            return Err(IggyError::CannotCreateTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        if !Path::new(&topic.partitions_path).exists()
            && create_dir_all(&topic.partitions_path).await.is_err()
        {
            return Err(IggyError::CannotCreatePartitionsDirectory(
                topic.stream_id,
                topic.topic_id,
            ));
        }

        info!(
            "Saving {} partition(s) for topic {topic}...",
            topic.partitions.len()
        );
        for (_, partition) in topic.partitions.iter() {
            let mut partition = partition.write().await;
            partition.persist().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to persist partition, topic: {topic}"
                )
            })?;
        }

        info!("Saved topic {topic}");

        Ok(())
    }

    async fn delete(&self, topic: &Topic) -> Result<(), IggyError> {
        info!("Deleting topic {topic}...");
        if fs::remove_dir_all(&topic.path).await.is_err() {
            return Err(IggyError::CannotDeleteTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        info!(
            "Deleted topic with ID: {} for stream with ID: {}.",
            topic.topic_id, topic.stream_id
        );

        Ok(())
    }
}
