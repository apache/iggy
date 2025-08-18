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

use crate::io::fs_utils;
use crate::state::system::TopicState;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::TopicStorage;
use crate::streaming::topics::COMPONENT;
use ahash::AHashSet;
use anyhow::Context;
use compio::fs;
use compio::fs::create_dir_all;
use error_set::ErrContext;
use futures::future::join_all;
use iggy_common::IggyError;
use iggy_common::locking::IggyRwLock;
use iggy_common::locking::IggyRwLockFn;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
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
        todo!();
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
        if fs_utils::remove_dir_all(&topic.path).await.is_err() {
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
