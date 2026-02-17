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

use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use iggy_common::IggyError;
use iggy_common::sharding::IggyNamespace;
use std::rc::Rc;
use tracing::{error, info, trace};

pub fn spawn_message_cleaner(shard: Rc<IggyShard>) {
    if !shard.config.data_maintenance.messages.cleaner_enabled {
        info!("Message cleaner is disabled.");
        return;
    }

    let period = shard
        .config
        .data_maintenance
        .messages
        .interval
        .get_duration();
    info!(
        "Message cleaner is enabled, expired segments will be automatically deleted every: {:?}",
        period
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("clean_messages")
        .every(period)
        .tick(move |_shutdown| clean_messages(shard_clone.clone()))
        .spawn();
}

/// Groups namespaces by topic and sends a single `CleanTopicMessages` per topic to the pump.
/// All segment inspection and deletion happens inside the pump handler — no TOCTOU.
async fn clean_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Cleaning expired messages...");

    let namespaces = shard.get_current_shard_namespaces();

    let mut topics: std::collections::HashMap<(usize, usize), Vec<usize>> =
        std::collections::HashMap::new();

    for ns in namespaces {
        topics
            .entry((ns.stream_id(), ns.topic_id()))
            .or_default()
            .push(ns.partition_id());
    }

    let mut total_deleted_segments = 0u64;
    let mut total_deleted_messages = 0u64;

    for ((stream_id, topic_id), partition_ids) in topics {
        let ns = IggyNamespace::new(stream_id, topic_id, partition_ids[0]);
        let payload = ShardRequestPayload::CleanTopicMessages {
            stream_id,
            topic_id,
            partition_ids,
        };
        let request = ShardRequest::data_plane(ns, payload);

        match shard.send_to_data_plane(request).await {
            Ok(ShardResponse::CleanTopicMessages {
                deleted_segments,
                deleted_messages,
            }) => {
                if deleted_segments > 0 {
                    info!(
                        "Deleted {} segments and {} messages for stream {}, topic {}",
                        deleted_segments, deleted_messages, stream_id, topic_id
                    );
                    shard.metrics.decrement_segments(deleted_segments as u32);
                    shard.metrics.decrement_messages(deleted_messages);
                    total_deleted_segments += deleted_segments;
                    total_deleted_messages += deleted_messages;
                }
            }
            Ok(ShardResponse::ErrorResponse(err)) => {
                error!(
                    "Failed to clean messages for stream {}, topic {}: {}",
                    stream_id, topic_id, err
                );
            }
            Ok(_) => unreachable!("Expected CleanTopicMessages response"),
            Err(err) => {
                error!(
                    "Failed to send CleanTopicMessages for stream {}, topic {}: {}",
                    stream_id, topic_id, err
                );
            }
        }
    }

    if total_deleted_segments > 0 {
        info!(
            "Total cleaned: {} segments and {} messages",
            total_deleted_segments, total_deleted_messages
        );
    }

    Ok(())
}
