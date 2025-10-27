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
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::{error, info, trace};

pub fn spawn_message_saver(shard: Rc<IggyShard>) {
    let period = shard.config.message_saver.interval.get_duration();
    let enforce_fsync = shard.config.message_saver.enforce_fsync;
    info!(
        "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce fsync: {enforce_fsync}.",
        period
    );
    let shard_clone = shard.clone();
    let shard_for_shutdown = shard.clone();
    shard
        .task_registry
        .periodic("save_messages")
        .every(period)
        .last_tick_on_shutdown(true)
        .tick(move |_shutdown| save_messages(shard_clone.clone()))
        .on_shutdown(move |result| {
            fsync_all_segments_on_shutdown(shard_for_shutdown.clone(), result)
        })
        .spawn();
}

async fn save_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Saving buffered messages...");

    let namespaces = shard.get_current_shard_namespaces();
    let mut total_saved_messages = 0u32;
    const REASON: &str = "background saver triggered";

    for ns in namespaces {
        let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
        let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
        let partition_id = ns.partition_id();

        match shard
            .streams
            .persist_messages(
                &stream_id,
                &topic_id,
                partition_id,
                REASON,
                &shard.config.system,
            )
            .await
        {
            Ok(batch_count) => {
                total_saved_messages += batch_count;
            }
            Err(err) => {
                error!(
                    "Failed to save messages for partition {}: {}",
                    partition_id, err
                );
            }
        }
    }

    if total_saved_messages > 0 {
        info!("Saved {} buffered messages on disk.", total_saved_messages);
    }
    Ok(())
}

async fn fsync_all_segments_on_shutdown(shard: Rc<IggyShard>, result: Result<(), IggyError>) {
    // Only fsync if the last save_messages tick succeeded
    if result.is_err() {
        error!(
            "Last save_messages tick failed, skipping fsync: {:?}",
            result
        );
        return;
    }

    trace!("Performing fsync on all segments during shutdown...");

    let namespaces = shard.get_current_shard_namespaces();

    for ns in namespaces {
        let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
        let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
        let partition_id = ns.partition_id();

        match shard
            .streams
            .fsync_all_messages(&stream_id, &topic_id, partition_id)
            .await
        {
            Ok(()) => {
                trace!(
                    "Successfully fsynced segment for stream: {}, topic: {}, partition: {} during shutdown",
                    stream_id, topic_id, partition_id
                );
            }
            Err(err) => {
                error!(
                    "Failed to fsync segment for stream: {}, topic: {}, partition: {} during shutdown: {}",
                    stream_id, topic_id, partition_id, err
                );
            }
        }
    }
}
