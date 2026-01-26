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
use iggy_common::IggyError;
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
    let mut total_saved_messages = 0u64;
    let mut partitions_flushed = 0u32;

    for ns in namespaces {
        if shard.local_partitions.borrow().get(&ns).is_some() {
            match shard
                .flush_unsaved_buffer_from_local_partitions(&ns, false)
                .await
            {
                Ok(saved) => {
                    if saved > 0 {
                        total_saved_messages += saved as u64;
                        partitions_flushed += 1;
                    }
                }
                Err(err) => {
                    error!("Failed to save messages for partition {:?}: {}", ns, err);
                }
            }
        }
    }

    if total_saved_messages > 0 {
        info!("Saved {total_saved_messages} messages from {partitions_flushed} partitions.");
    }
    Ok(())
}

async fn fsync_all_segments_on_shutdown(shard: Rc<IggyShard>, result: Result<(), IggyError>) {
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
        if shard.local_partitions.borrow().get(&ns).is_some() {
            match shard.fsync_all_messages_from_local_partitions(&ns).await {
                Ok(()) => {
                    trace!(
                        "Successfully fsynced segment for partition {:?} during shutdown",
                        ns
                    );
                }
                Err(err) => {
                    error!(
                        "Failed to fsync segment for partition {:?} during shutdown: {}",
                        ns, err
                    );
                }
            }
        }
    }
}
