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
    shard
        .task_registry
        .periodic("save_messages")
        .every(period)
        // No last_tick_on_shutdown — the pump handles final flush + fsync
        // during its own shutdown (see message_pump.rs).
        .tick(move |_shutdown| save_messages(shard_clone.clone()))
        .spawn();
}

async fn save_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Saving buffered messages...");

    let namespaces = shard.get_current_shard_namespaces();
    let mut partitions_flushed = 0u32;

    for ns in namespaces {
        let payload = ShardRequestPayload::FlushUnsavedBuffer { fsync: false };
        let request = ShardRequest::data_plane(ns, payload);
        match shard.send_to_data_plane(request).await {
            Ok(ShardResponse::FlushUnsavedBuffer { flushed_count }) if flushed_count > 0 => {
                partitions_flushed += 1;
            }
            Ok(ShardResponse::FlushUnsavedBuffer { .. }) => {}
            Ok(ShardResponse::ErrorResponse(err)) => {
                error!("Failed to save messages for partition {:?}: {}", ns, err);
            }
            Err(err) => {
                error!("Failed to save messages for partition {:?}: {}", ns, err);
            }
            _ => {}
        }
    }

    if partitions_flushed > 0 {
        info!("Flushed {partitions_flushed} partitions.");
    }
    Ok(())
}
