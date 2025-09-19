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

use crate::{shard::IggyShard, shard_info};
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::{error, info, trace};

pub async fn save_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.message_saver;
    if !config.enabled {
        info!("Message saver is disabled.");
        return Ok(());
    }

    // TODO: Maybe we should get rid of it in order to not complicate, and use the fsync settings per partition from config.
    let enforce_fsync = config.enforce_fsync;
    let interval = config.interval;
    info!(
        "Message saver is enabled, buffered messages will be automatically saved every: {interval}, enforce fsync: {enforce_fsync}."
    );

    let mut interval_timer = compio::time::interval(interval.get_duration());
    loop {
        interval_timer.tick().await;
        trace!("Saving buffered messages...");

        let namespaces = shard.get_current_shard_namespaces();
        let mut total_saved_messages = 0u32;
        let reason = "background saver triggered".to_string();

        for ns in namespaces {
            let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
            let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
            let partition_id = ns.partition_id();

            match shard
                .streams2
                .persist_messages(
                    shard.id,
                    &stream_id,
                    &topic_id,
                    partition_id,
                    reason.clone(),
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
            shard_info!(shard.id, "Saved {} buffered messages on disk.", total_saved_messages);
        }

        trace!("Finished saving buffered messages.");
    }
}
