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
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use iggy_common::{IggyError, IggyTimestamp};
use std::rc::Rc;
use tracing::{info, trace, warn};

pub fn spawn_revocation_timeout_checker(shard: Rc<IggyShard>) {
    let interval = shard.config.consumer_group.rebalancing_check_interval;
    let timeout = shard.config.consumer_group.rebalancing_timeout;
    info!(
        "Pending partition revocations will be checked every: {}. Timeout: {}.",
        interval, timeout
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("check_revocation_timeouts")
        .every(interval.get_duration())
        .tick(move |_shutdown| check_revocation_timeouts(shard_clone.clone()))
        .spawn();
}

async fn check_revocation_timeouts(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Checking pending revocation timeouts...");

    let now = IggyTimestamp::now().as_micros();
    let timeout_micros = shard.config.consumer_group.rebalancing_timeout.as_micros();

    let timed_out = shard.metadata.with_metadata(|metadata| {
        metadata
            .streams
            .iter()
            .flat_map(|(stream_id, stream)| {
                stream.topics.iter().flat_map(move |(topic_id, topic)| {
                    topic.consumer_groups.iter().flat_map(move |(_, group)| {
                        let group_id = group.id;
                        group.members.iter().flat_map(move |(slab_id, member)| {
                            let member_id = member.id;
                            member
                                .pending_revocations
                                .iter()
                                .filter(move |revocation| {
                                    now.saturating_sub(revocation.created_at_micros)
                                        >= timeout_micros
                                })
                                .map(move |revocation| {
                                    (
                                        stream_id,
                                        topic_id,
                                        group_id,
                                        slab_id,
                                        member_id,
                                        revocation.partition_id,
                                    )
                                })
                        })
                    })
                })
            })
            .collect::<Vec<_>>()
    });

    if timed_out.is_empty() {
        return Ok(());
    }

    let count = timed_out.len();
    for (stream_id, topic_id, group_id, member_slab_id, member_id, partition_id) in timed_out {
        warn!(
            "Force-completing timed out revocation: stream={stream_id}, topic={topic_id}, group={group_id}, \
             member_slab={member_slab_id}, partition={partition_id}",
        );
        let request =
            ShardRequest::control_plane(ShardRequestPayload::CompletePartitionRevocation {
                stream_id,
                topic_id,
                group_id,
                member_slab_id,
                member_id,
                partition_id,
                timed_out: true,
            });
        let _ = shard.send_to_control_plane(request).await;
    }
    info!("Force-completed {count} timed out partition revocations.");

    Ok(())
}
