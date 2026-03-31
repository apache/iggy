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

use crate::binary::dispatch::HandlerResult;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::system::get_stats::{CacheMetricEntry, StatsResponse};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_stats(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_stats");
    shard.ensure_authenticated(session)?;
    shard.metadata.perm_get_stats(session.get_user_id())?;

    let request = ShardRequest::control_plane(ShardRequestPayload::GetStats {
        user_id: session.get_user_id(),
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::GetStatsResponse(stats) => {
            let response = StatsResponse {
                process_id: stats.process_id,
                cpu_usage: stats.cpu_usage,
                total_cpu_usage: stats.total_cpu_usage,
                memory_usage: stats.memory_usage.as_bytes_u64(),
                total_memory: stats.total_memory.as_bytes_u64(),
                available_memory: stats.available_memory.as_bytes_u64(),
                run_time: stats.run_time.into(),
                start_time: stats.start_time.into(),
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
                iggy_server_semver: stats.iggy_server_semver,
                cache_metrics: stats
                    .cache_metrics
                    .iter()
                    .map(|(key, metrics)| CacheMetricEntry {
                        stream_id: key.stream_id,
                        topic_id: key.topic_id,
                        partition_id: key.partition_id,
                        hits: metrics.hits,
                        misses: metrics.misses,
                        hit_ratio: metrics.hit_ratio,
                    })
                    .collect(),
                threads_count: stats.threads_count,
                free_disk_space: stats.free_disk_space.as_bytes_u64(),
                total_disk_space: stats.total_disk_space.as_bytes_u64(),
            };
            sender.send_ok_response(&response.to_bytes()).await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected GetStatsResponse"),
    }

    Ok(HandlerResult::Finished)
}
