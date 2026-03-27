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

use crate::binary::dispatch::{HandlerResult, wire_id_to_identifier};
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_delete_segments", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_delete_segments(
    req: DeleteSegmentsRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!(
        "session: {session}, command: delete_segments, stream_id: {stream_id}, topic_id: {topic_id}"
    );
    shard.ensure_authenticated(session)?;

    let partition_id = req.partition_id as usize;
    let segments_count = req.segments_count;

    let partition = shard.resolve_partition_for_delete_segments(
        session.get_user_id(),
        &stream_id,
        &topic_id,
        partition_id,
    )?;

    let namespace = IggyNamespace::new(
        partition.stream_id,
        partition.topic_id,
        partition.partition_id,
    );
    let payload = ShardRequestPayload::DeleteSegments { segments_count };
    let request = ShardRequest::data_plane(namespace, payload);

    match shard.send_to_data_plane(request).await? {
        ShardResponse::DeleteSegments {
            deleted_segments,
            deleted_messages,
        } => {
            shard.metrics.decrement_segments(deleted_segments as u32);
            shard.metrics.decrement_messages(deleted_messages);
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected DeleteSegments"),
    }

    Ok(HandlerResult::Finished)
}
