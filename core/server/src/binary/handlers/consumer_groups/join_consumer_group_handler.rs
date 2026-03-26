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
use iggy_binary_protocol::requests::consumer_groups::JoinConsumerGroupRequest;
use iggy_common::join_consumer_group::JoinConsumerGroup;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_join_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_join_consumer_group(
    req: JoinConsumerGroupRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    let group_id = wire_id_to_identifier(&req.group_id)?;
    debug!(
        "session: {session}, command: join_consumer_group, stream_id: {stream_id}, topic_id: {topic_id}, group_id: {group_id}"
    );
    shard.ensure_authenticated(session)?;

    let command = JoinConsumerGroup {
        stream_id,
        topic_id,
        group_id,
    };

    let request = ShardRequest::control_plane(ShardRequestPayload::JoinConsumerGroupRequest {
        user_id: session.get_user_id(),
        client_id: session.client_id,
        command,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::JoinConsumerGroupResponse => {
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected JoinConsumerGroupResponse"),
    }

    Ok(HandlerResult::Finished)
}
