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
use crate::streaming::session::Session;
use bytes::Bytes;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::consumer_groups::GetConsumerGroupsRequest;
use iggy_binary_protocol::responses::consumer_groups::{
    ConsumerGroupResponse, GetConsumerGroupsResponse,
};
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_consumer_groups(
    req: GetConsumerGroupsRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!(
        "session: {session}, command: get_consumer_groups, stream_id: {stream_id}, topic_id: {topic_id}"
    );
    shard.ensure_authenticated(session)?;

    let Some(consumer_groups) =
        shard
            .metadata
            .query_consumer_groups(session.get_user_id(), &stream_id, &topic_id)?
    else {
        sender.send_ok_response(&Bytes::new()).await?;
        return Ok(HandlerResult::Finished);
    };

    let groups: Vec<ConsumerGroupResponse> = consumer_groups
        .iter()
        .map(|cg| {
            Ok(ConsumerGroupResponse {
                id: cg.id as u32,
                partitions_count: cg.partitions.len() as u32,
                members_count: cg.members.len() as u32,
                name: WireName::new(cg.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
            })
        })
        .collect::<Result<_, IggyError>>()?;
    let response = GetConsumerGroupsResponse { groups };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
