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

use crate::binary::dispatch::{HandlerResult, wire_consumer_to_consumer, wire_id_to_identifier};
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedTopic;
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::consumer_offsets::GetConsumerOffsetRequest;
use iggy_binary_protocol::responses::consumer_offsets::ConsumerOffsetResponse;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_consumer_offset(
    req: GetConsumerOffsetRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let consumer = wire_consumer_to_consumer(&req.consumer)?;
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!(
        "session: {session}, command: get_consumer_offset, stream_id: {stream_id}, topic_id: {topic_id}, partition_id: {:?}",
        req.partition_id
    );
    shard.ensure_authenticated(session)?;

    let Some(resolved) =
        shard
            .metadata
            .resolve_for_consumer_offset(session.get_user_id(), &stream_id, &topic_id)?
    else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let topic = ResolvedTopic {
        stream_id: resolved.stream_id,
        topic_id: resolved.topic_id,
    };

    let Ok(offset) = shard
        .get_consumer_offset(session.client_id, consumer, topic, req.partition_id)
        .await
    else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let Some(offset) = offset else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let response = ConsumerOffsetResponse {
        partition_id: offset.partition_id,
        current_offset: offset.current_offset,
        stored_offset: offset.stored_offset,
    };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
