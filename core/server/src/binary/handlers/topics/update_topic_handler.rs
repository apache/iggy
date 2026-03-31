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
use iggy_binary_protocol::requests::topics::UpdateTopicRequest;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{
    CompressionAlgorithm, IggyError, IggyExpiry, MaxTopicSize, SenderKind, Validatable,
};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_update_topic(
    req: UpdateTopicRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!(
        "session: {session}, command: update_topic, stream_id: {stream_id}, topic_id: {topic_id}"
    );
    shard.ensure_authenticated(session)?;

    let command = UpdateTopic {
        stream_id,
        topic_id,
        compression_algorithm: CompressionAlgorithm::from_code(req.compression_algorithm)?,
        message_expiry: IggyExpiry::from(req.message_expiry),
        max_topic_size: MaxTopicSize::from(req.max_topic_size),
        replication_factor: if req.replication_factor == 0 {
            None
        } else {
            Some(req.replication_factor)
        },
        name: req.name.to_string(),
    };
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdateTopicRequest {
        user_id: session.get_user_id(),
        command,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::UpdateTopicResponse => {
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected UpdateTopicResponse"),
    }

    Ok(HandlerResult::Finished)
}
