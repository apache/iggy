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
use crate::binary::handlers::streams::get_stream_handler::build_topic_header;
use crate::metadata::TopicMeta;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::topics::GetTopicRequest;
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_topic(
    req: GetTopicRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    let topic_id = wire_id_to_identifier(&req.topic_id)?;
    debug!("session: {session}, command: get_topic, stream_id: {stream_id}, topic_id: {topic_id}");
    shard.ensure_authenticated(session)?;

    let Some(topic) = shard
        .metadata
        .query_topic(session.get_user_id(), &stream_id, &topic_id)?
    else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let response = build_get_topic_response(&topic)?;
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}

fn build_get_topic_response(topic: &TopicMeta) -> Result<GetTopicResponse, IggyError> {
    let header = build_topic_header(topic)?;

    let partitions: Vec<PartitionResponse> = topic
        .partitions
        .iter()
        .enumerate()
        .map(|(partition_id, partition)| PartitionResponse {
            id: partition_id as u32,
            created_at: partition.created_at.into(),
            segments_count: partition.stats.segments_count_inconsistent(),
            current_offset: partition.stats.current_offset(),
            size_bytes: partition.stats.size_bytes_inconsistent(),
            messages_count: partition.stats.messages_count_inconsistent(),
        })
        .collect();

    Ok(GetTopicResponse {
        topic: header,
        partitions,
    })
}
