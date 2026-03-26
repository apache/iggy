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
use crate::metadata::{StreamMeta, TopicMeta};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::streams::GetStreamRequest;
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{GetStreamResponse, TopicHeader};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_stream(
    req: GetStreamRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: get_stream, stream_id: {:?}",
        req.stream_id
    );
    shard.ensure_authenticated(session)?;

    let stream_id = wire_id_to_identifier(&req.stream_id)?;

    let Some(stream) = shard
        .metadata
        .query_stream(session.get_user_id(), &stream_id)?
    else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let response = build_get_stream_response(&stream)?;
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}

pub(crate) fn compute_stream_stats(stream: &StreamMeta) -> (u64, u64) {
    let mut size = 0u64;
    let mut messages = 0u64;
    for (_, topic) in stream.topics.iter() {
        for partition in topic.partitions.iter() {
            size += partition.stats.size_bytes_inconsistent();
            messages += partition.stats.messages_count_inconsistent();
        }
    }
    (size, messages)
}

fn compute_topic_stats(topic: &TopicMeta) -> (u64, u64) {
    let mut size = 0u64;
    let mut messages = 0u64;
    for partition in topic.partitions.iter() {
        size += partition.stats.size_bytes_inconsistent();
        messages += partition.stats.messages_count_inconsistent();
    }
    (size, messages)
}

pub(crate) fn build_topic_header(topic: &TopicMeta) -> Result<TopicHeader, IggyError> {
    let (size, messages) = compute_topic_stats(topic);
    Ok(TopicHeader {
        id: topic.id as u32,
        created_at: topic.created_at.into(),
        partitions_count: topic.partitions.len() as u32,
        message_expiry: topic.message_expiry.into(),
        compression_algorithm: topic.compression_algorithm.as_code(),
        max_topic_size: topic.max_topic_size.into(),
        replication_factor: topic.replication_factor,
        size_bytes: size,
        messages_count: messages,
        name: WireName::new(topic.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
    })
}

fn build_get_stream_response(stream: &StreamMeta) -> Result<GetStreamResponse, IggyError> {
    let mut topic_ids: Vec<_> = stream.topics.iter().map(|(k, _)| k).collect();
    topic_ids.sort_unstable();

    let (total_size, total_messages) = compute_stream_stats(stream);

    let mut topics = Vec::with_capacity(topic_ids.len());
    for &topic_id in &topic_ids {
        if let Some(topic) = stream.topics.get(topic_id) {
            topics.push(build_topic_header(topic)?);
        }
    }

    Ok(GetStreamResponse {
        stream: StreamResponse {
            id: stream.id as u32,
            created_at: stream.created_at.into(),
            topics_count: topic_ids.len() as u32,
            size_bytes: total_size,
            messages_count: total_messages,
            name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
        },
        topics,
    })
}
