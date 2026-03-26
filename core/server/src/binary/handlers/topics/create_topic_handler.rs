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
use bytes::BytesMut;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::topics::CreateTopicRequest;
use iggy_binary_protocol::responses::streams::get_stream::TopicHeader;
use iggy_binary_protocol::responses::topics::get_topic::PartitionResponse;
use iggy_common::create_topic::CreateTopic;
use iggy_common::{
    CompressionAlgorithm, IggyError, IggyExpiry, MaxTopicSize, SenderKind, Validatable,
};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_create_topic(
    req: CreateTopicRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    let stream_id = wire_id_to_identifier(&req.stream_id)?;
    debug!(
        "session: {session}, command: create_topic, stream_id: {stream_id}, name: {}",
        req.name.as_str()
    );
    shard.ensure_authenticated(session)?;

    let command = CreateTopic {
        stream_id,
        partitions_count: req.partitions_count,
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

    let request = ShardRequest::control_plane(ShardRequestPayload::CreateTopicRequest {
        user_id: session.get_user_id(),
        command,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::CreateTopicResponse(data) => {
            let header = TopicHeader {
                id: data.id,
                created_at: data.created_at.into(),
                partitions_count: data.partitions.len() as u32,
                message_expiry: data.message_expiry.into(),
                compression_algorithm: data.compression_algorithm.as_code(),
                max_topic_size: data.max_topic_size.into(),
                replication_factor: data.replication_factor,
                size_bytes: 0,
                messages_count: 0,
                name: WireName::new(data.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
            };
            let partitions: Vec<PartitionResponse> = data
                .partitions
                .iter()
                .map(|p| PartitionResponse {
                    id: p.id as u32,
                    created_at: p.created_at.into(),
                    segments_count: 0,
                    current_offset: 0,
                    size_bytes: 0,
                    messages_count: 0,
                })
                .collect();

            let mut buf = BytesMut::with_capacity(
                header.encoded_size()
                    + partitions
                        .iter()
                        .map(WireEncode::encoded_size)
                        .sum::<usize>(),
            );
            header.encode(&mut buf);
            for partition in &partitions {
                partition.encode(&mut buf);
            }
            sender.send_ok_response(&buf.freeze()).await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected CreateTopicResponse"),
    }

    Ok(HandlerResult::Finished)
}
