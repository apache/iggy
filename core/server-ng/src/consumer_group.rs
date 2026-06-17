// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Consumer-group Join/Leave request enrichment.
//!
//! The wire `Join`/`Leave` requests carry only the stream/topic/group
//! identifiers. The replicated metadata `apply` additionally needs the
//! joining client's VSR id (which member) and, for Join, the topic's
//! partition count (to seed the group's partition list) -- and it cannot
//! read the Streams STM from inside the consumer-group apply. So the
//! primary enriches the op here before replication, mirroring the PAT mint
//! in [`crate::pat`] and the password hash in [`crate::users`].

use crate::bootstrap::ServerNgShard;
use crate::wire::{request_body, rewrite_request_body};
use consensus::MetadataHandle;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::consumer_groups::{
    JoinConsumerGroupRequest as WireJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as WireLeaveConsumerGroupRequest,
};
use iggy_binary_protocol::{Operation, RequestHeader};
use iggy_common::IggyError;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::consumer_group::{
    JoinConsumerGroupRequest as ReplicatedJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as ReplicatedLeaveConsumerGroupRequest,
};
use server_common::Message;
use std::rc::Rc;

/// Rewrite a `Join`/`Leave` request body into the replicated form carrying
/// the client's VSR id (and, for Join, the topic partition count). Every
/// other operation passes through unchanged.
pub(crate) fn maybe_rewrite_consumer_group_request(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError> {
    let operation = request.header().operation;
    let client_id = request.header().client;
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::JoinConsumerGroup => {
            let wire = WireJoinConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let ((resolved_stream_id, resolved_topic_id), partitions_count) = shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .partition_count_context(&wire.stream_id, &wire.topic_id)
                .map_or(((0, 0), 0), |((stream_id, topic_id), count)| {
                    (
                        (
                            u32::try_from(stream_id).unwrap_or(0),
                            u32::try_from(topic_id).unwrap_or(0),
                        ),
                        count,
                    )
                });
            ReplicatedJoinConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
                partitions_count,
                resolved_stream_id,
                resolved_topic_id,
            }
            .to_bytes()
        }
        Operation::LeaveConsumerGroup => {
            let wire = WireLeaveConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            ReplicatedLeaveConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
            }
            .to_bytes()
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}
