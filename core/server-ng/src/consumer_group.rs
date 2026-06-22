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
use crate::responses::resolve_partition_namespace;
use crate::wire::{request_body, rewrite_request_body};
use consensus::MetadataHandle;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_groups::{
    JoinConsumerGroupRequest as WireJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as WireLeaveConsumerGroupRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{Operation, RequestHeader, WireIdentifier};
use iggy_common::IggyError;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::consumer_group::{
    JoinConsumerGroupRequest as ReplicatedJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as ReplicatedLeaveConsumerGroupRequest,
};
use server_common::Message;
use shard::{PartitionRead, PartitionReadReply};
use std::rc::Rc;

/// Rewrite a `Join`/`Leave` request body into the replicated form carrying the
/// client's VSR id (which the apply can't read from the consensus header).
///
/// For `Join` the home shard also gathers `in_flight` -- the group's partitions
/// with uncommitted polled data -- by reading each partition's poll/commit state
/// (via the partition-read mesh), so the cooperative rebalance pending-revokes
/// only those and hands off never-polled/drained partitions synchronously at
/// join. Every other operation passes through.
pub(crate) async fn maybe_rewrite_consumer_group_request(
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
            let in_flight =
                gather_in_flight(shard, &wire.stream_id, &wire.topic_id, &wire.group_id).await;
            ReplicatedJoinConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
                in_flight,
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

/// Gather the group's in-flight partitions (`last_polled` present and
/// `committed < last_polled`) for the cooperative-rebalance classification. A
/// not-yet-created group, an unresolved topic, or a partition that does not
/// answer is treated as not-in-flight (eager handoff), which the reconciler
/// corrects on a later pass if it was wrong.
async fn gather_in_flight(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    group_id: &WireIdentifier,
) -> Vec<u32> {
    let streams = shard.plane.metadata().mux_stm.streams();
    let Some(monotonic_group_id) = streams.resolve_consumer_group_id(stream_id, topic_id, group_id)
    else {
        // Fresh group (e.g. create-if-not-exists): nothing polled yet.
        return Vec::new();
    };
    let Some(partition_ids) = streams.topic_partition_ids(stream_id, topic_id) else {
        return Vec::new();
    };
    // Partitions a live member currently owns. A `last_polled` past the commit
    // only means in-flight work when a live member still holds the partition;
    // for an unowned one it is the residue of a member removed on disconnect
    // (the reconnect case), which must be reassigned and re-read, not protected.
    let assigned = streams
        .consumer_group_assigned_partitions(stream_id, topic_id, group_id)
        .unwrap_or_default();
    let mut in_flight = Vec::new();
    for partition_id in partition_ids {
        let Ok(ns) = resolve_partition_namespace(shard, stream_id, topic_id, Some(partition_id))
        else {
            continue;
        };
        let reply = shard
            .partition_read(
                ns,
                PartitionRead::GroupOffsetState {
                    group_id: monotonic_group_id,
                },
            )
            .await;
        let Some(PartitionReadReply::GroupOffsetState {
            last_polled: Some(polled),
            committed,
        }) = reply
        else {
            continue;
        };
        if committed.is_some_and(|c| c >= polled) {
            continue;
        }
        if assigned.contains(&partition_id) {
            in_flight.push(partition_id);
        } else if let Ok(ns) =
            resolve_partition_namespace(shard, stream_id, topic_id, Some(partition_id))
        {
            // Stale mark from a removed member: drop it so a later join in this
            // same restart does not misread it once the partition is reassigned.
            let _ = shard
                .partition_read(
                    ns,
                    PartitionRead::ClearGroupLastPolled {
                        group_id: monotonic_group_id,
                    },
                )
                .await;
        }
    }
    in_flight
}

/// Rewrite a group consumer-offset op so its consumer id is the group's
/// monotonic id rather than the wire name. The partition plane keys group
/// offsets by that numeric id (decoded from `WireIdentifier::Numeric`), so the
/// read path -- which resolves the same id from metadata -- and the reconciler
/// purge agree, and a re-created group (new id) never inherits a stale offset.
/// Individual-consumer ops and every other operation pass through untouched.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn maybe_rewrite_consumer_offset_request(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError> {
    let operation = request.header().operation;
    if !matches!(
        operation,
        Operation::StoreConsumerOffset
            | Operation::StoreConsumerOffset2
            | Operation::DeleteConsumerOffset
            | Operation::DeleteConsumerOffset2
    ) {
        return Ok(request);
    }
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::StoreConsumerOffset => {
            let mut wire = StoreConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let Some(group_id) =
                resolve_group_offset_id(shard, &wire.consumer, (&wire.stream_id, &wire.topic_id))
            else {
                return Ok(request);
            };
            wire.consumer.id = WireIdentifier::Numeric(group_id as u32);
            wire.to_bytes()
        }
        Operation::StoreConsumerOffset2 => {
            let mut wire = StoreConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let Some(group_id) =
                resolve_group_offset_id(shard, &wire.consumer, (&wire.stream_id, &wire.topic_id))
            else {
                return Ok(request);
            };
            wire.consumer.id = WireIdentifier::Numeric(group_id as u32);
            wire.to_bytes()
        }
        Operation::DeleteConsumerOffset => {
            let mut wire = DeleteConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let Some(group_id) =
                resolve_group_offset_id(shard, &wire.consumer, (&wire.stream_id, &wire.topic_id))
            else {
                return Ok(request);
            };
            wire.consumer.id = WireIdentifier::Numeric(group_id as u32);
            wire.to_bytes()
        }
        Operation::DeleteConsumerOffset2 => {
            let mut wire = DeleteConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let Some(group_id) =
                resolve_group_offset_id(shard, &wire.consumer, (&wire.stream_id, &wire.topic_id))
            else {
                return Ok(request);
            };
            wire.consumer.id = WireIdentifier::Numeric(group_id as u32);
            wire.to_bytes()
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

/// Resolve the monotonic group id for a group consumer-offset op, or `None` for
/// an individual consumer (kind != 2) / unresolved group (leave the body as-is;
/// the apply / read path handle the miss).
fn resolve_group_offset_id(
    shard: &Rc<ServerNgShard>,
    consumer: &WireConsumer,
    namespace: (&WireIdentifier, &WireIdentifier),
) -> Option<u64> {
    if consumer.kind != 2 {
        return None;
    }
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .resolve_consumer_group_id(namespace.0, namespace.1, &consumer.id)
}
