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
use iggy_binary_protocol::{KIND_CONSUMER_GROUP, Operation, RequestHeader, WireIdentifier};
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
/// answer is treated as not-in-flight (eager handoff). An eager handoff leaves
/// no `PendingRevocation` record, so the reconciler never revisits it -- a
/// misclassification here just redelivers the uncommitted range to the new
/// owner, which is correct under at-least-once.
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
    // Resolve namespaces up front (sync), then fire every partition's
    // `GroupOffsetState` read concurrently. The reads are independent, so a
    // wide-topic join must not serialize N cross-shard round-trips before the
    // join op can even be proposed.
    let targets: Vec<(u32, _)> = partition_ids
        .into_iter()
        .filter_map(|partition_id| {
            resolve_partition_namespace(shard, stream_id, topic_id, Some(partition_id))
                .ok()
                .map(|ns| (partition_id, ns))
        })
        .collect();
    let results = futures::future::join_all(targets.iter().map(|&(partition_id, ns)| async move {
        let reply = shard
            .partition_read(
                ns,
                PartitionRead::GroupOffsetState {
                    group_id: monotonic_group_id,
                },
            )
            .await;
        (partition_id, ns, reply)
    }))
    .await;

    let mut in_flight = Vec::new();
    let mut stale_clears = Vec::new();
    for (partition_id, ns, reply) in results {
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
        } else {
            // Stale mark from a removed member: drop it so a later join in this
            // same restart does not misread it once the partition is reassigned.
            stale_clears.push(shard.partition_read(
                ns,
                PartitionRead::ClearGroupLastPolled {
                    group_id: monotonic_group_id,
                },
            ));
        }
    }
    // Fire the stale-mark clears concurrently too; the result is unused.
    futures::future::join_all(stale_clears).await;
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
    // The 4 store/delete (v1 + v2) ops differ only in the decode type; this
    // collapses their identical decode -> resolve group id -> rewrite consumer
    // id -> re-encode bodies. A non-group consumer or unresolved group returns
    // the request untouched (the apply/read path handles the miss).
    macro_rules! rewrite_group_offset {
        ($ty:ty) => {{
            let mut wire = <$ty>::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            let Some(group_id) =
                resolve_group_offset_id(shard, &wire.consumer, (&wire.stream_id, &wire.topic_id))
            else {
                return Ok(request);
            };
            // The partition-plane group-offset key is u32 (see the documented
            // ceiling on `Topic::next_consumer_group_id`). Clamp on the
            // ~4-billion-creates overflow rather than panic this live
            // client-driven path, matching `iggy_partition.rs`'s identical cast.
            wire.consumer.id = WireIdentifier::Numeric(u32::try_from(group_id).unwrap_or(u32::MAX));
            wire.to_bytes()
        }};
    }
    let rewritten = match operation {
        Operation::StoreConsumerOffset => rewrite_group_offset!(StoreConsumerOffsetRequest),
        Operation::StoreConsumerOffset2 => rewrite_group_offset!(StoreConsumerOffset2Request),
        Operation::DeleteConsumerOffset => rewrite_group_offset!(DeleteConsumerOffsetRequest),
        Operation::DeleteConsumerOffset2 => rewrite_group_offset!(DeleteConsumerOffset2Request),
        // The outer `matches!` already filtered to the 4 ops above, but the
        // match is over the 37-variant `Operation`, so a catch-all is required.
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
    if consumer.kind != KIND_CONSUMER_GROUP {
        return None;
    }
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .resolve_consumer_group_id(namespace.0, namespace.1, &consumer.id)
}
