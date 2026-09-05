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

//! Identifier resolution against the replicated streams STM.
//!
//! Resolve partition-addressed requests to the owning `IggyNamespace` through
//! the STM's stream / topic resolvers, naming the level that missed with the
//! legacy typed not-found errors. Also home of the consumer-group
//! ownership fence the offset ops run before resolving, which stops a stale
//! group member from writing an offset for a partition it no longer owns.

use crate::shell::{ShellBus, ShellShard};
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::SendMessagesHeader;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::{
    KIND_CONSUMER_GROUP, Operation, WireDecode, WireIdentifier, WirePartitioning,
};
use iggy_common::{Identifier, IggyError};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

/// Fence a consumer-group offset commit/delete: a group consumer may only
/// touch the offset of a partition it currently owns. `Ok` for individual
/// consumers (no fence) and for owned group partitions; `Err` otherwise so a
/// stale client re-syncs instead of corrupting the shared group offset.
fn fence_group_offset<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if consumer.kind != KIND_CONSUMER_GROUP {
        return Ok(());
    }
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    #[allow(clippy::cast_possible_truncation)]
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        // Commit fence: allow a pending-revoked partition (the source commits it
        // to drain the cooperative handoff), so `require_pollable = false`.
        .consumer_group_fence(
            stream_id,
            topic_id,
            &consumer.id,
            client_id,
            partition_id,
            false,
        )
        .map(|_| ())
        .ok_or(IggyError::ConsumerGroupPartitionNotOwned(
            client_id as u32,
            partition_id,
        ))
}

/// Fence a consumer-group offset op then resolve its target partition
/// namespace. Shared by the four `Store`/`Delete` consumer-offset arms.
fn fence_and_resolve_offset_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    fence_group_offset(
        shard,
        consumer,
        stream_id,
        topic_id,
        partition_id,
        client_id,
    )?;
    resolve_partition_namespace(shard, stream_id, topic_id, partition_id)
}

pub fn resolve_partition_request_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    operation: Operation,
    body: &[u8],
    client_id: u128,
) -> Result<u64, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let namespace = match operation {
        Operation::SendMessages => {
            if body.len() < 4 {
                return Err(IggyError::InvalidCommand);
            }
            let metadata_length = u32::from_le_bytes(
                body[..4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if body.len() < 4 + metadata_length {
                return Err(IggyError::InvalidCommand);
            }
            let header = SendMessagesHeader::decode_from(&body[4..4 + metadata_length])
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_send_messages_namespace(shard, &header)?
        }
        Operation::StoreConsumerOffset => {
            let request = StoreConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::DeleteConsumerOffset => {
            let request = DeleteConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::DeleteSegments => {
            let request =
                DeleteSegmentsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                Some(request.partition_id),
            )?
        }
        _ => return Err(IggyError::FeatureUnavailable),
    };
    Ok(namespace.inner())
}

fn resolve_send_messages_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    header: &SendMessagesHeader,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let partition_id = match &header.partitioning {
        WirePartitioning::PartitionId(partition_id) => *partition_id,
        WirePartitioning::Balanced => shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .next_balanced_partition(&header.stream_id, &header.topic_id)
            .ok_or(IggyError::InvalidIdentifier)?,
        WirePartitioning::MessagesKey(key) => shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .partition_by_messages_key(&header.stream_id, &header.topic_id, key)
            .ok_or(IggyError::InvalidIdentifier)?,
    };
    resolve_partition_namespace(
        shard,
        &header.stream_id,
        &header.topic_id,
        Some(partition_id),
    )
}

pub fn resolve_partition_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    let streams = shard.plane.metadata().mux_stm.streams();
    if let Some(namespace) = streams.namespace_from_partition(stream_id, topic_id, partition_id) {
        return Ok(namespace);
    }
    // Name the level that missed - partition, topic, or stream - with the
    // legacy typed not-found, so a client can tell an addressing typo from an
    // empty partition. Callers that shape their own reply (empty poll, group
    // gather) treat every variant the same, so the split is reply-visible only
    // where a caller denies typed.
    if streams.topic_partition_ids(stream_id, topic_id).is_some() {
        return Err(IggyError::PartitionNotFound(
            partition_id as usize,
            wire_identifier_for_display(topic_id),
            wire_identifier_for_display(stream_id),
        ));
    }
    Err(streams.read(|inner| {
        let Some(resolved_stream) = inner.resolve_stream_id(stream_id) else {
            return stream_not_found(stream_id);
        };
        if inner.resolve_topic_id(resolved_stream, topic_id).is_none() {
            return topic_not_found(stream_id, topic_id);
        }
        // Unreachable while `topic_partition_ids` misses only on stream/topic;
        // kept as the safe generic rejection should that invariant drift.
        IggyError::InvalidIdentifier
    }))
}

/// Best-effort conversion for error payloads only: the wire reply carries just
/// the error code, so a failed conversion may fall back to a default without
/// changing what the client sees.
fn wire_identifier_for_display(id: &WireIdentifier) -> Identifier {
    match id {
        WireIdentifier::Numeric(numeric_id) => Identifier::numeric(*numeric_id),
        WireIdentifier::String(name) => Identifier::named(name.as_str()),
    }
    .unwrap_or_default()
}

/// Reject a consumer-group read whose parent stream/topic is absent with the
/// legacy typed error naming the level that missed; the group itself missing
/// stays the shared not-found reply (empty over TCP, 404 over HTTP).
pub fn ensure_topic_exists<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let resolved_stream = streams
            .resolve_stream_id(stream_id)
            .ok_or_else(|| stream_not_found(stream_id))?;
        streams
            .resolve_topic_id(resolved_stream, topic_id)
            .ok_or_else(|| topic_not_found(stream_id, topic_id))?;
        Ok(())
    })
}

/// Convert a `WireIdentifier` to the domain `Identifier`.
fn wire_id_to_identifier(wire: &WireIdentifier) -> Result<Identifier, IggyError> {
    match wire {
        WireIdentifier::Numeric(id) => Identifier::numeric(*id),
        WireIdentifier::String(name) => Identifier::named(name.as_str()),
    }
}

/// Typed miss for a read's parent stream, matching the legacy servers' error
/// shape. The identifier only feeds the error message; a wire form with no
/// domain equivalent (numeric 0 is a live slab id here but not a legacy id)
/// falls back to the default identifier.
fn stream_not_found(stream_id: &WireIdentifier) -> IggyError {
    IggyError::StreamIdNotFound(wire_id_to_identifier(stream_id).unwrap_or_default())
}

/// Typed miss for a read's parent topic; see [`stream_not_found`]. The variant's
/// display order is (topic, stream).
fn topic_not_found(stream_id: &WireIdentifier, topic_id: &WireIdentifier) -> IggyError {
    IggyError::TopicIdNotFound(
        wire_id_to_identifier(topic_id).unwrap_or_default(),
        wire_id_to_identifier(stream_id).unwrap_or_default(),
    )
}
