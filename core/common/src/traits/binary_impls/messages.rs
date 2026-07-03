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

use crate::BinaryClient;
use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{
    consumer_to_wire, identifier_to_wire, partitioning_to_wire, polling_strategy_to_wire,
};
use crate::{
    Consumer, Identifier, IggyError, IggyMessage, MessageClient, Partitioning, PolledMessages,
    PollingStrategy,
};
#[cfg(feature = "vsr")]
use crate::{ConsumerKind, PartitioningKind, TopicClient, calculate_32};
use bytes::BytesMut;
#[cfg(feature = "vsr")]
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::codec::WireEncode;
#[cfg(feature = "vsr")]
use iggy_binary_protocol::codes::SYNC_CONSUMER_GROUP_CODE;
use iggy_binary_protocol::codes::{
    FLUSH_UNSAVED_BUFFER_CODE, POLL_MESSAGES_CODE, SEND_MESSAGES_CODE,
};
#[cfg(feature = "vsr")]
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::messages::{
    FlushUnsavedBufferRequest, PollMessagesRequest, RawMessage, SendMessagesEncoder,
};
#[cfg(feature = "vsr")]
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use std::time::Duration;

/// Max attempts to resolve a fenced consumer-group poll: one re-sync after the
/// coordinator rejects a stale assignment, then retry once.
#[cfg(feature = "vsr")]
const GROUP_POLL_MAX_ATTEMPTS: usize = 2;

fn duration_to_wait_timeout_us(wait_timeout: Duration) -> Result<u64, IggyError> {
    wait_timeout
        .as_micros()
        .try_into()
        .map_err(|_| IggyError::InvalidNumberValue)
}

#[cfg(feature = "vsr")]
fn group_cache_key(stream_id: &Identifier, topic_id: &Identifier, group_id: &Identifier) -> String {
    format!("{stream_id}|{topic_id}|{group_id}")
}

#[cfg(feature = "vsr")]
fn topic_cache_key(stream_id: &Identifier, topic_id: &Identifier) -> String {
    format!("{stream_id}|{topic_id}")
}

/// Sync the requesting member's assignment from the coordinator into the
/// transport cache. An empty reply means the member holds no partitions.
#[cfg(feature = "vsr")]
async fn sync_group_assignment<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
) -> Result<(), IggyError> {
    let request = SyncConsumerGroupRequest {
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        group_id: identifier_to_wire(group_id)?,
    };
    let response = client
        .send_raw_with_response(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
        .await?;
    let key = group_cache_key(stream_id, topic_id, group_id);
    if response.is_empty() {
        // Empty reply = the client is not a member of this group (or holds no
        // partitions). Record the empty assignment but do NOT `register_group`:
        // registering would make the heartbeat re-sync it for the connection's
        // lifetime and leak a `joined_groups` entry per distinct non-member
        // group ever polled. Only a real membership (non-empty reply) registers.
        client
            .consumer_group_state()
            .set_assignment(key, 0, Vec::new());
        return Ok(());
    }
    let (assignment, _) =
        SyncConsumerGroupResponse::decode(&response).map_err(|_| IggyError::InvalidCommand)?;
    client.consumer_group_state().register_group(
        key.clone(),
        stream_id.clone(),
        topic_id.clone(),
        group_id.clone(),
    );
    client
        .consumer_group_state()
        .set_assignment(key, assignment.generation, assignment.partitions);
    Ok(())
}

/// Re-sync every joined group's assignment from the coordinator. Heartbeat
/// driven so a member picks up a widened assignment (e.g. after a
/// partition-count change) without first hitting an ownership fence. A failed
/// per-group sync is logged and skipped so one bad group can't stall the rest.
#[cfg(feature = "vsr")]
pub(crate) async fn refresh_group_assignments<B: BinaryClient>(client: &B) {
    for (stream_id, topic_id, group_id) in client.consumer_group_state().registered_groups() {
        if let Err(error) = sync_group_assignment(client, &stream_id, &topic_id, &group_id).await {
            tracing::warn!(
                "Failed to refresh consumer-group assignment for {stream_id}|{topic_id}|{group_id}: {error}"
            );
        }
    }
}

/// Resolve (and cache) the topic's partition count for client-side produce
/// partitioning.
#[cfg(feature = "vsr")]
async fn topic_partition_count<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<u32, IggyError> {
    let key = topic_cache_key(stream_id, topic_id);
    if let Some(count) = client.consumer_group_state().partition_count(&key) {
        return Ok(count);
    }
    let details = TopicClient::get_topic(client, stream_id, topic_id)
        .await?
        .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
    client
        .consumer_group_state()
        .set_partition_count(key, details.partitions_count);
    Ok(details.partitions_count)
}

/// Resolve `Balanced` / `MessagesKey` to an explicit `PartitionId` client-side
/// (the VSR broker only routes explicit partitions, matching Kafka).
#[cfg(feature = "vsr")]
async fn resolve_partitioning<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
) -> Result<Partitioning, IggyError> {
    match partitioning.kind {
        PartitioningKind::PartitionId => Ok(partitioning.clone()),
        PartitioningKind::Balanced => {
            let count = topic_partition_count(client, stream_id, topic_id).await?;
            if count == 0 {
                return Err(IggyError::TopicIdNotFound(
                    topic_id.clone(),
                    stream_id.clone(),
                ));
            }
            let key = topic_cache_key(stream_id, topic_id);
            let partition = client
                .consumer_group_state()
                .next_balanced_partition(&key, count);
            Ok(Partitioning::partition_id(partition))
        }
        PartitioningKind::MessagesKey => {
            let count = topic_partition_count(client, stream_id, topic_id).await?;
            if count == 0 {
                return Err(IggyError::TopicIdNotFound(
                    topic_id.clone(),
                    stream_id.clone(),
                ));
            }
            let partition = calculate_32(&partitioning.value) % count;
            Ok(Partitioning::partition_id(partition))
        }
    }
}

/// Poll a consumer group: select one of the member's assigned partitions
/// (round-robin) and send an explicit-partition poll. A coordinator fence
/// rejection (stale assignment after a rebalance) triggers one re-sync + retry.
#[cfg(feature = "vsr")]
#[allow(clippy::too_many_arguments)]
async fn poll_group_messages<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer: &Consumer,
    strategy: &PollingStrategy,
    count: u32,
    auto_commit: bool,
    wait_timeout_us: u64,
) -> Result<PolledMessages, IggyError> {
    let key = group_cache_key(stream_id, topic_id, &consumer.id);
    if !client.consumer_group_state().has_assignment(&key) {
        sync_group_assignment(client, stream_id, topic_id, &consumer.id).await?;
    }
    for _ in 0..GROUP_POLL_MAX_ATTEMPTS {
        let Some(partition_id) = client.consumer_group_state().next_group_partition(&key) else {
            // Synced but holds no partitions: not a member, or none assigned.
            return Ok(PolledMessages::empty());
        };
        let request = PollMessagesRequest {
            consumer: consumer_to_wire(consumer)?,
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            partition_id: Some(partition_id),
            strategy: polling_strategy_to_wire(strategy),
            count,
            auto_commit,
            wait_timeout_us,
        };
        match client
            .send_raw_with_response(POLL_MESSAGES_CODE, request.to_bytes())
            .await
        {
            Ok(response) => {
                let polled = PolledMessages::from_bytes(response)?;
                // The coordinator can't yet signal a generation fence as a typed
                // error (no reply-header status), so it rides the empty-poll body
                // as a sentinel partition id. Re-sync and retry, same as the
                // typed error below; a genuine empty poll echoes the real id.
                if polled.messages.is_empty()
                    && polled.partition_id == crate::RESYNC_REQUIRED_PARTITION_SENTINEL
                {
                    client.consumer_group_state().invalidate_assignment(&key);
                    sync_group_assignment(client, stream_id, topic_id, &consumer.id).await?;
                    continue;
                }
                return Ok(polled);
            }
            Err(IggyError::ConsumerGroupPartitionNotOwned(..)) => {
                client.consumer_group_state().invalidate_assignment(&key);
                sync_group_assignment(client, stream_id, topic_id, &consumer.id).await?;
            }
            Err(error) => return Err(error),
        }
    }
    // Exhausted the retry budget on back-to-back fences (a rebalance landed on
    // every attempt) -- rare, and the cursor is already re-synced. Surface an
    // empty poll rather than `ConsumerGroupPartitionNotOwned(0, 0)`: the (0, 0)
    // ids are fabricated and a normal rebalance must not look like a hard error
    // to a CG app that doesn't special-case 5009. The caller just re-polls.
    Ok(PolledMessages::empty())
}

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        self.poll_messages_with_timeout(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            strategy,
            count,
            auto_commit,
            Duration::ZERO,
        )
        .await
    }

    async fn poll_messages_with_timeout(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
        wait_timeout: Duration,
    ) -> Result<PolledMessages, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wait_timeout_us = duration_to_wait_timeout_us(wait_timeout)?;
        // VSR: a consumer-group poll without an explicit partition is resolved
        // client-side from the member's cached assignment (the broker routes
        // explicit partitions only).
        #[cfg(feature = "vsr")]
        if consumer.kind == ConsumerKind::ConsumerGroup && partition_id.is_none() {
            return poll_group_messages(
                self,
                stream_id,
                topic_id,
                consumer,
                strategy,
                count,
                auto_commit,
                wait_timeout_us,
            )
            .await;
        }
        let req = PollMessagesRequest {
            consumer: consumer_to_wire(consumer)?,
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            partition_id,
            strategy: polling_strategy_to_wire(strategy),
            count,
            auto_commit,
            wait_timeout_us,
        };
        let response = self
            .send_raw_with_response(POLL_MESSAGES_CODE, req.to_bytes())
            .await?;
        PolledMessages::from_bytes(response)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        // VSR: resolve Balanced/MessagesKey to an explicit partition client-side.
        // An explicit `PartitionId` needs no resolution, so borrow the input
        // directly on that fast path instead of cloning its `value: Vec<u8>`.
        #[cfg(feature = "vsr")]
        let resolved_partitioning;
        #[cfg(feature = "vsr")]
        let partitioning = if partitioning.kind == PartitioningKind::PartitionId {
            partitioning
        } else {
            resolved_partitioning =
                resolve_partitioning(self, stream_id, topic_id, partitioning).await?;
            &resolved_partitioning
        };
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_partitioning = partitioning_to_wire(partitioning)?;
        let raw_messages: Vec<RawMessage<'_>> = messages
            .iter()
            .map(|m| RawMessage {
                id: m.header.id,
                origin_timestamp: m.header.origin_timestamp,
                headers: m.user_headers.as_deref(),
                payload: &m.payload,
            })
            .collect();
        let size = SendMessagesEncoder::encoded_size(
            &wire_stream_id,
            &wire_topic_id,
            &wire_partitioning,
            &raw_messages,
        );
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(
            &mut buf,
            &wire_stream_id,
            &wire_topic_id,
            &wire_partitioning,
            &raw_messages,
        );
        self.send_raw_with_response(SEND_MESSAGES_CODE, buf.freeze())
            .await?;
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let req = FlushUnsavedBufferRequest {
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            partition_id,
            fsync,
        };
        self.send_raw_with_response(FLUSH_UNSAVED_BUFFER_CODE, req.to_bytes())
            .await?;
        Ok(())
    }
}
