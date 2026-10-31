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
    CommitPolicy, Consumer, DecodeError, DeferredPollOptions, FetchedSection, Identifier,
    IggyError, IggyMessage, MessageClient, Partitioning, PollOptions, PolledMessages,
    PollingStrategy, SendMessagesResponse,
};
use crate::{
    PartitioningKind, TopicClient, binary_operation_deadline, calculate_32, with_binary_deadline,
    with_binary_lineage,
};
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::SEND_MESSAGES_CODE;
use iggy_binary_protocol::codes::SYNC_CONSUMER_GROUP_CODE;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::messages::{
    FetchMessagesRequest, FetchMessagesTarget, PollMessagesRequest, RawMessage, SendMessagesEncoder,
};
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use std::borrow::Cow;

/// The key of a group in [`crate::ConsumerGroupClientState`].
#[doc(hidden)]
pub fn group_cache_key(
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
) -> String {
    format!("{stream_id}|{topic_id}|{group_id}")
}

/// Sync the requesting member's assignment from the coordinator into the
/// transport cache. An empty reply means the client is not a member.
///
/// # Errors
/// Propagates transport and assignment-decoding failures.
#[doc(hidden)]
pub async fn sync_group_assignment<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
) -> Result<(), IggyError> {
    synced_group_assignment(client, stream_id, topic_id, group_id)
        .await
        .map(drop)
}

/// [`sync_group_assignment`], returning the generation and partitions it put in
/// the cache, or `None` when the client is not a member. A read of the cache
/// after it could see another call's change.
pub(crate) async fn synced_group_assignment<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
) -> Result<Option<(u64, Vec<u32>)>, IggyError> {
    with_binary_lineage(client.operation_lineage(), async {
        let state = client.consumer_group_state();
        let request = SyncConsumerGroupRequest {
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            group_id: identifier_to_wire(group_id)?,
        };
        let deadline = crate::binary_operation_deadline();
        loop {
            let generation = state.session_generation();
            let response = client
                .send_raw_with_response(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
                .await?;
            let assignment = if response.is_empty() {
                None
            } else {
                let (assignment, _) = SyncConsumerGroupResponse::decode(&response)
                    .map_err(|_| IggyError::InvalidCommand)?;
                Some((assignment.generation, assignment.partitions))
            };
            let applied = state.apply_group_assignment(
                generation,
                group_cache_key(stream_id, topic_id, group_id),
                (stream_id.clone(), topic_id.clone(), group_id.clone()),
                assignment.clone(),
            );
            match applied {
                // A successful read from the replaced session cannot publish its old membership.
                Err(IggyError::TransientNotAccepted) if tokio::time::Instant::now() < deadline => {}
                Err(error) => return Err(error),
                Ok(()) => return Ok(assignment),
            }
        }
    })
    .await
}

/// Re-sync every joined group's assignment from the coordinator. Heartbeat
/// driven so a member picks up a widened assignment (e.g. after a
/// partition-count change) without first hitting an ownership fence. A failed
/// per-group sync is logged. Sequential refreshes share the heartbeat's deadline.
pub(crate) async fn refresh_group_assignments<B: BinaryClient>(client: &B) {
    for (stream_id, topic_id, group_id) in client.consumer_group_state().registered_groups() {
        if let Err(error) = sync_group_assignment(client, &stream_id, &topic_id, &group_id).await {
            tracing::warn!(
                "Failed to refresh consumer-group assignment for {stream_id}|{topic_id}|{group_id}: {error}"
            );
        }
    }
}

/// The topic's partition count for client-side produce partitioning: the
/// count of a usable cached route, else a fresh one.
async fn topic_partition_count<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<u32, IggyError> {
    match client.routed_partition_count(stream_id, topic_id) {
        Some(count) => Ok(count),
        None => fresh_topic_partition_count(client, stream_id, topic_id).await,
    }
}

/// The partition count from a fresh read of the topic details. A send that
/// found its partition missing refreshes through it, because a cached route
/// can still count the partitions from before the change.
async fn fresh_topic_partition_count<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<u32, IggyError> {
    let details = TopicClient::get_topic(client, stream_id, topic_id)
        .await?
        .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
    Ok(details.partitions_count)
}

/// A chunk keeps its selected partition while an earlier lane blocker resolves.
/// Only a definitive missing-partition result permits one fresh selection.
#[doc(hidden)]
pub struct BinarySendPartitioning<'a> {
    original: &'a Partitioning,
    selected: Option<Cow<'a, Partitioning>>,
    refreshed: bool,
}

impl<'a> BinarySendPartitioning<'a> {
    pub fn new(original: &'a Partitioning) -> Self {
        Self {
            original,
            selected: None,
            refreshed: false,
        }
    }

    pub fn original(&self) -> &Partitioning {
        self.original
    }

    pub fn is_resolved(&self) -> bool {
        self.selected.is_some()
    }
}

#[doc(hidden)]
pub async fn send_binary_messages<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &mut BinarySendPartitioning<'_>,
    messages: &mut [IggyMessage],
) -> Result<SendMessagesResponse, IggyError> {
    with_binary_lineage(
        client.operation_lineage(),
        with_binary_deadline(binary_operation_deadline(), async {
            fail_if_not_authenticated(client).await?;
            if partitioning.selected.is_none() {
                partitioning.selected = Some(match partitioning.original.kind {
                    PartitioningKind::PartitionId => Cow::Borrowed(partitioning.original),
                    _ => {
                        let count = topic_partition_count(client, stream_id, topic_id).await?;
                        Cow::Owned(select_partition(
                            client,
                            stream_id,
                            topic_id,
                            partitioning.original,
                            count,
                        )?)
                    }
                });
            }
            loop {
                let selected = partitioning
                    .selected
                    .as_deref()
                    .ok_or(IggyError::InvalidCommand)?;
                let payload = encode_messages(stream_id, topic_id, selected, messages)?;
                match client
                    .send_raw_with_context(
                        SEND_MESSAGES_CODE,
                        payload,
                        crate::PartitionContext::default(),
                    )
                    .await
                {
                    Err(IggyError::PartitionNotFound(..))
                        if !partitioning.refreshed
                            && partitioning.original.kind != PartitioningKind::PartitionId =>
                    {
                        partitioning.refreshed = true;
                        let count =
                            fresh_topic_partition_count(client, stream_id, topic_id).await?;
                        partitioning.selected = Some(Cow::Owned(select_partition(
                            client,
                            stream_id,
                            topic_id,
                            partitioning.original,
                            count,
                        )?));
                    }
                    result => {
                        return result.map(|response| committed_send_confirmations(&response));
                    }
                }
            }
        }),
    )
    .await
}

fn select_partition<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
    count: u32,
) -> Result<Partitioning, IggyError> {
    if count == 0 {
        return Err(IggyError::TopicIdNotFound(
            topic_id.clone(),
            stream_id.clone(),
        ));
    }
    let partition = match partitioning.kind {
        PartitioningKind::Balanced => client
            .consumer_group_state()
            .next_balanced_partition(stream_id, topic_id, count),
        PartitioningKind::MessagesKey => calculate_32(&partitioning.value) % count,
        PartitioningKind::PartitionId => return Err(IggyError::InvalidCommand),
    };
    Ok(Partitioning::partition_id(partition))
}

/// Read up to `count` messages from several partitions in one request to their primary, and
/// return one section per answered target, in reply order.
///
/// The targets are one readiness group: partitions with one primary, as the SDK groups them.
/// Each start point is [`PollingStrategy::offset`] or [`PollingStrategy::next`] of `consumer`,
/// and each partition appears once. A continuation fences its partition with its context and
/// must belong to its target. A target without a context takes the context of its route. The
/// server waits up to `options.max_wait` until the targets hold `options.min_count` messages.
/// `count` bounds the messages of the whole reply. `options.max_bytes` bounds the records of
/// the whole reply and of each section. The section table adds 4 bytes and 56 bytes per
/// target. `options.request_timeout` must exceed `max_wait` by
/// [`MIN_DEFERRED_POLL_REPLY_MARGIN_US`](crate::MIN_DEFERRED_POLL_REPLY_MARGIN_US), also for a
/// zero wait.
///
/// A section serves messages and the offset where the next fetch of its partition starts, or
/// holds the refusal of its partition, such as `NotPrimary` after a move. A target without a
/// section keeps its position.
///
/// A fetch with `auto_commit` or a `next` target waits for the offset reads, stores and
/// deletes of the client on its partitions. A store or delete with an unknown outcome blocks
/// it with [`IggyError::PartitionLaneBlocked`], which names the partition. A committing fetch
/// with an unknown outcome returns [`IggyError::FetchProgressUncertain`] and keeps all
/// affected progress lanes blocked until explicit `Client::abandon_operation`. The local
/// recovery identity survives reconnect and session replacement. Resolution never replays
/// the fetch, and abandonment accepts uncertainty without stopping the server request.
/// GetOffset and stateless reads remain usable. A partial reply keeps healthy sections and
/// attaches the recovery error to uncertain sections. Record decoding and decryption failures
/// of known healthy sections retain their ordinary rewind behavior. A noncommitting `next`
/// fetch still ends with `TransientNotCommitted` without retaining a progress block.
///
/// Do not drop the future of a fetch with `auto_commit`. TCP and WebSocket then send a cancel,
/// but a cancel cannot undo a fetch that the server already served: serving it committed the
/// offsets, so the client loses the served messages. Await such a fetch until it ends, also at
/// shutdown. It ends within `options.request_timeout`.
/// Dropping it after possible send retains the block, and the next progress call names its
/// partition and recovery identity with `PartitionLaneBlocked`.
///
/// # Errors
/// The errors of [`DeferredPollOptions::validate_fetch`], [`IggyError::InvalidCommand`] for an
/// invalid target list or a continuation of another partition, and the errors of the send.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn fetch_binary_messages<B: BinaryClient>(
    client: &B,
    stream_id: &Identifier,
    topic_id: &Identifier,
    targets: &[(u32, PollingStrategy)],
    consumer: &Consumer,
    count: u32,
    auto_commit: bool,
    options: DeferredPollOptions,
) -> Result<Vec<FetchedSection>, IggyError> {
    options.validate_fetch(count)?;
    let request = fetch_messages_request(
        stream_id,
        topic_id,
        targets,
        consumer,
        count,
        auto_commit,
        options,
    )?;
    let deadline = tokio::time::Instant::now()
        .checked_add(options.request_timeout.get_duration())
        .ok_or(IggyError::InvalidCommand)?;
    with_binary_lineage(
        client.operation_lineage(),
        with_binary_deadline(deadline, async {
            fail_if_not_authenticated(client).await?;
            client.send_fetch_sections(&request).await
        }),
    )
    .await
}

/// The wire form of a fetch. A target without a context gets incarnation
/// zero, which the transport replaces with the incarnation of its route.
fn fetch_messages_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    targets: &[(u32, PollingStrategy)],
    consumer: &Consumer,
    count: u32,
    auto_commit: bool,
    options: DeferredPollOptions,
) -> Result<FetchMessagesRequest, IggyError> {
    let request = FetchMessagesRequest {
        consumer: consumer_to_wire(consumer)?,
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        wait_us: options.max_wait.as_micros(),
        request_timeout_us: options.request_timeout.as_micros(),
        min_count: options.min_count,
        count,
        max_bytes: options.max_bytes,
        auto_commit,
        targets: targets
            .iter()
            .map(|(partition_id, strategy)| {
                strategy.validate_partition(*partition_id)?;
                let context = strategy.context().unwrap_or_default();
                Ok(FetchMessagesTarget {
                    partition_id: *partition_id,
                    strategy: polling_strategy_to_wire(strategy),
                    // The server allots the budget by the bytes that it knows
                    // a target holds, and a smaller cap would slice batches.
                    max_bytes: options.max_bytes,
                    incarnation: context.incarnation,
                    owner_generation: context.owner_generation,
                })
            })
            .collect::<Result<_, IggyError>>()?,
    };
    request.validate().map_err(|_| IggyError::InvalidCommand)?;
    Ok(request)
}

/// Map a raw `SendMessages` reply body to its confirmation payload. An empty
/// body means the batch was accepted but no offsets were reported: the legacy
/// server answers that way, so absence must never surface as a decode failure.
///
/// Absence is reported as an empty list, never as a zeroed entry. Every field
/// of a confirmation has 0 as a legitimate value (ids are 0-based slab keys,
/// the first batch of a partition commits at offset 0), so a synthetic entry
/// would be indistinguishable from a real one and a caller checkpointing
/// `base_offset` would record a commit that never happened.
pub fn decode_send_confirmations(response: &[u8]) -> Result<SendMessagesResponse, IggyError> {
    if response.is_empty() {
        return Ok(SendMessagesResponse {
            confirmations: Vec::new(),
        });
    }
    super::decode_response::<SendMessagesResponse>(response)
}

/// Confirmations for a batch the server has already committed.
///
/// An unreadable body degrades to no confirmations instead of an error. The
/// producer retry loop filters nothing and resends on any `Err`, so failing
/// here would resend a committed write under a new request id, outside the
/// partition's retry deduplication. A zeroed entry would be indistinguishable
/// from a genuine commit at offset 0 and would checkpoint the shape mismatch.
fn committed_send_confirmations(response: &[u8]) -> SendMessagesResponse {
    decode_send_confirmations(response).unwrap_or_else(|_| SendMessagesResponse {
        confirmations: Vec::new(),
    })
}

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, IggyError> {
        self.poll_messages_raw(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            strategy,
            options,
        )
        .await
        .map_err(|error| error.error)
    }

    async fn poll_messages_raw(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, DecodeError> {
        let auto_commit = match options.commit {
            CommitPolicy::Manual => false,
            CommitPolicy::OnRead => true,
            CommitPolicy::Auto(_) => {
                return Err(DecodeError::without_record(IggyError::InvalidConfiguration));
            }
        };
        options.validate().map_err(DecodeError::without_record)?;
        strategy
            .validate_partition(partition_id)
            .map_err(DecodeError::without_record)?;
        let deadline = tokio::time::Instant::now()
            .checked_add(options.request_timeout().get_duration())
            .ok_or(IggyError::InvalidCommand)
            .map_err(DecodeError::without_record)?;
        let response = with_binary_lineage(
            self.operation_lineage(),
            with_binary_deadline(deadline, async {
                fail_if_not_authenticated(self).await?;
                let request = PollMessagesRequest {
                    consumer: consumer_to_wire(consumer)?,
                    stream_id: identifier_to_wire(stream_id)?,
                    topic_id: identifier_to_wire(topic_id)?,
                    partition_id: Some(partition_id),
                    strategy: polling_strategy_to_wire(strategy),
                    count: options.count,
                    auto_commit,
                };
                let response = self
                    .send_poll_with_response_and_options(
                        &request,
                        strategy.context(),
                        Some(options.into()),
                    )
                    .await?;
                Ok::<Bytes, IggyError>(response)
            }),
        )
        .await
        .map_err(DecodeError::without_record)?;
        PolledMessages::decode(response, strategy)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        send_binary_messages(
            self,
            stream_id,
            topic_id,
            &mut BinarySendPartitioning::new(partitioning),
            messages,
        )
        .await
    }
}

fn encode_messages(
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
    messages: &mut [IggyMessage],
) -> Result<Bytes, IggyError> {
    let wire_stream_id = identifier_to_wire(stream_id)?;
    let wire_topic_id = identifier_to_wire(topic_id)?;
    let wire_partitioning = partitioning_to_wire(partitioning)?;
    // The producer owns message ids now that batches ride the wire
    // verbatim: a zero id is minted here, before the frame checksum
    // covers it.
    for message in messages.iter_mut() {
        if message.header.id == 0 {
            message.header.id = crate::utils::random_id::get_uuid();
        }
    }
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
    )
    .map_err(|error| match error {
        iggy_binary_protocol::WireError::InvalidMessageTimestampDelta(delta) => {
            IggyError::InvalidMessageTimestampDelta(delta)
        }
        _ => IggyError::InvalidCommand,
    })?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::{committed_send_confirmations, decode_send_confirmations};
    use crate::{IggyError, SendMessagesConfirmationResponse, SendMessagesResponse};
    use iggy_binary_protocol::codec::WireEncode;

    fn response() -> SendMessagesResponse {
        SendMessagesResponse {
            confirmations: vec![SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 2,
                partition_id: 3,
                base_offset: 42,
            }],
        }
    }

    /// The legacy server reports nothing at all, and nothing is what the caller
    /// must see: no error to retry on, and no entry that reads as a commit at
    /// offset 0.
    #[test]
    fn empty_body_is_no_confirmations() {
        let decoded = decode_send_confirmations(&[]).expect("empty body must not fail");
        assert!(decoded.confirmations.is_empty());
    }

    #[test]
    fn populated_body_decodes() {
        let expected = response();
        let bytes = expected.to_bytes();
        let decoded = decode_send_confirmations(&bytes).expect("valid payload must decode");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn zero_count_body_decodes_to_empty_list() {
        let bytes = SendMessagesResponse {
            confirmations: vec![],
        }
        .to_bytes();
        let decoded = decode_send_confirmations(&bytes).expect("zero-count payload must decode");
        assert!(decoded.confirmations.is_empty());
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = response().to_bytes().to_vec();
        bytes.push(0xFF);
        assert!(matches!(
            decode_send_confirmations(&bytes),
            Err(IggyError::InvalidFormat)
        ));
    }

    #[test]
    fn truncated_body_is_rejected() {
        let bytes = response().to_bytes();
        for length in 1..bytes.len() {
            assert!(
                matches!(
                    decode_send_confirmations(&bytes[..length]),
                    Err(IggyError::InvalidFormat)
                ),
                "expected error for truncation at byte {length}"
            );
        }
    }

    #[test]
    fn committed_body_keeps_reported_confirmations() {
        let expected = response();
        assert_eq!(committed_send_confirmations(&expected.to_bytes()), expected);
    }

    /// The write is already durable once the reply arrives, so an unreadable
    /// body degrades to no confirmations. Anything else either resends a
    /// committed batch or hands the caller a fabricated offset.
    #[test]
    fn committed_malformed_body_is_no_confirmations() {
        let valid = response().to_bytes();

        let mut with_tail = valid.to_vec();
        with_tail.push(0xFF);
        let degraded = committed_send_confirmations(&with_tail);
        assert!(degraded.confirmations.is_empty());

        for length in 1..valid.len() {
            let degraded = committed_send_confirmations(&valid[..length]);
            assert!(
                degraded.confirmations.is_empty(),
                "expected no confirmations for truncation at byte {length}"
            );
        }
    }
}
