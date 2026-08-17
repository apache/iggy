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

use crate::{
    Consensus, IgnoreReason, Pipeline, PipelineEntry, PlaneKind, PrepareOkOutcome, Sequencer,
    Status, VsrConsensus,
};
use iggy_binary_protocol::{
    CHECKSUM_UNSEALED, Command, ConsensusHeader, GenericHeader, PrepareHeader, PrepareOkHeader,
    ReplyHeader, RoutedRequestHeader, frame_body,
};
use message_bus::{MessageBus, SendError};
use server_common::{
    MESSAGE_ALIGN, Message,
    iobuf::{Frozen, Owned},
};
use std::{error::Error, fmt, mem::size_of, ops::AsyncFnOnce};

/// Failure to route or forward a prepare through the replication chain.
#[derive(Debug)]
#[non_exhaustive]
pub enum ChainReplicationError {
    MalformedPrepare,
    UnexpectedCommand { command: Command },
    CommittedPrepare { op: u64, commit_min: u64 },
    SelfRoute { replica: u8 },
    Transport(SendError),
}

impl ChainReplicationError {
    #[must_use]
    pub const fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

impl fmt::Display for ChainReplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedPrepare => formatter.write_str("malformed prepare frame"),
            Self::UnexpectedCommand { command } => {
                write!(formatter, "expected prepare command, found {command:?}")
            }
            Self::CommittedPrepare { op, commit_min } => write!(
                formatter,
                "prepare op {op} is not above committed op {commit_min}"
            ),
            Self::SelfRoute { replica } => {
                write!(
                    formatter,
                    "replication chain routes replica {replica} to itself"
                )
            }
            Self::Transport(error) => error.fmt(formatter),
        }
    }
}

impl Error for ChainReplicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SendError> for ChainReplicationError {
    fn from(error: SendError) -> Self {
        Self::Transport(error)
    }
}

/// Shared pipeline-first request flow (metadata + partitions).
///
/// # Panics
/// If not primary, status not Normal, or syncing.
#[allow(clippy::future_not_send)]
pub async fn pipeline_prepare_common<C, F>(
    consensus: &C,
    plane: PlaneKind,
    prepare: C::Message<C::ReplicateHeader>,
    on_replicate: F,
) where
    C: Consensus,
    F: AsyncFnOnce(C::Message<C::ReplicateHeader>) -> (),
{
    assert!(!consensus.is_follower(), "on_request: primary only");
    assert!(consensus.is_normal(), "on_request: status must be normal");
    assert!(
        !consensus.is_transferring(),
        "on_request: must not be transferring state"
    );

    consensus.verify_pipeline();
    consensus.pipeline_message(plane, &prepare);
    on_replicate(prepare).await;
}

/// Shared commit-based old-prepare fence.
///
/// Uses `commit_min` (locally executed), not `commit_max`. A backup may know
/// that op 50 is committed (`commit_max = 50`) but only have executed up to
/// op 14 (`commit_min = 14`). A retransmitted prepare for op 15 must NOT be
/// fenced out, the backup still needs it in the WAL for `commit_journal`.
#[must_use]
pub const fn fence_old_prepare_by_commit<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    header.op <= consensus.commit_min()
}

/// Shared chain-replication forwarding to the next replica.
///
/// Borrows the message, makes a deep copy for the wire, and lets the caller
/// retain ownership for journal append.
///
/// # Errors
///
/// Returns an error if the prepare cannot be routed or the bus cannot deliver
/// it to the next replica.
/// Callers decide error policy (VSR retransmits from WAL via prepare timeout).
#[allow(clippy::future_not_send)]
pub async fn replicate_to_next_in_chain<B, P>(
    consensus: &VsrConsensus<B, P>,
    message: &Message<PrepareHeader>,
) -> Result<(), ChainReplicationError>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let Some(next) = replication_target(consensus, message.header())? else {
        return Ok(());
    };
    let frozen = message.deep_copy().into_generic().into_frozen();
    consensus
        .message_bus()
        .send_to_replica(next, frozen)
        .await
        .map_err(Into::into)
}

/// Forward an already validated frozen prepare to the next replica without
/// copying its payload.
///
/// # Errors
///
/// Returns an error if the frame is malformed, cannot be routed, or the bus
/// cannot deliver it to the next replica.
#[allow(clippy::future_not_send)]
pub async fn replicate_frozen_to_next_in_chain<B, P>(
    consensus: &VsrConsensus<B, P>,
    message: Frozen<MESSAGE_ALIGN>,
) -> Result<(), ChainReplicationError>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header = frozen_prepare_header(&message)?;
    let Some(next) = replication_target(consensus, &header)? else {
        return Ok(());
    };
    consensus
        .message_bus()
        .send_to_replica(next, message)
        .await
        .map_err(Into::into)
}

fn frozen_prepare_header(
    message: &Frozen<MESSAGE_ALIGN>,
) -> Result<PrepareHeader, ChainReplicationError> {
    let header_bytes = message
        .as_slice()
        .get(..size_of::<PrepareHeader>())
        .ok_or(ChainReplicationError::MalformedPrepare)?;
    let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
        .copied()
        .map_err(|_| ChainReplicationError::MalformedPrepare)?;
    header
        .validate()
        .map_err(|_| ChainReplicationError::MalformedPrepare)?;
    let frame_size =
        usize::try_from(header.size).map_err(|_| ChainReplicationError::MalformedPrepare)?;
    if !(size_of::<PrepareHeader>()..=message.len()).contains(&frame_size) {
        return Err(ChainReplicationError::MalformedPrepare);
    }
    Ok(header)
}

fn replication_target<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> Result<Option<u8>, ChainReplicationError>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    if header.command != Command::Prepare {
        return Err(ChainReplicationError::UnexpectedCommand {
            command: header.command,
        });
    }
    let commit_min = consensus.commit_min();
    if header.op <= commit_min {
        return Err(ChainReplicationError::CommittedPrepare {
            op: header.op,
            commit_min,
        });
    }

    let next = (consensus.replica() + 1) % consensus.replica_count();
    let primary = consensus.primary_index(header.view);

    if next == primary {
        return Ok(None);
    }

    if next == consensus.replica() {
        return Err(ChainReplicationError::SelfRoute {
            replica: consensus.replica(),
        });
    }
    Ok(Some(next))
}

/// Re-stamp a stored prepare with the current view before retransmission.
/// The prepare identity excludes `view`, so the payload and operation identity
/// remain unchanged.
#[must_use]
pub fn restamp_prepare_view(
    stored: Frozen<MESSAGE_ALIGN>,
    view: u32,
) -> Option<Frozen<MESSAGE_ALIGN>> {
    const VIEW_OFFSET: usize = std::mem::offset_of!(PrepareHeader, view);

    let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
        stored.as_slice().get(..size_of::<PrepareHeader>())?,
    )
    .ok()?;
    if header.view == view {
        return Some(stored);
    }

    let mut owned = Owned::<MESSAGE_ALIGN>::copy_from_slice(stored.as_slice());
    owned.as_mut_slice()[VIEW_OFFSET..VIEW_OFFSET + size_of::<u32>()]
        .copy_from_slice(&view.to_ne_bytes());
    Message::<GenericHeader>::try_from(owned)
        .ok()
        .map(Message::into_frozen)
}

/// Recompute a prepare's integrity fields and report the first that disagrees.
///
/// Everywhere else `checksum` is an opaque token: the pipeline, the merge, and the
/// repair ingest compare it for equality without asking whether it describes the
/// bytes it arrived with, so a corrupted frame is admitted whenever its flipped
/// value satisfies those comparisons, then journaled and re-served to peers.
///
/// `frame` is the whole message. The body range comes from [`frame_body`], not the
/// caller, so no ingress point can verify a different span than the producer sealed.
/// [`CHECKSUM_UNSEALED`] skips the partition plane, which carries `batch_checksum`
/// over the same bytes instead.
///
/// # Errors
/// Returns a static description of which field failed.
pub fn verify_prepare_integrity(header: &PrepareHeader, frame: &[u8]) -> Result<(), &'static str> {
    if header.checksum != CHECKSUM_UNSEALED && header.identity_checksum() != header.checksum {
        return Err("prepare header does not match its own checksum");
    }
    if header.checksum_body != 0
        && u128::from(iggy_common::calculate_checksum(frame_body(
            frame,
            header.size,
        ))) != header.checksum_body
    {
        return Err("prepare body does not match its checksum");
    }
    Ok(())
}

/// Shared preflight checks for `on_replicate`.
///
/// Returns current op on success.
///
/// # Errors
/// Returns a static error string if the replica is syncing, not in normal
/// status, or the message's view differs from the replica's.
///
/// # Panics
/// If `header.command` is not `Command::Prepare`.
pub fn replicate_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> Result<u64, IgnoreReason>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command::Prepare);

    if consensus.is_transferring() {
        return Err(IgnoreReason::StateTransfer);
    }

    let current_op = consensus.sequencer().current_sequence();

    if consensus.status() != Status::Normal {
        return Err(IgnoreReason::NotNormal);
    }

    if header.view > consensus.view() {
        // Dropped, but recorded: a newer-view prepare is proof the cluster
        // moved past this replica, so the heartbeat-timeout handler probes to
        // catch up rather than starting a futile election.
        consensus.observe_newer_view(header.view);
        return Err(IgnoreReason::NewerView);
    }

    // Deposed-primary prepares can still be in flight after a view change:
    // replicate_to_next_in_chain stops the ring at primary_index(header.view),
    // not the current view's primary, so a stale prepare reaches the new
    // primary and would hit its sequencer invariants. Uncommitted old-view
    // ops are decided by the view change, never by late delivery. Message
    // repair, once it lands, fetches prepares via its own path, not here.
    if header.view < consensus.view() {
        return Err(IgnoreReason::OlderView);
    }

    if consensus.is_follower() {
        consensus.advance_commit_max(header.commit);
    }

    Ok(current_op)
}

/// Stamp [`PrepareHeader::identity_checksum`] into a freshly built prepare.
///
/// Call once, after every other field is final: the checksum covers them.
/// `checksum_body` in particular, since that is how the body reaches the value.
///
/// # Panics
/// If the message is shorter than its own header.
#[must_use]
pub fn seal_prepare_checksum(mut message: Message<PrepareHeader>) -> Message<PrepareHeader> {
    let checksum = message.header().identity_checksum();
    let bytes = &mut message.as_mut_slice()[..size_of::<PrepareHeader>()];
    let header = bytemuck::checked::try_from_bytes_mut::<PrepareHeader>(bytes)
        .expect("a prepare header round-trips its own bit pattern");
    header.checksum = checksum;
    message
}

/// Shared preflight checks for `on_ack`.
///
/// # Errors
/// Returns a static error string if the replica is not primary or not in
/// normal status.
pub fn ack_preflight<B, P>(consensus: &VsrConsensus<B, P>) -> Result<(), IgnoreReason>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    if !consensus.is_primary() {
        return Err(IgnoreReason::NotPrimary);
    }

    if consensus.status() != Status::Normal {
        return Err(IgnoreReason::NotNormal);
    }

    Ok(())
}

/// Shared quorum tracking flow for ack handling.
///
/// After recording the ack, walks forward from `current_commit + 1` advancing
/// the commit number only while consecutive ops have achieved quorum. This
/// prevents committing ops that have gaps in quorum acknowledgment.
pub fn ack_quorum_reached<B, P>(
    consensus: &VsrConsensus<B, P>,
    plane: PlaneKind,
    ack: &PrepareOkHeader,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    if !matches!(
        consensus.handle_prepare_ok(plane, ack),
        PrepareOkOutcome::Accepted {
            quorum_reached: true,
            ..
        }
    ) {
        return false;
    }

    let mut new_commit = consensus.commit_max();
    consensus.with_pipeline(|pipeline| {
        while let Some(entry) = pipeline.entry_by_op(new_commit + 1) {
            if !entry.ok_quorum_received {
                break;
            }
            new_commit += 1;
        }
    });

    if new_commit > consensus.commit_max() {
        consensus.advance_commit_max(new_commit);
        return true;
    }

    false
}

/// Drain and return committable prepares from the pipeline head.
///
/// Entries are drained only from the head and only while their op is covered
/// by the current commit frontier.
///
/// # Panics
/// If `head()` returns `Some` but `pop()` returns `None` (unreachable).
pub fn drain_committable_prefix<B, P>(consensus: &VsrConsensus<B, P>) -> Vec<PipelineEntry>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let commit = consensus.commit_max();
    let mut drained = Vec::new();

    consensus.with_pipeline_mut(|pipeline| {
        while let Some(head_op) = pipeline.head().map(|entry| entry.header.op) {
            if head_op > commit {
                break;
            }

            let entry = pipeline
                .pop()
                .expect("drain_committable_prefix: head exists");
            drained.push(entry);
        }
    });

    // Popping through the pipeline directly bypasses
    // `VsrConsensus::pop_committed_prepare`, so re-establish the prepare
    // timeout's ticking-iff-non-empty invariant here: an emptied pipeline
    // disarms it, and a remaining head becomes the entry the timer measures,
    // timed from now rather than inheriting the drained entry's elapsed ticks.
    if !drained.is_empty() {
        consensus.sync_prepare_timeout();
    }

    drained
}

/// Header of the pipeline head, iff its op is covered by the commit frontier.
///
/// Peek-only counterpart of [`drain_committable_prefix`] for commit paths that
/// must survive their driving future being canceled between "committable" and
/// "applied" (see `IggyMetadata::on_ack`): the caller peeks here, performs its
/// awaits with the entry still in the pipeline, then — in a sync region —
/// revalidates that the head is still this exact entry before popping and
/// applying it. A driver dropped at an await strands nothing; a sibling driver
/// that committed the op first fails the caller's revalidation and re-peeks.
pub fn peek_committable_head<B, P>(consensus: &VsrConsensus<B, P>) -> Option<PrepareHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let commit = consensus.commit_max();
    consensus
        .pipeline_head_header()
        .filter(|header| header.op <= commit)
}

/// Build reply for a committed prepare.
///
/// Every field except `size` comes from `prepare_header`, bytes are identical
/// across replicas regardless of when they commit (primary inline, backup via
/// `commit_journal`, promoted-primary via `register_preflight::AlreadyRegistered`).
///
/// **Not from current state**: cached reply lives in [`crate::ClientTable`]
/// and may be replayed by any replica. Sourcing `view` from `consensus.view()`
/// would let a post-view-change `commit_journal` stamp a different view than
/// the original primary, diverging cached bytes for `(client, request)`.
/// Reading from `prepare_header` makes determinism structural.
///
/// # Panics
/// If buffer is not a valid reply.
pub fn build_reply_message(
    prepare_header: &PrepareHeader,
    body: &bytes::Bytes,
) -> Message<ReplyHeader> {
    build_reply_message_with(prepare_header, body.len(), |dst| dst.copy_from_slice(body))
}

/// Builds a reply [`Message`] whose body region is filled in place by `fill`.
///
/// Elides the throwaway `Bytes` a caller would otherwise allocate just to have
/// it copied in here. `fill` is handed the zeroed `body_len`-byte region and
/// must populate exactly that many bytes. Header fields follow the commit-time
/// determinism rules of [`build_reply_message`].
///
/// # Panics
/// If buffer is not a valid reply.
#[allow(clippy::cast_possible_truncation)]
pub fn build_reply_message_with<F>(
    prepare_header: &PrepareHeader,
    body_len: usize,
    fill: F,
) -> Message<ReplyHeader>
where
    F: FnOnce(&mut [u8]),
{
    let header_size = std::mem::size_of::<ReplyHeader>();
    let total_size = header_size + body_len;
    let mut buffer = bytes::BytesMut::zeroed(total_size);

    let header = ReplyHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: prepare_header.cluster,
        size: total_size as u32,
        // Commit-time view
        view: prepare_header.view,
        release: prepare_header.release,
        command: Command::Reply,
        // Original primary's id
        replica: prepare_header.replica,
        reserved_frame: [0; 66],
        request_checksum: prepare_header.request_checksum,
        context: 0,
        client: prepare_header.client,
        op: prepare_header.op,
        // Prepare's op (not commit_max): drives ClientTable eviction order;
        // must be deterministic across replicas.
        commit: prepare_header.op,
        timestamp: prepare_header.timestamp,
        request: prepare_header.request,
        operation: prepare_header.operation,
        ..Default::default()
    };
    // `BytesMut` makes no alignment guarantee, so never cast into it.
    buffer[..header_size].copy_from_slice(bytemuck::bytes_of(&header));

    fill(&mut buffer[header_size..]);

    // TODO: drop this copy once replies stop round-tripping through `Bytes`
    // and the binary protocol uses `Owned` end-to-end.
    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("reply buffer must contain a valid reply message")
}

/// Builds a `Reply` carrying only a single-entry result section, no payload:
/// the generic `[count=1][index=0][result=code]` rejection frame.
///
/// Two families of `code` ride this frame. `TransientNotCommitted` marks a
/// request that could not be committed *right now* (not-caught-up / in-flight
/// / pipeline-full / view-change cancel); the SDK decodes it and replays the
/// same `request_id` immediately instead of waiting out its response
/// read-timeout. Any other code is a TERMINAL rejection (e.g. a committed
/// metadata rejection like `UserAlreadyExists`) that the SDK surfaces as the
/// typed error.
///
/// Stamped from the request header (no prepare exists for a rejected op).
/// `commit` is the primary's current commit position and is informational
/// here: a rejection reply is sent, never cached in the `ClientTable`, so it
/// is not subject to the `commit_reply` regression order.
///
/// # Panics
/// If the constructed message buffer is not a valid reply.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn build_result_rejection_reply(
    request_header: &RoutedRequestHeader,
    commit: u64,
    code: u32,
) -> Message<ReplyHeader> {
    // `[count: u32][index: u32][result: u32]`, the single-entry rejection shape
    // of `ApplyReply::write_reply_body` (mirrored here to avoid a metadata dep).
    const RESULT_BODY_LEN: usize = 12;
    let header_size = std::mem::size_of::<ReplyHeader>();
    let total_size = header_size + RESULT_BODY_LEN;
    let mut buffer = bytes::BytesMut::zeroed(total_size);

    let header = ReplyHeader {
        cluster: request_header.cluster,
        size: total_size as u32,
        view: request_header.view,
        release: request_header.release,
        command: Command::Reply,
        replica: request_header.replica,
        request_checksum: request_header.request_checksum,
        client: request_header.client,
        // Position-typed like the sibling builders (`build_reply_from_request`
        // stamps `op: commit` too); inert on this path -- rejections are never
        // cached -- but keeps the wire field convention for frame inspection.
        op: commit,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        ..Default::default()
    };
    buffer[..header_size].copy_from_slice(bytemuck::bytes_of(&header));

    let body = &mut buffer[header_size..];
    body[0..4].copy_from_slice(&1u32.to_le_bytes());
    body[4..8].copy_from_slice(&0u32.to_le_bytes());
    body[8..12].copy_from_slice(&code.to_le_bytes());

    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("transient reply buffer must contain a valid reply message")
}

/// Reply for fast paths that skip the VSR pipeline (e.g. `AckLevel::NoAck`).
///
/// Stamps `op` and `commit` with `commit_max` — monotonic, so
/// `ClientTable::commit_reply` regression checks always pass.
///
/// # Panics
/// If the constructed message buffer is not valid.
#[allow(clippy::needless_pass_by_value, clippy::cast_possible_truncation)]
pub fn build_reply_from_request<B, P>(
    consensus: &VsrConsensus<B, P>,
    request_header: &RoutedRequestHeader,
    body: bytes::Bytes,
) -> Message<ReplyHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = std::mem::size_of::<ReplyHeader>();
    let total_size = header_size + body.len();
    let mut buffer = bytes::BytesMut::zeroed(total_size);

    let commit = consensus.commit_max();
    let header = ReplyHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: consensus.cluster(),
        size: total_size as u32,
        view: consensus.view(),
        release: 0,
        command: Command::Reply,
        replica: consensus.replica(),
        reserved_frame: [0; 66],
        request_checksum: request_header.request_checksum,
        context: 0,
        client: request_header.client,
        op: commit,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        ..Default::default()
    };
    buffer[..header_size].copy_from_slice(bytemuck::bytes_of(&header));

    if !body.is_empty() {
        buffer[header_size..].copy_from_slice(&body);
    }

    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("reply buffer must contain a valid reply message")
}

/// Reply that denies a request on the primary before it enters the VSR
/// pipeline.
///
/// The request's frame with an empty body and a nonzero `ReplyHeader.status`
/// (the request-level error channel the SDK peeks before body decode).
/// Nothing is prepared or replicated, so backups never see the denied
/// request; `op` stays 0, the shape reply consumers already read as "nothing
/// committed".
///
/// # Panics
/// If the constructed message buffer is not valid.
pub fn build_deny_reply_from_request<B, P>(
    consensus: &VsrConsensus<B, P>,
    request_header: &RoutedRequestHeader,
    status: u32,
) -> Message<ReplyHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let mut reply = build_reply_from_request(consensus, request_header, bytes::Bytes::new());
    let header_size = std::mem::size_of::<ReplyHeader>();
    let header_bytes = &mut reply.as_mut_slice()[..header_size];
    let mut header = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(header_bytes)
        .expect("freshly built reply header is valid");
    header.status = status;
    header.op = 0;
    header_bytes.copy_from_slice(bytemuck::bytes_of(&header));
    reply
}

/// [`build_deny_reply_from_request`] for layers that hold no consensus group
/// for the request's group (a shard fencing a frame aimed at a torn-down
/// or never-materialised partition).
///
/// Replica-stamped fields (`cluster`, `view`, `replica`) echo the request
/// instead, the same convention as [`build_result_rejection_reply`];
/// `commit` stays 0 because no commit position exists here and deny frames
/// are never cached in the `ClientTable`.
///
/// # Panics
/// If the constructed message buffer is not a valid reply message.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn build_deny_reply_from_request_header(
    request_header: &RoutedRequestHeader,
    status: u32,
) -> Message<ReplyHeader> {
    let header_size = std::mem::size_of::<ReplyHeader>();
    let mut buffer = bytes::BytesMut::zeroed(header_size);

    let header = ReplyHeader {
        cluster: request_header.cluster,
        size: header_size as u32,
        view: request_header.view,
        release: request_header.release,
        command: Command::Reply,
        replica: request_header.replica,
        request_checksum: request_header.request_checksum,
        client: request_header.client,
        status,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        ..Default::default()
    };
    buffer[..header_size].copy_from_slice(bytemuck::bytes_of(&header));

    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("deny reply buffer must contain a valid reply message")
}

/// Verify hash chain would not break if we add this header.
///
/// # Panics
/// If both headers share the same view and `current.parent != previous.checksum`.
pub fn panic_if_hash_chain_would_break_in_same_view(
    previous: &PrepareHeader,
    current: &PrepareHeader,
) {
    // If both headers are in the same view, parent must chain correctly.
    if previous.view == current.view {
        assert_eq!(
            current.parent, previous.checksum,
            "hash chain broken in same view: op={} parent={} expected={}",
            current.op, current.parent, previous.checksum
        );
    }
}

// TODO: Figure out how to make this check the journal if it contains the prepare.
/// # Panics
/// - If `header.command` is not `Command::Prepare`.
/// - If `header.view > consensus.view()`.
#[allow(clippy::cast_possible_truncation, clippy::future_not_send)]
pub async fn send_prepare_ok<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
    is_persisted: Option<bool>,
) where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command::Prepare);

    if consensus.status() != Status::Normal {
        return;
    }

    if consensus.is_transferring() {
        return;
    }

    if is_persisted == Some(false) {
        return;
    }

    assert!(
        header.view <= consensus.view(),
        "send_prepare_ok: prepare view {} > our view {}",
        header.view,
        consensus.view()
    );

    if header.op > consensus.sequencer().current_sequence() {
        return;
    }

    let prepare_ok_header = PrepareOkHeader {
        command: Command::PrepareOk,
        cluster: consensus.cluster(),
        replica: consensus.replica(),
        view: consensus.view(),
        op: header.op,
        commit: consensus.commit_max(),
        timestamp: header.timestamp,
        parent: header.parent,
        prepare_checksum: header.checksum,
        request: header.request,
        operation: header.operation,
        group: header.group,
        size: std::mem::size_of::<PrepareOkHeader>() as u32,
        ..Default::default()
    };

    let message: Message<PrepareOkHeader> = Message::<PrepareOkHeader>::new(std::mem::size_of::<
        PrepareOkHeader,
    >())
    .transmute_header(|_, new| {
        *new = prepare_ok_header;
        new.seal();
    });
    let primary = consensus.primary_index(consensus.view());

    consensus
        .send_or_loopback(primary, message.into_generic())
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Consensus, LocalPipeline, VsrAction};
    use aligned_vec::{AVec, ConstAlign};
    use iggy_binary_protocol::{ConsensusHeader, Operation, StartViewChangeHeader};
    use iggy_common::calculate_checksum;
    use message_bus::SendError;
    use server_common::{MESSAGE_ALIGN, iobuf::Frozen};

    /// `PrepareHeader`'s alignment, which every suffix body has to satisfy.
    const BODY_ALIGN: usize = align_of::<PrepareHeader>();

    /// A control-message body, aligned for the headers packed into it.
    type Body = AVec<u8, ConstAlign<BODY_ALIGN>>;

    #[derive(Debug, Default)]
    struct NoopBus;

    impl MessageBus for NoopBus {
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
    }

    #[allow(clippy::cast_possible_truncation)]
    fn prepare_message(op: u64, parent: u128, checksum: u128) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(std::mem::size_of::<PrepareHeader>()).transmute_header(
            |_, new| {
                *new = PrepareHeader {
                    command: Command::Prepare,
                    size: std::mem::size_of::<PrepareHeader>() as u32,
                    op,
                    parent,
                    checksum,
                    ..Default::default()
                };
            },
        )
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn replicate_preflight_fences_prepare_views() {
        let mut consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.set_view(2);

        let prepare_with_view = |view: u32| {
            Message::<PrepareHeader>::new(std::mem::size_of::<PrepareHeader>()).transmute_header(
                |_, new| {
                    *new = PrepareHeader {
                        command: Command::Prepare,
                        size: std::mem::size_of::<PrepareHeader>() as u32,
                        op: 1,
                        view,
                        ..Default::default()
                    };
                },
            )
        };

        let stale = prepare_with_view(1);
        assert_eq!(
            replicate_preflight(&consensus, stale.header()),
            Err(IgnoreReason::OlderView)
        );

        let ahead = prepare_with_view(3);
        assert_eq!(
            replicate_preflight(&consensus, ahead.header()),
            Err(IgnoreReason::NewerView)
        );

        let current = prepare_with_view(2);
        assert!(replicate_preflight(&consensus, current.header()).is_ok());
    }

    // Regression: DVC must carry commit_max not commit_min - see
    // `handle_start_view_change`.
    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn do_view_change_carries_commit_max_not_commit_min() {
        let consensus = VsrConsensus::new(1, 1, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        // Diverge the frontiers: applied (commit_min=5) lags known-committed
        // (commit_max=13). op is at 13 (>= commit_max), so the op clamp on the
        // DVC commit is a no-op here and the carried value is commit_max. The
        // clamp itself is covered by
        // `do_view_change_commit_clamped_to_op_when_commit_max_exceeds_op`.
        consensus.advance_commit_max(13);
        consensus.sequencer().set_sequence(13);
        for op in 1..=5 {
            consensus.advance_commit_min(op);
        }
        assert_eq!(consensus.commit_min(), 5);
        assert_eq!(consensus.commit_max(), 13);

        // An SVC for a higher view from another replica moves this node into the
        // view change; with f = 1 SVC excluding self, it emits its own DVC.
        let svc =
            Message::<StartViewChangeHeader>::new(std::mem::size_of::<StartViewChangeHeader>())
                .transmute_header(|_, new: &mut StartViewChangeHeader| {
                    new.command = Command::StartViewChange;
                    new.size = std::mem::size_of::<StartViewChangeHeader>() as u32;
                    new.view = 1;
                    new.replica = 0;
                    new.group = 0;
                });
        let actions = consensus.handle_start_view_change(PlaneKind::Metadata, svc.header());

        let dvc_commit = actions.iter().find_map(|action| match action {
            VsrAction::SendDoViewChange { commit, .. } => Some(*commit),
            _ => None,
        });
        assert_eq!(
            dvc_commit,
            Some(13),
            "DVC must carry commit_max (13), not commit_min (5)"
        );
    }

    // Regression: a backup that learned commit_max from a heartbeat before
    // receiving the prepares has commit_max > op. The DVC must clamp commit to
    // op so `DoViewChangeHeader::validate` (commit <= op) does not drop it;
    // dropping a quorum DVC stalls the view change.
    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn do_view_change_commit_clamped_to_op_when_commit_max_exceeds_op() {
        use iggy_binary_protocol::{ConsensusHeader, DoViewChangeHeader};

        let consensus = VsrConsensus::new(1, 1, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        // Behind backup: op = 4, but commit_max = 5 (a heartbeat raised the
        // commit point ahead of the prepares this replica holds).
        consensus.sequencer().set_sequence(4);
        consensus.advance_commit_max(5);
        assert_eq!(consensus.commit_max(), 5);

        let svc =
            Message::<StartViewChangeHeader>::new(std::mem::size_of::<StartViewChangeHeader>())
                .transmute_header(|_, new: &mut StartViewChangeHeader| {
                    new.command = Command::StartViewChange;
                    new.size = std::mem::size_of::<StartViewChangeHeader>() as u32;
                    new.view = 1;
                    new.replica = 0;
                    new.group = 0;
                });
        let actions = consensus.handle_start_view_change(PlaneKind::Metadata, svc.header());

        let (dvc_op, dvc_commit) = actions
            .iter()
            .find_map(|action| match action {
                VsrAction::SendDoViewChange { op, commit, .. } => Some((*op, *commit)),
                _ => None,
            })
            .expect("a DoViewChange must be emitted");
        assert_eq!(dvc_op, 4);
        assert_eq!(
            dvc_commit, 4,
            "commit must be clamped to op (4), not commit_max (5)"
        );

        // The clamped value passes the wire validate gate; the unclamped
        // commit_max (5) would be rejected and the DVC dropped.
        let header = |commit: u64| DoViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: std::mem::size_of::<DoViewChangeHeader>() as u32,
            view: 1,
            release: 0,
            command: Command::DoViewChange,
            replica: 1,
            reserved_frame: [0; 66],
            op: dvc_op,
            commit,
            group: 0,
            log_view: 0,
            reserved: [0; 68],
            nack_bitset: 0,
            present_bitset: 0,
        };
        assert!(header(dvc_commit).validate().is_ok());
        assert!(
            header(5).validate().is_err(),
            "commit > op must be rejected by the wire gate"
        );
    }

    #[test]
    fn given_restamped_view_when_sealing_should_keep_the_same_identity() {
        // `restamp_prepare_view` rewrites `view` on retransmission. If the identity
        // moved with it, one op would carry different checksums per receiving view
        // and the merge would read them as competing prepares nacking each other.
        let base = PrepareHeader {
            command: Command::Prepare,
            operation: iggy_binary_protocol::Operation::CreateStream,
            op: 9,
            view: 4,
            client: 11,
            request: 2,
            timestamp: 1234,
            checksum_body: 99,
            ..Default::default()
        };
        let restamped = PrepareHeader { view: 12, ..base };
        assert_eq!(
            base.identity_checksum(),
            restamped.identity_checksum(),
            "view must not participate in a prepare's identity"
        );
    }

    #[test]
    fn given_matching_view_when_restamping_should_reuse_frozen_prepare() {
        let message = prepare_message(9, 7, 11).transmute_header(|old, new: &mut PrepareHeader| {
            *new = old;
            new.view = 4;
        });
        let frozen = message.into_frozen();
        let original_ptr = frozen.as_slice().as_ptr();

        let restamped = restamp_prepare_view(frozen, 4).expect("valid prepare");
        let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            &restamped[..size_of::<PrepareHeader>()],
        )
        .expect("restamped prepare header");

        assert_eq!(restamped.as_slice().as_ptr(), original_ptr);
        assert_eq!(header.view, 4);
    }

    #[test]
    fn given_new_view_when_restamping_should_only_change_view() {
        let message = prepare_message(9, 7, 11).transmute_header(|old, new: &mut PrepareHeader| {
            *new = old;
            new.view = 4;
            new.client = 17;
            new.request = 23;
        });
        let expected_identity = message.header().identity_checksum();
        let expected_op = message.header().op;
        let expected_client = message.header().client;
        let expected_request = message.header().request;

        let restamped = restamp_prepare_view(message.into_frozen(), 12).expect("valid prepare");
        let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            &restamped[..size_of::<PrepareHeader>()],
        )
        .expect("restamped prepare header");

        assert_eq!(header.view, 12);
        assert_eq!(header.identity_checksum(), expected_identity);
        assert_eq!(header.op, expected_op);
        assert_eq!(header.client, expected_client);
        assert_eq!(header.request, expected_request);
    }

    #[test]
    fn given_truncated_buffer_when_restamping_should_reject() {
        let malformed: Frozen<MESSAGE_ALIGN> = Owned::<MESSAGE_ALIGN>::copy_from_slice(&[0]).into();

        assert!(restamp_prepare_view(malformed, 1).is_none());
    }

    #[test]
    fn given_truncated_buffer_when_reading_frozen_prepare_should_reject() {
        let malformed: Frozen<MESSAGE_ALIGN> = Owned::<MESSAGE_ALIGN>::copy_from_slice(&[0]).into();

        assert!(matches!(
            frozen_prepare_header(&malformed),
            Err(ChainReplicationError::MalformedPrepare)
        ));
    }

    #[test]
    fn given_committed_prepare_when_selecting_replication_target_should_reject() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        let prepare = prepare_message(0, 0, 0);

        assert!(matches!(
            replication_target(&consensus, prepare.header()),
            Err(ChainReplicationError::CommittedPrepare {
                op: 0,
                commit_min: 0
            })
        ));
    }

    #[test]
    fn given_different_prepares_at_one_op_when_sealing_should_differ() {
        // The distinction the merge depends on: two prepares at one op number are
        // told apart, so a canonical header is distinguishable from a stale one.
        let first = PrepareHeader {
            command: Command::Prepare,
            operation: iggy_binary_protocol::Operation::CreateStream,
            op: 5,
            client: 1,
            request: 1,
            timestamp: 100,
            ..Default::default()
        };
        let second = PrepareHeader { client: 2, ..first };
        assert_ne!(
            first.identity_checksum(),
            second.identity_checksum(),
            "distinct prepares at the same op must not share an identity"
        );

        let body_differs = PrepareHeader {
            checksum_body: 7,
            ..first
        };
        assert_ne!(
            first.identity_checksum(),
            body_differs.identity_checksum(),
            "the body reaches the identity through checksum_body"
        );
    }

    #[test]
    fn loopback_push_and_drain() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(msg.into_generic());
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        buf.clear();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn loopback_cleared_on_view_change_reset() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(msg.into_generic());
        consensus.reset_view_change_state();
        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn send_prepare_ok_pushes_to_loopback_when_primary() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let prepare_header = PrepareHeader {
            command: Command::Prepare,
            cluster: 1,
            view: 0,
            op: 0,
            checksum: 42,
            ..Default::default()
        };

        futures::executor::block_on(send_prepare_ok(&consensus, &prepare_header, Some(true)));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].header().command, Command::PrepareOk);

        let typed: Message<PrepareOkHeader> = buf
            .remove(0)
            .try_into_typed()
            .expect("loopback message must be PrepareOk");
        assert_eq!(typed.header().command, Command::PrepareOk);
    }

    /// A sender's suffix and matching body bytes for a replica that holds every op
    /// in `commit..=op` and can serve each body. Checksums derive from `(op, view)`
    /// so the hash chain connects, which the merge checks.
    fn dvc_with_full_suffix(
        replica: u8,
        view: u32,
        log_view: u32,
        op: u64,
        commit: u64,
    ) -> (iggy_binary_protocol::DoViewChangeHeader, Body) {
        dvc_with_suffix(replica, view, log_view, op, commit, None)
    }

    /// As [`dvc_with_full_suffix`], but `withhold_body` names one op whose present
    /// bit is cleared: header held, body unservable. A quorum where every sender
    /// withholds the same op decides nothing yet.
    fn dvc_with_suffix(
        replica: u8,
        view: u32,
        log_view: u32,
        op: u64,
        commit: u64,
        withhold_body: Option<u64>,
    ) -> (iggy_binary_protocol::DoViewChangeHeader, Body) {
        use iggy_binary_protocol::DoViewChangeHeader;

        let headers = suffix_headers(commit, op, log_view);
        let body = encode_body(&headers);
        let mut present = if headers.is_empty() {
            0
        } else {
            (1u128 << headers.len()) - 1
        };
        if let Some(withheld) = withhold_body
            && let Some(index) = headers.iter().position(|header| header.op == withheld)
        {
            present &= !(1u128 << index);
        }
        let header = DoViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: u32::try_from(std::mem::size_of::<DoViewChangeHeader>() + body.len())
                .expect("synthetic DVC frame fits u32"),
            view,
            release: 0,
            command: Command::DoViewChange,
            replica,
            reserved_frame: [0; 66],
            op,
            commit,
            group: 0,
            log_view,
            reserved: [0; 68],
            nack_bitset: 0,
            present_bitset: present,
        };
        (header, body)
    }

    /// Headers for `low..=high`, high-to-low as a suffix requires, sealed and
    /// chained the way a real producer writes them.
    ///
    /// Built ascending so each `parent` is the previous entry's real identity, then
    /// reversed. The decoder recomputes both, so fabricated checksums are rejected
    /// before the code under test sees them.
    fn suffix_headers(low: u64, high: u64, view: u32) -> Vec<PrepareHeader> {
        if high == 0 {
            return Vec::new();
        }
        let mut parent = 0u128;
        let mut ascending = Vec::new();
        for op in low.max(1)..=high {
            let mut header = PrepareHeader {
                command: Command::Prepare,
                operation: iggy_binary_protocol::Operation::CreateStream,
                op,
                view,
                parent,
                // Strictly increasing with op, so the suffix reads decreasing.
                timestamp: op,
                // Zero so the DVC's own commit drives `commit_max`.
                commit: 0,
                ..Default::default()
            };
            header.checksum = header.identity_checksum();
            parent = header.checksum;
            ascending.push(header);
        }
        ascending.reverse();
        ascending
    }

    fn svc_header(replica: u8, view: u32) -> iggy_binary_protocol::StartViewChangeHeader {
        iggy_binary_protocol::StartViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: u32::try_from(std::mem::size_of::<
                iggy_binary_protocol::StartViewChangeHeader,
            >())
            .expect("header fits u32"),
            view,
            release: 0,
            command: Command::StartViewChange,
            replica,
            reserved_frame: [0; 66],
            group: 0,
            reserved: [0; 120],
        }
    }

    /// Install the suffix this replica would read from its own journal.
    fn install_local_suffix(
        consensus: &VsrConsensus<NoopBus, LocalPipeline>,
        op: u64,
        commit: u64,
        log_view: u32,
    ) {
        let headers = suffix_headers(commit, op, log_view);
        consensus.set_local_dvc_suffix(crate::dvc_merge::suffix_all_present(headers));
    }

    #[test]
    fn given_an_undecidable_quorum_when_a_later_dvc_decides_it_should_start_the_view() {
        // Reaching a view-change quorum is not the same as deciding a log. Latching
        // `do_view_change_quorum` at the quorum makes every non-Ready outcome
        // terminal: later DoViewChanges are recorded, but the guard that calls the
        // merge is already false, so the view burns its status timeout for nothing.
        //
        // 5 replicas, view_change quorum 3, replica 0 is primary for view 5.
        let consensus = VsrConsensus::new(1, 0, 5, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.restore_commit_state(2, 2);
        consensus.sequencer().set_sequence(4);
        // This replica holds op 4's header but cannot serve its body. Suffix entries
        // run high-to-low, so bit 0 is op 4: clearing it offers ops 3 and 2 only.
        let local = suffix_headers(2, 4, 0);
        consensus.set_local_dvc_suffix(crate::view_change_quorum::DvcSuffix::new(local, 0, 0b110));

        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc_header(1, 5));

        // Two peers report, reaching the quorum of 3. All three hold op 4's header,
        // none can serve its body, and two replicas have yet to report.
        for replica in [1u8, 2] {
            let (dvc, body) = dvc_with_suffix(replica, 5, 0, 4, 2, Some(4));
            let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);
            assert!(actions.is_empty());
        }
        assert!(
            consensus.pending_view_log().is_none(),
            "op 4 is neither servable nor provably dead, so nothing may be parked yet"
        );

        // Replica 3 arrives holding the body: the deciding message, still merged.
        let (dvc, body) = dvc_with_full_suffix(3, 5, 0, 4, 2);
        let _ = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);

        let pending = consensus
            .pending_view_log()
            .expect("the DVC that supplies the missing body must complete the merge");
        assert_eq!(pending.op_head, 4);
        assert_eq!(pending.commit_max, 2);
    }

    #[test]
    fn given_a_sealed_prepare_when_verifying_integrity_should_accept() {
        let message = Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.op = 7;
                header.size = u32::try_from(size_of::<PrepareHeader>()).expect("header fits u32");
            },
        );
        let sealed = seal_prepare_checksum(message);
        assert_eq!(verify_prepare_integrity(sealed.header(), &[]), Ok(()));
    }

    #[test]
    fn given_a_prepare_whose_header_was_altered_when_verifying_should_reject() {
        // Downstream compares `checksum` as an opaque token, so without this a frame
        // corrupted in transit is journaled and then re-served to peers from the WAL.
        let message = Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.op = 7;
                header.size = u32::try_from(size_of::<PrepareHeader>()).expect("header fits u32");
            },
        );
        let mut sealed = seal_prepare_checksum(message);
        let bytes = &mut sealed.as_mut_slice()[..size_of::<PrepareHeader>()];
        let header = bytemuck::checked::try_from_bytes_mut::<PrepareHeader>(bytes)
            .expect("a prepare header round-trips its own bit pattern");
        header.op = 8;
        assert!(verify_prepare_integrity(&header.clone(), &[]).is_err());
    }

    #[test]
    fn given_an_unsealed_prepare_when_verifying_should_abstain() {
        // The partition plane leaves `checksum` at `CHECKSUM_UNSEALED` and carries a
        // verified `batch_checksum` over the same bytes instead.
        let header = PrepareHeader {
            command: Command::Prepare,
            op: 7,
            ..Default::default()
        };
        assert_eq!(header.checksum, CHECKSUM_UNSEALED);
        assert_eq!(verify_prepare_integrity(&header, &[]), Ok(()));
    }

    /// A frame carrying `body`, with `size` covering exactly header + body and
    /// `checksum_body` sealed over it, as the metadata projection does.
    /// `trailing` bytes of garbage past the sealed frame, which `size` does not
    /// cover. The buffer is `MESSAGE_ALIGN`ed: `PrepareHeader` holds `u128`s, so a
    /// `Vec<u8>` would only be 16-aligned by the allocator's good graces and miri
    /// rejects the cast.
    fn sealed_frame(body: &[u8], trailing: usize) -> Owned<MESSAGE_ALIGN> {
        let size = size_of::<PrepareHeader>() + body.len();
        let mut frame = Owned::<MESSAGE_ALIGN>::zeroed(size + trailing);
        let bytes = frame.as_mut_slice();
        bytes[size_of::<PrepareHeader>()..size].copy_from_slice(body);
        bytes[size..].fill(0xAA);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut bytes[..size_of::<PrepareHeader>()],
        );
        header.command = Command::Prepare;
        header.op = 7;
        header.size = u32::try_from(size).expect("fits u32");
        header.checksum_body = u128::from(calculate_checksum(body));
        frame
    }

    fn frame_header(frame: &Owned<MESSAGE_ALIGN>) -> PrepareHeader {
        *bytemuck::checked::from_bytes::<PrepareHeader>(
            &frame.as_slice()[..size_of::<PrepareHeader>()],
        )
    }

    #[test]
    fn given_a_prepare_whose_body_was_altered_when_verifying_should_reject() {
        let mut frame = sealed_frame(b"body", 0);
        let header = frame_header(&frame);
        assert_eq!(verify_prepare_integrity(&header, frame.as_slice()), Ok(()));

        *frame
            .as_mut_slice()
            .last_mut()
            .expect("the frame has a body") ^= 1;
        assert!(verify_prepare_integrity(&header, frame.as_slice()).is_err());
    }

    #[test]
    fn given_bytes_past_the_frame_size_when_verifying_should_ignore_them() {
        // `try_from` accepts a buffer longer than `size` without trimming; hashing to
        // the end would reject a correctly sealed prepare and disagree with the WAL scan.
        let padded = sealed_frame(b"body", 16);
        let header = frame_header(&padded);
        assert_eq!(
            verify_prepare_integrity(&header, padded.as_slice()),
            Ok(()),
            "only the bytes `size` covers are the body"
        );
    }

    #[test]
    fn given_a_size_that_overruns_the_buffer_when_verifying_should_reject() {
        // Truncated frame, header still claims the full length: the body it names is
        // not there to hash.
        let frame = sealed_frame(b"body", 0);
        let header = frame_header(&frame);

        let truncated = &frame.as_slice()[..frame.as_slice().len() - 1];
        assert!(verify_prepare_integrity(&header, truncated).is_err());
    }

    #[test]
    fn given_a_parked_merge_when_not_yet_started_should_not_advance_log_view() {
        // `log_view` claims "my log IS the log this view decided", which is what
        // makes a sender canonical next time. Raising it when the merge parks, before
        // the merged head is installed, lets a primary-elect that never finishes
        // repair vote as canonical carrying its own stale head, and ops the merge
        // kept then fall outside the next scan range, dropped with no nack.
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.restore_commit_state(2, 2);
        consensus.sequencer().set_sequence(3);
        install_local_suffix(&consensus, 3, 2, 0);
        assert_eq!(consensus.log_view(), 0);

        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc_header(1, 3));
        let (dvc, body) = dvc_with_full_suffix(2, 3, 0, 3, 2);
        let _ = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);

        assert!(consensus.pending_view_log().is_some(), "the merge parks");
        assert_eq!(
            consensus.log_view(),
            0,
            "a parked merge has installed nothing, so log_view must still \
             describe the log this replica actually holds"
        );
        assert_eq!(consensus.view(), 3, "the view itself did advance");

        let _ = consensus.start_pending_view(PlaneKind::Metadata);
        assert_eq!(
            consensus.log_view(),
            3,
            "installing the merged head is what earns the log_view claim"
        );
    }

    #[test]
    fn loopback_cleared_on_complete_view_change_as_primary() {
        // 3 replicas, replica 0 is primary for view 0 (and view 3: 3 % 3 = 0).
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.restore_commit_state(2, 2);
        consensus.sequencer().set_sequence(3);
        install_local_suffix(&consensus, 3, 2, 0);

        // SVC from replica 1, view 3. Replica 0 advances, records own SVC+DVC and
        // replica 1's SVC. DVC quorum needs 2; have 1.
        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc_header(1, 3));

        // Stale loopback queued between SVC and DVC quorum.
        let stale_msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(stale_msg.into_generic());

        // DVC from replica 2 forms the quorum and the merge settles the log.
        let (dvc, body) = dvc_with_full_suffix(2, 3, 0, 3, 2);
        let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);

        // Parked: nothing is announced until the journal can serve it.
        assert!(
            actions.is_empty(),
            "a merged view change must announce nothing until repair completes"
        );
        let pending = consensus
            .pending_view_log()
            .expect("a decidable quorum must park a merged log");
        assert_eq!(pending.op_head, 3);
        assert_eq!(pending.commit_max, 2);
        assert_eq!(consensus.status(), Status::ViewChange);

        // Stale loopback must be cleared.
        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(
            buf.is_empty(),
            "loopback queue must be empty after view change completion"
        );

        // Journal now covers the merged log, so the view starts and announces.
        let actions = consensus.start_pending_view(PlaneKind::Metadata);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, crate::VsrAction::SendStartView { .. })),
            "expected SendStartView once the view starts"
        );
        assert_eq!(consensus.status(), Status::Normal);
        assert!(
            consensus.pending_view_log().is_none(),
            "starting the view must consume the parked log"
        );
    }

    /// Refusing to start a view must not be terminal.
    ///
    /// A parked merge leaves the replica in `ViewChange` announcing nothing if the
    /// bodies never arrive, which is the intended trade against losing data but has
    /// to stay recoverable: the status timeout fires, escalates, and drops the parked
    /// log. Reusing a log merged for a superseded view would leak a truncation
    /// decided there into a view that never voted for it.
    /// A `StartView` from the view's primary, optionally carrying the view's
    /// canonical headers.
    fn start_view_with_suffix(
        replica: u8,
        view: u32,
        op: u64,
        commit: u64,
        with_suffix: bool,
    ) -> (iggy_binary_protocol::StartViewHeader, Body) {
        use iggy_binary_protocol::StartViewHeader;

        let body = if with_suffix {
            encode_body(&suffix_headers(commit, op, view))
        } else {
            Body::new(BODY_ALIGN)
        };
        let header = StartViewHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: u32::try_from(std::mem::size_of::<StartViewHeader>() + body.len())
                .expect("synthetic StartView fits u32"),
            view,
            release: 0,
            command: Command::StartView,
            replica,
            reserved_frame: [0; 66],
            op,
            commit,
            group: 0,
            reserved: [0; 88],
            incarnation: 0,
        };
        (header, body)
    }

    /// Encode headers as a control-message body.
    ///
    /// Aligned, because `dvc_suffix_decode` uses a checked `bytemuck` cast per
    /// 256-byte chunk: a `Vec<u8>` body reports `MalformedHeader` for entry 0
    /// instead of the failure under test. glibc over-aligns these; Miri does not.
    fn encode_body(headers: &[PrepareHeader]) -> Body {
        let mut body = Body::with_capacity(BODY_ALIGN, std::mem::size_of_val(headers));
        for header in headers {
            body.extend_from_slice(bytemuck::bytes_of(header));
        }
        body
    }

    #[test]
    fn given_a_corrupted_suffix_entry_when_decoding_should_reject_the_frame() {
        // The worst failure mode: a flipped bit in a canonical sender's header makes
        // it canonical for the view, so honest senders read as disagreeing and can
        // reach a nack quorum against a committed op. Recomputing keeps that out.
        let mut headers = suffix_headers(2, 4, 1);
        headers[0].timestamp ^= 0xFF;
        let body = encode_body(&headers);

        let error = crate::dvc_suffix_decode(&body, 4, 0, 0)
            .expect_err("a header that does not match its own checksum must be rejected");
        assert_eq!(error, crate::DvcSuffixError::ChecksumMismatch { index: 0 });
    }

    #[test]
    fn given_a_broken_suffix_chain_when_decoding_should_reject_the_frame() {
        // Well-sealed entries that do not link: a log, not a bag of records.
        let mut headers = suffix_headers(2, 4, 1);
        headers[0].parent ^= 0xFF;
        headers[0].checksum = headers[0].identity_checksum();
        let body = encode_body(&headers);

        let error = crate::dvc_suffix_decode(&body, 4, 0, 0)
            .expect_err("a suffix whose entries do not chain must be rejected");
        assert_eq!(error, crate::DvcSuffixError::ChainBreak { index: 1 });
    }

    #[test]
    fn given_a_suffix_with_mixed_view_stamps_when_decoding_should_be_accepted() {
        // A stitched suffix: a held op keeps the view that delivered it, a repaired
        // neighbour carries the view that decided it. Rejecting drops the sender's
        // vote forever (the retransmit is byte-identical) and the cluster can fail to
        // elect. No re-seal: `identity_checksum` excludes `view`.
        let mut headers = suffix_headers(2, 4, 2);
        headers[1].view = 1;
        let body = encode_body(&headers);

        let suffix = crate::dvc_suffix_decode(&body, 4, 0, 0)
            .expect("a stitched suffix with mixed view stamps must decode");
        assert_eq!(suffix.len(), 3);
    }

    #[test]
    fn given_an_unsealed_suffix_when_decoding_should_be_accepted() {
        // The on-disk sentinel: suffixes are read out of the journal, which may hold
        // pre-seal entries, and partition-plane prepares are unsealed by construction.
        let headers: Vec<PrepareHeader> = suffix_headers(2, 4, 1)
            .into_iter()
            .map(|mut header| {
                header.checksum = 0;
                header.parent = 0;
                header
            })
            .collect();
        let body = encode_body(&headers);

        let suffix =
            crate::dvc_suffix_decode(&body, 4, 0, 0).expect("an unsealed suffix must still decode");
        assert_eq!(suffix.len(), 3);
    }

    #[test]
    fn given_start_view_with_suffix_when_adopted_should_record_the_canonical_headers() {
        // The backup keeps the view's headers so its repair ingest can reject a body
        // that disagrees with the view's decision, and so a disagreeing local entry
        // is reported rather than silently blocking its own repair forever.
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        // Replica 1 is primary for view 1 (1 % 3).
        let (header, body) = start_view_with_suffix(1, 1, 5, 3, true);
        let actions = consensus.handle_start_view(PlaneKind::Metadata, &header, &body);

        assert!(!actions.is_empty(), "a valid StartView must be adopted");
        assert_eq!(consensus.status(), Status::Normal);
        assert_eq!(consensus.sequencer().current_sequence(), 5);

        let recorded = consensus
            .pending_view_log()
            .expect("an adopted StartView carrying a suffix must record its headers");
        assert_eq!(recorded.op_head, 5);
        assert_eq!(recorded.commit_max, 3);
        assert_eq!(
            recorded.headers.iter().map(|h| h.op).collect::<Vec<_>>(),
            vec![5, 4, 3],
            "headers run high-to-low from the head down to the announced commit"
        );
    }

    #[test]
    fn given_start_view_without_suffix_when_adopted_should_trust_the_announced_op() {
        // Probe answers and stale-view corrections carry numbers only: a backup must
        // still adopt, and record nothing it could mistake for the view's decision.
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let (header, body) = start_view_with_suffix(1, 1, 5, 3, false);
        assert!(body.is_empty());
        let actions = consensus.handle_start_view(PlaneKind::Metadata, &header, &body);

        assert!(
            !actions.is_empty(),
            "a numbers-only StartView must still adopt"
        );
        assert_eq!(consensus.sequencer().current_sequence(), 5);
        assert!(
            consensus.pending_view_log().is_none(),
            "no suffix means no canonical headers to verify against"
        );
    }

    #[test]
    fn given_parked_view_change_when_status_timeout_fires_should_escalate_and_drop_merged_log() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.restore_commit_state(2, 2);
        consensus.sequencer().set_sequence(3);
        install_local_suffix(&consensus, 3, 2, 0);

        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc_header(1, 3));
        let (dvc, body) = dvc_with_full_suffix(2, 3, 0, 3, 2);
        let _ = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);

        // Parked: no shard here reports coverage, so the view never starts.
        assert!(consensus.pending_view_log().is_some());
        assert_eq!(consensus.status(), Status::ViewChange);
        let parked_view = consensus.view();

        // `VIEW_CHANGE_STATUS_TICKS` is 500; tick past it. Escalation shows as the
        // view advancing, since the 50-tick SVC retransmit also emits a send.
        let mut escalated = false;
        for _ in 0..600 {
            let _ = consensus.tick(PlaneKind::Metadata);
            if consensus.view() > parked_view {
                escalated = true;
                break;
            }
        }

        assert!(
            escalated,
            "a parked view change must still escalate on the status timeout"
        );
        assert!(
            consensus.view() > parked_view,
            "escalation must advance the view past {parked_view}, got {}",
            consensus.view()
        );
        assert!(
            consensus.pending_view_log().is_none(),
            "the superseded merged log must be dropped, not carried into the next view"
        );
        assert_eq!(consensus.status(), Status::ViewChange);
    }

    /// A merged log may claim an uncommitted range up to the *configured*
    /// prepare depth. With a pipeline deeper than the default const, the new
    /// primary schedules the rebuild rather than panicking on the old
    /// `PIPELINE_PREPARE_QUEUE_MAX` bound.
    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn given_view_change_range_above_default_when_starting_view_should_rebuild() {
        let depth = crate::PIPELINE_PREPARE_QUEUE_MAX * 2;
        // Strictly above the default const, still within the configured depth.
        let winner_op = (crate::PIPELINE_PREPARE_QUEUE_MAX + 8) as u64;

        // 3 replicas, replica 0 is primary for view 3 (3 % 3 = 0).
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            0,
            NoopBus,
            LocalPipeline::with_capacities(depth, depth * 2),
        );
        consensus.init();
        consensus.sequencer().set_sequence(winner_op);
        install_local_suffix(&consensus, winner_op, 1, 0);

        // SVC from replica 1 moves replica 0 into view 3 and records its own DVC.
        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc_header(1, 3));

        // DVC from replica 2 claims the same deep log, forming quorum.
        let (dvc, body) = dvc_with_full_suffix(2, 3, 0, winner_op, 1);
        let _ = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc, &body);

        let actions = consensus.start_pending_view(PlaneKind::Metadata);
        assert!(
            actions.iter().any(|action| matches!(
                action,
                VsrAction::RebuildPipeline { from_op: 2, to_op } if *to_op == winner_op
            )),
            "expected RebuildPipeline over the uncommitted range, got {actions:?}"
        );
    }

    #[test]
    fn send_prepare_ok_sends_to_bus_when_not_primary() {
        // Replica 1, view 0; primary=0, so send_or_loopback takes bus path.
        let consensus = VsrConsensus::new(1, 1, 3, 0, SpyBus::new(), LocalPipeline::new());
        consensus.init();

        let prepare_header = PrepareHeader {
            command: Command::Prepare,
            cluster: 1,
            view: 0,
            op: 0,
            checksum: 42,
            ..Default::default()
        };

        futures::executor::block_on(send_prepare_ok(&consensus, &prepare_header, Some(true)));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty(), "non-primary must not loopback");

        let sent = consensus.message_bus().sent.borrow();
        assert_eq!(
            sent.len(),
            1,
            "exactly one PrepareOk must be sent to the bus"
        );
        assert_eq!(
            sent[0].0, 0,
            "addressed to the primary (replica 0 in view 0)"
        );
    }

    struct SpyBus {
        sent: std::cell::RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>,
    }

    impl SpyBus {
        fn new() -> Self {
            Self {
                sent: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for SpyBus {
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        async fn send_to_replica(
            &self,
            replica: u8,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.sent.borrow_mut().push((replica, data));
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
    }

    #[test]
    fn send_or_loopback_routes_self_to_queue() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, SpyBus::new(), LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        futures::executor::block_on(consensus.send_or_loopback(0, msg.into_generic()));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        assert!(consensus.message_bus().sent.borrow().is_empty());
    }

    #[test]
    fn send_or_loopback_routes_other_to_bus() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, SpyBus::new(), LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        futures::executor::block_on(consensus.send_or_loopback(1, msg.into_generic()));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());

        let sent = consensus.message_bus().sent.borrow();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, 1);
    }

    #[test]
    fn drains_only_up_to_commit_frontier_even_without_quorum_flags() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(5, 0, 50));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(6, 50, 60));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(7, 60, 70));

        consensus.advance_commit_max(6);
        let drained = drain_committable_prefix(&consensus);
        let drained_ops: Vec<_> = drained.into_iter().map(|entry| entry.header.op).collect();

        assert_eq!(drained_ops, vec![5, 6]);
        assert_eq!(
            consensus.pipeline_head_header().map(|header| header.op),
            Some(7)
        );
    }

    // A deny echoes the request frame with an empty body, carries the error
    // code in `status`, and keeps `op` at 0 even when the group has committed
    // ops -- reply consumers read `op == 0` as "nothing committed", and the
    // deny must never be mistaken for a committed reply.
    #[test]
    fn deny_reply_from_request_stamps_status_and_commits_nothing() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.advance_commit_max(4);

        let request = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::DeleteConsumerOffset,
            client: 42,
            request: 7,
            ..Default::default()
        };
        let status = 3021;
        let reply = build_deny_reply_from_request(&consensus, &request, status);

        let header = reply.header();
        assert_eq!(header.command, Command::Reply);
        assert_eq!(header.status, status);
        assert_eq!(header.op, 0, "a deny commits nothing");
        assert_eq!(header.commit, 4);
        assert_eq!(header.client, 42);
        assert_eq!(header.request, 7);
        assert_eq!(header.operation, Operation::DeleteConsumerOffset);
        assert_eq!(
            header.size as usize,
            std::mem::size_of::<ReplyHeader>(),
            "deny reply body must be empty"
        );
        assert!(header.validate().is_ok());
    }
}
