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

//! Batched `Commit` and `CommitReply` frames.
//!
//! A shard does not send a `Commit` or a `CommitReply` when a group asks for
//! it. It queues an entry in an outbox, one queue per peer node. Each flush
//! sends one frame per peer node with all of its entries.

use bytemuck::Pod;
use consensus::{Pipeline, PipelineEntry, VsrAction, VsrConsensus};
use iggy_binary_protocol::{
    COMMIT_BATCH_ENTRIES_MAX, Command, CommitEntry, CommitHeader, CommitReplyEntry,
    CommitReplyHeader, ConsensusHeader, HEADER_SIZE, batch_entries, batch_frame_size,
};
use message_bus::MessageBus;
use server_common::Message;
use std::cell::RefCell;
use std::collections::BTreeMap;

/// A `Commit` entry and the member index of its target in the group.
#[derive(Debug, Clone, Copy)]
pub struct QueuedCommit {
    pub entry: CommitEntry,
    pub member: u8,
}

/// Entries that wait for the next batch, one queue per peer node.
#[derive(Debug)]
pub struct PeerOutbox<T> {
    queued: RefCell<Option<QueuedBatches<T>>>,
}

/// The queues of one flush, and the cluster and the node that send them.
#[derive(Debug)]
pub struct QueuedBatches<T> {
    pub cluster: u128,
    pub sender: u8,
    pub peers: BTreeMap<u8, Vec<T>>,
}

impl<T> Default for PeerOutbox<T> {
    fn default() -> Self {
        Self {
            queued: RefCell::new(None),
        }
    }
}

impl<T> PeerOutbox<T> {
    /// Queue `item` for node `peer`. All entries of one shard come from the
    /// same cluster and the same node.
    pub fn push(&self, cluster: u128, sender: u8, peer: u8, item: T) {
        self.queued
            .borrow_mut()
            .get_or_insert_with(|| QueuedBatches {
                cluster,
                sender,
                peers: BTreeMap::new(),
            })
            .peers
            .entry(peer)
            .or_default()
            .push(item);
    }

    /// Take every queue and leave the outbox empty.
    pub fn take(&self) -> Option<QueuedBatches<T>> {
        self.queued.borrow_mut().take()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queued.borrow().is_none()
    }
}

pub type CommitOutbox = PeerOutbox<QueuedCommit>;
pub type ReplyOutbox = PeerOutbox<CommitReplyEntry>;

/// Move every `SendCommit` of `actions` into `outbox`. A heartbeat goes to
/// every other member of the group.
pub fn queue_commits<B, P>(
    outbox: &CommitOutbox,
    consensus: &VsrConsensus<B, P>,
    actions: &mut Vec<VsrAction>,
) where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    actions.retain(|action| {
        let VsrAction::SendCommit {
            view,
            commit,
            group,
            timestamp_monotonic,
            heartbeat_state,
            target,
        } = *action
        else {
            return true;
        };
        let entry = CommitEntry::new(
            group,
            consensus.partition_incarnation(),
            view,
            commit,
            timestamp_monotonic,
            heartbeat_state,
            consensus.replica(),
        );
        let members = consensus.replica_count();
        for member in (0..members).filter(|member| target.is_none_or(|target| target == *member)) {
            if member == consensus.replica() {
                continue;
            }
            if let Some(peer) = consensus.members().node(member) {
                outbox.push(
                    consensus.cluster(),
                    consensus.node_id(),
                    peer,
                    QueuedCommit { entry, member },
                );
            }
        }
        false
    });
}

/// Queue the reply that `consensus` owes for the accepted `commit`.
pub fn queue_commit_reply<B, P>(
    outbox: &ReplyOutbox,
    consensus: &VsrConsensus<B, P>,
    commit: &CommitEntry,
) where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let Some(peer) = consensus.members().node(commit.replica) else {
        return;
    };
    outbox.push(
        consensus.cluster(),
        consensus.node_id(),
        peer,
        CommitReplyEntry::new(
            commit.group,
            consensus.partition_incarnation(),
            commit.view,
            commit.timestamp_monotonic,
            consensus.replica(),
        ),
    );
}

/// A `Commit` frame that holds `entries`. `None` when the entries do not fit
/// in one frame.
#[must_use]
pub fn encode_commit_batch(
    cluster: u128,
    sender: u8,
    entries: &[CommitEntry],
) -> Option<Message<CommitHeader>> {
    if entries.len() > COMMIT_BATCH_ENTRIES_MAX {
        return None;
    }
    let (message, size, checksum_body, entry_count) = encode_body::<CommitHeader, _>(entries)?;
    Some(message.transmute_header(|_, header: &mut CommitHeader| {
        header.command = Command::Commit;
        header.cluster = cluster;
        header.replica = sender;
        header.size = size;
        header.entry_count = entry_count;
        header.checksum_body = checksum_body;
        header.seal();
    }))
}

/// A `CommitReply` frame that holds `entries`. `None` when the entries do not
/// fit in one frame.
#[must_use]
pub fn encode_commit_reply_batch(
    cluster: u128,
    sender: u8,
    entries: &[CommitReplyEntry],
) -> Option<Message<CommitReplyHeader>> {
    let (message, size, checksum_body, entry_count) = encode_body::<CommitReplyHeader, _>(entries)?;
    Some(
        message.transmute_header(|_, header: &mut CommitReplyHeader| {
            header.command = Command::CommitReply;
            header.cluster = cluster;
            header.replica = sender;
            header.size = size;
            header.entry_count = entry_count;
            header.checksum_body = checksum_body;
            header.seal();
        }),
    )
}

/// The entries of a batch frame, or `None` when its body does not match its
/// checksum. A receiver then drops the whole frame.
#[must_use]
pub fn verified_entries<H, E>(message: &Message<H>, checksum_body: u128) -> Option<Vec<E>>
where
    H: ConsensusHeader,
    E: Pod,
{
    let body = message
        .as_slice()
        .get(HEADER_SIZE..message.header().size() as usize)?;
    if body_checksum(body) != checksum_body {
        return None;
    }
    Some(batch_entries(body).collect())
}

fn encode_body<H, E>(entries: &[E]) -> Option<(Message<H>, u32, u128, u32)>
where
    H: ConsensusHeader,
    E: Pod,
{
    if entries.is_empty() {
        return None;
    }
    let size = batch_frame_size::<E>(entries.len())?;
    let entry_count = u32::try_from(entries.len()).ok()?;
    let mut message = Message::<H>::new(size as usize);
    // Body first: `transmute_header` writes only the header bytes.
    let body = &mut message.as_mut_slice()[HEADER_SIZE..size as usize];
    body.copy_from_slice(bytemuck::cast_slice(entries));
    let checksum_body = body_checksum(body);
    Some((message, size, checksum_body, entry_count))
}

fn body_checksum(body: &[u8]) -> u128 {
    u128::from(iggy_common::calculate_checksum(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::HeartbeatState;
    use server_common::MessageBag;

    fn entry(group: u64) -> CommitEntry {
        CommitEntry::new(group, 1, 2, 3, 4, HeartbeatState::Busy, 1)
    }

    #[test]
    fn given_commit_entries_when_encoded_should_decode_through_message_bag() {
        let entries = [entry(7), entry(8)];
        let message = encode_commit_batch(5, 2, &entries).unwrap();
        let bag = MessageBag::try_from(message.into_generic()).unwrap();
        let MessageBag::Commit(message) = bag else {
            panic!("not a Commit");
        };
        assert_eq!(message.header().replica, 2);
        assert_eq!(message.header().entry_count, 2);
        let checksum_body = message.header().checksum_body;
        let decoded: Vec<CommitEntry> = verified_entries(&message, checksum_body).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn given_no_entries_or_too_many_when_encoding_should_refuse() {
        assert!(encode_commit_batch(1, 0, &[]).is_none());
        let too_many = vec![entry(1); COMMIT_BATCH_ENTRIES_MAX + 1];
        assert!(encode_commit_batch(1, 0, &too_many).is_none());
    }
}
