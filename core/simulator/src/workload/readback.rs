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

//! Poll readback oracle.
//!
//! Every message a poll serves must be committed: when the reply arrives, some
//! live replica of the same partition incarnation must hold a committed prefix
//! that covers it, and at quiesce the committed log must hold the same message at
//! the same offset. Two polls that read one offset of one incarnation must agree.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::hash::{DefaultHasher, Hasher};

use consensus::PartitionsHandle;
use iggy_binary_protocol::Operation;
use iggy_binary_protocol::responses::messages::PollMessagesResponse;
use partitions::IggyPartition;
use server_common::send_messages::decode_prepare_slice_trusted;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::bus::SharedSimOutbox;
use crate::deps::SimSuperblock;

type SimPartition = IggyPartition<SharedSimOutbox, SimSuperblock>;

/// One message as a poll or the committed log holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Served {
    id: u128,
    payload_digest: u64,
}

impl Served {
    fn new(id: u128, payload: &[u8]) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(payload);
        Self {
            id,
            payload_digest: hasher.finish(),
        }
    }
}

/// A served poll that still owes the committed-prefix check.
#[derive(Debug, Clone, Copy)]
struct PendingPoll {
    namespace: IggyNamespace,
    incarnation: u64,
    /// One past the highest offset served.
    end: u64,
    /// [`PollReadback::epoch`] when the poll was built.
    epoch: u64,
    client: u128,
    request: u64,
}

/// Counts for the run summary.
#[derive(Debug, Default, Clone, Copy)]
pub struct PollStats {
    /// Polls that served at least one message.
    pub polls_served: u64,
    pub messages_served: u64,
    /// Served polls whose messages a live committed prefix covered on arrival.
    pub polls_compared: u64,
    /// Served polls whose server could have crashed while the reply was on the
    /// wire. Only the quiesce comparison checks them.
    pub polls_deferred: u64,
    /// Served messages compared against the committed log at quiesce.
    pub messages_compared_at_quiesce: u64,
    /// Served messages without retained bytes on at least one live host, including
    /// incarnations no live replica hosts anymore. These are not full comparisons.
    pub messages_unverifiable_at_quiesce: u64,
    pub group_polls_served: u64,
    /// Group polls answered with the re-sync sentinel by a partition that installed
    /// another owner.
    pub group_polls_fenced: u64,
    /// Served group polls that saw a different owner generation than the previous
    /// served group poll of the same partition incarnation.
    pub owner_changes: u64,
}

/// Served messages, keyed by partition incarnation and offset, and the checks
/// that they were committed.
#[derive(Debug, Default)]
pub struct PollReadback {
    observed: BTreeMap<(IggyNamespace, u64, u64), Served>,
    pending: Vec<PendingPoll>,
    /// Epoch each in-flight poll was built in, by `(client, request)`.
    built: BTreeMap<(u128, u64), u64>,
    group_owners: BTreeMap<(IggyNamespace, u64), u64>,
    /// Crashed replica count and the sum of metadata incarnations last seen. A
    /// crash raises the count and a restart raises the sum, so any crash or
    /// restart changes the pair.
    liveness: (usize, u128),
    /// Bumped whenever `liveness` changes.
    epoch: u64,
    stats: PollStats,
}

impl PollReadback {
    #[must_use]
    pub const fn stats(&self) -> PollStats {
        self.stats
    }

    /// Advance the epoch if a replica crashed or restarted since the last call.
    pub fn observe_liveness(&mut self, sim: &Simulator) {
        let incarnations = sim
            .replicas
            .iter()
            .map(|replica| replica.metadata_incarnation)
            .fold(0u128, u128::wrapping_add);
        let liveness = (sim.crashed.len(), incarnations);
        if liveness != self.liveness {
            self.liveness = liveness;
            self.epoch += 1;
        }
    }

    pub(crate) fn note_built(&mut self, key: (u128, u64)) {
        self.built.insert(key, self.epoch);
    }

    pub(crate) fn forget(&mut self, key: (u128, u64)) {
        self.built.remove(&key);
    }

    pub(crate) fn forget_client(&mut self, client_id: u128) {
        self.built.retain(|&(owner, _), _| owner != client_id);
    }

    /// Record a served poll of `count` messages from `offset` as `group` or not.
    ///
    /// # Panics
    /// If the reply does not decode, serves more than `count` messages, serves a
    /// gap or starts past `offset`, or serves a message at an offset another poll
    /// of the same incarnation saw with different content.
    pub(crate) fn on_served(
        &mut self,
        key: (u128, u64),
        namespace: IggyNamespace,
        (offset, count, group): (u64, u32, bool),
        body: &[u8],
        seed: u64,
    ) {
        let epoch = self.built.remove(&key).unwrap_or(0);
        let response = PollMessagesResponse::decode(body).unwrap_or_else(|error| {
            panic!(
                "poll reply for {namespace:?} does not decode: {error:?} (client={}, \
                 request={}, seed={seed:#x})",
                key.0, key.1,
            )
        });
        if group && response.header.partition_id == iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL
        {
            self.stats.group_polls_fenced += 1;
            return;
        }
        let incarnation = response.header.context.incarnation;
        let mut next = offset;
        for message in response.messages {
            let message = message.unwrap_or_else(|error| {
                panic!(
                    "poll reply for {namespace:?} carries a malformed batch: {error:?} \
                     (client={}, request={}, seed={seed:#x})",
                    key.0, key.1,
                )
            });
            assert_eq!(
                message.offset, next,
                "poll of {namespace:?} from offset {offset} served offset {} where {next} \
                 was due (client={}, request={}, seed={seed:#x})",
                message.offset, key.0, key.1,
            );
            let served = Served::new(message.id, message.payload);
            match self.observed.entry((namespace, incarnation, next)) {
                Entry::Vacant(slot) => {
                    slot.insert(served);
                }
                Entry::Occupied(slot) => assert_eq!(
                    *slot.get(),
                    served,
                    "two polls of {namespace:?} incarnation {incarnation} disagree on offset \
                     {next} (client={}, request={}, seed={seed:#x})",
                    key.0,
                    key.1,
                ),
            }
            next += 1;
        }
        let served = next - offset;
        assert!(
            served <= u64::from(count),
            "poll of {namespace:?} asked for {count} message(s) and got {served} \
             (client={}, request={}, seed={seed:#x})",
            key.0,
            key.1,
        );
        if group {
            self.stats.group_polls_served += 1;
            let generation = response.header.context.owner_generation;
            if let Some(previous) = self
                .group_owners
                .insert((namespace, incarnation), generation)
                && previous != generation
            {
                self.stats.owner_changes += 1;
            }
        }
        if served == 0 {
            return;
        }
        self.stats.polls_served += 1;
        self.stats.messages_served += served;
        self.pending.push(PendingPoll {
            namespace,
            incarnation,
            end: next,
            epoch,
            client: key.0,
            request: key.1,
        });
    }

    /// Check every served poll against the committed prefixes of the live replicas.
    ///
    /// # Panics
    /// If no live replica of the served incarnation has committed a served message
    /// while no replica crashed or restarted since the poll was built: the server
    /// that answered is still live, so its own committed prefix must cover what it
    /// served.
    pub fn check_served(&mut self, sim: &Simulator, seed: u64) {
        self.observe_liveness(sim);
        for poll in std::mem::take(&mut self.pending) {
            let mut covered = false;
            let mut unknown = false;
            for replica_idx in 0..sim.replica_count {
                if sim.is_crashed(replica_idx) {
                    continue;
                }
                let Some(partition) = hosted(sim, replica_idx, poll.namespace, poll.incarnation)
                else {
                    continue;
                };
                match committed_end(partition) {
                    Some(end) => covered |= end >= poll.end,
                    None => unknown = true,
                }
            }
            if covered {
                self.stats.polls_compared += 1;
                continue;
            }
            assert!(
                unknown || poll.epoch != self.epoch,
                "a poll of {:?} incarnation {} served offsets below {} that no live replica \
                 has committed (client={}, request={}, seed={seed:#x})",
                poll.namespace,
                poll.incarnation,
                poll.end,
                poll.client,
                poll.request,
            );
            self.stats.polls_deferred += 1;
        }
    }

    /// Compare every served message with the committed log of each live replica
    /// that retains it in the journal or repair ring, after the drain. Missing
    /// historical bytes are counted as unverifiable; lagging replicas may have
    /// shorter committed prefixes.
    ///
    /// # Panics
    /// If a replica committed different content at a served offset, or every live
    /// replica of the incarnation committed less than a served offset.
    pub fn assert_served_committed(&mut self, sim: &Simulator, seed: u64) {
        self.check_served(sim, seed);
        let mut logs: BTreeMap<(u8, IggyNamespace), Option<CommittedLog>> = BTreeMap::new();
        for (&(namespace, incarnation, offset), served) in &self.observed {
            let mut hosts = 0;
            let mut found = false;
            let mut short = 0;
            let mut unavailable = false;
            for replica_idx in 0..sim.replica_count {
                if sim.is_crashed(replica_idx) {
                    continue;
                }
                let log = logs
                    .entry((replica_idx, namespace))
                    .or_insert_with(|| CommittedLog::read(sim, replica_idx, namespace, seed));
                let Some(log) = log.as_ref().filter(|log| log.incarnation == incarnation) else {
                    continue;
                };
                hosts += 1;
                if let Some(committed) = log.messages.get(&offset) {
                    assert_eq!(
                        committed, served,
                        "replica {replica_idx} committed other content at offset {offset} of \
                         {namespace:?} incarnation {incarnation} than a poll served \
                         (seed={seed:#x})",
                    );
                    found = true;
                } else {
                    unavailable = true;
                    if log.complete && log.end <= offset {
                        short += 1;
                    }
                }
            }
            assert!(
                hosts == 0 || short < hosts,
                "a poll served offset {offset} of {namespace:?} incarnation {incarnation}, \
                 but no live replica committed it (seed={seed:#x})",
            );
            if found {
                self.stats.messages_compared_at_quiesce += 1;
            }
            if hosts == 0 || unavailable {
                self.stats.messages_unverifiable_at_quiesce += 1;
            }
        }
    }
}

/// The partition `replica_idx` hosts for `namespace`, if it is `incarnation`.
fn hosted(
    sim: &Simulator,
    replica_idx: u8,
    namespace: IggyNamespace,
    incarnation: u64,
) -> Option<&SimPartition> {
    sim.replicas[usize::from(replica_idx)]
        .partition_shard(namespace)
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .filter(|partition| partition.created_revision() == incarnation)
}

/// One past the last offset of the newest committed send, `Some(0)` when nothing
/// was sent, or `None` when the journal no longer holds a committed op it needs.
fn committed_end(partition: &SimPartition) -> Option<u64> {
    let journal = &partition.log.journal().inner;
    for op in (1..=partition.consensus().commit_min()).rev() {
        if journal.repair_header(op)?.operation != Operation::SendMessages {
            continue;
        }
        let entry = journal.repair_entry(op)?;
        let batch = decode_prepare_slice_trusted(entry.as_slice()).ok()?;
        return Some(batch.header.base_offset + u64::from(batch.header.message_count));
    }
    Some(0)
}

/// The committed messages one replica holds for a namespace.
struct CommittedLog {
    incarnation: u64,
    messages: BTreeMap<u64, Served>,
    /// One past the highest committed offset.
    end: u64,
    complete: bool,
}

impl CommittedLog {
    fn read(sim: &Simulator, replica_idx: u8, namespace: IggyNamespace, seed: u64) -> Option<Self> {
        let partition = sim.replicas[usize::from(replica_idx)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)?;
        let journal = &partition.log.journal().inner;
        let mut messages = BTreeMap::new();
        let mut end = 0;
        let mut retained = 0;
        let mut complete = true;
        for (op, header) in journal.repair_headers_in(1..=partition.consensus().commit_min()) {
            retained += 1;
            if header.operation != Operation::SendMessages {
                continue;
            }
            let Some(entry) = journal.repair_entry(op) else {
                complete = false;
                continue;
            };
            let batch = decode_prepare_slice_trusted(entry.as_slice()).unwrap_or_else(|error| {
                panic!(
                    "replica {replica_idx} holds committed op {op} of {namespace:?} whose batch \
                     does not decode: {error:?} (seed={seed:#x})"
                )
            });
            for message in &batch {
                let offset = batch.header.base_offset + u64::from(message.header.offset_delta);
                messages.insert(offset, Served::new(message.header.id, message.payload));
                end = end.max(offset + 1);
            }
        }
        Some(Self {
            incarnation: partition.created_revision(),
            messages,
            end,
            complete: complete && retained == partition.consensus().commit_min(),
        })
    }
}
