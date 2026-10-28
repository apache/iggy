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

use crate::{Consensus, Pipeline, PipelineEntry, Sequencer, VsrConsensus};
use iggy_binary_protocol::PrepareProgressHeader;
use message_bus::MessageBus;

pub const REPLICATION_BATCH_BYTES_MAX: u64 = 32 * 1024 * 1024;
const PROGRESS_INTERVAL_MICROS: u64 = 1_000_000;

/// A probe follows every prepare in its batch on the same FIFO link. No new
/// prepares are sent until its reply, so a reply also drains the batch's credit.
/// Payloads remain in the journal, never in this per-peer state.
#[derive(Debug)]
pub struct ReplicationFlow {
    epoch: Option<(u32, u64)>,
    nonce: u64,
    peers: Vec<PeerReplication>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerReplication {
    sent_through: u64,
    bytes: u64,
    replay_from: Option<u64>,
    probe: Option<(u64, u64)>,
    due: u64,
    retry_at: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationWork {
    Probe {
        replica: u8,
        nonce: u64,
        through: u64,
    },
    Replay {
        replica: u8,
        from_op: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationProgress {
    Busy,
    Ready { received_op: u64 },
}

impl ReplicationFlow {
    pub fn new(replica_count: u8) -> Self {
        Self {
            epoch: None,
            nonce: 0,
            peers: vec![PeerReplication::default(); usize::from(replica_count)],
        }
    }

    fn in_epoch(&mut self, view: u32, incarnation: u64) {
        if self.epoch != Some((view, incarnation)) {
            self.epoch = Some((view, incarnation));
            self.peers.fill(PeerReplication::default());
        }
    }

    fn admit(&mut self, replica: u8, op: u64, size: u64, replay: bool) -> bool {
        let peer = &mut self.peers[usize::from(replica)];
        if peer.probe.is_some() {
            return false;
        }
        if !replay && peer.replay_from.is_some_and(|from| from != op) {
            return false;
        }
        // One legal frame larger than the window must still make progress.
        if peer.bytes > 0 && peer.bytes.saturating_add(size) > REPLICATION_BATCH_BYTES_MAX {
            peer.replay_from.get_or_insert(op);
            peer.due = 0;
            return false;
        }
        true
    }

    fn sent(&mut self, replica: u8, op: u64, size: u64, replay: bool, now: u64) {
        let peer = &mut self.peers[usize::from(replica)];
        if peer.bytes == 0 {
            peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
        }
        peer.bytes = peer.bytes.saturating_add(size);
        peer.sent_through = peer.sent_through.max(op);
        if replay || peer.replay_from.is_some() {
            peer.replay_from = Some(op.saturating_add(1));
        }
    }

    fn failed(&mut self, replica: u8, op: u64) {
        let peer = &mut self.peers[usize::from(replica)];
        peer.replay_from = Some(peer.replay_from.map_or(op, |from| from.min(op)));
        peer.due = 0;
    }

    fn work(&mut self, replica: u8, head: u64, now: u64) -> Option<ReplicationWork> {
        let peer = &mut self.peers[usize::from(replica)];
        if now < peer.retry_at && peer.bytes == 0 {
            return None;
        }
        if let Some((nonce, through)) = peer.probe {
            if now < peer.due {
                return None;
            }
            peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
            return Some(ReplicationWork::Probe {
                replica,
                nonce,
                through,
            });
        }
        if peer.replay_from.is_some_and(|from| from > head) {
            peer.replay_from = None;
        }
        if now >= peer.due && (peer.bytes > 0 || peer.replay_from.is_some()) {
            self.nonce = self.nonce.wrapping_add(1).max(1);
            let through = peer.sent_through;
            peer.probe = Some((self.nonce, through));
            peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
            return Some(ReplicationWork::Probe {
                replica,
                nonce: self.nonce,
                through,
            });
        }
        peer.replay_from
            .map(|from_op| ReplicationWork::Replay { replica, from_op })
    }

    fn reply(&mut self, header: &PrepareProgressHeader, head: u64) -> Option<ReplicationProgress> {
        let peer = &mut self.peers[usize::from(header.replica)];
        if peer.probe != Some((header.nonce, header.observed_op)) {
            return None;
        }
        if header.ready == 0 {
            // Readiness can change before the retry interval for a lost probe expires.
            peer.due = 0;
            return Some(ReplicationProgress::Busy);
        }
        peer.probe = None;
        peer.bytes = 0;
        peer.retry_at = 0;
        peer.sent_through = header.received_op.min(head);
        peer.replay_from = (peer.sent_through < head).then(|| peer.sent_through + 1);
        peer.due = u64::MAX;
        Some(ReplicationProgress::Ready {
            received_op: peer.sent_through,
        })
    }
}

impl<B, P> VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    pub fn admit_replication(&self, replica: u8, op: u64, size: u64, replay: bool) -> bool {
        if !self.is_acting_primary() || self.has_observed_newer_view() || self.is_transferring() {
            return false;
        }
        let mut flow = self.replication.borrow_mut();
        flow.in_epoch(self.view(), self.partition_incarnation());
        flow.admit(replica, op, size, replay)
    }

    pub fn record_replication_send(&self, replica: u8, op: u64, size: u64, replay: bool) {
        self.replication.borrow_mut().sent(
            replica,
            op,
            size,
            replay,
            self.message_bus().monotonic_micros(),
        );
    }

    pub fn record_replication_failure(&self, replica: u8, op: u64) {
        self.replication.borrow_mut().failed(replica, op);
    }

    pub fn defer_replication(&self, replica: u8) {
        let mut flow = self.replication.borrow_mut();
        let peer = &mut flow.peers[usize::from(replica)];
        peer.due = 0;
        peer.retry_at = self
            .message_bus()
            .monotonic_micros()
            .saturating_add(PROGRESS_INTERVAL_MICROS);
    }

    pub fn replication_work(&self, replica: u8) -> Option<ReplicationWork> {
        if !self.is_acting_primary()
            || self.has_observed_newer_view()
            || self.is_transferring()
            || self.has_untrusted_log()
        {
            return None;
        }
        let mut flow = self.replication.borrow_mut();
        flow.in_epoch(self.view(), self.partition_incarnation());
        flow.work(
            replica,
            self.sequencer().current_sequence(),
            self.message_bus().monotonic_micros(),
        )
    }

    pub fn handle_prepare_progress(
        &self,
        header: &PrepareProgressHeader,
    ) -> Option<ReplicationProgress> {
        if !self.is_acting_primary()
            || self.has_observed_newer_view()
            || self.is_transferring()
            || header.kind != PrepareProgressHeader::REPLY
            || header.cluster != self.cluster()
            || header.group != self.group()
            || header.view != self.view()
            || header.partition_incarnation != self.partition_incarnation()
            || header.replica >= self.replica_count()
            || header.replica == self.replica()
        {
            return None;
        }
        let mut flow = self.replication.borrow_mut();
        flow.in_epoch(self.view(), self.partition_incarnation());
        let progress = flow.reply(header, self.sequencer().current_sequence())?;
        if let ReplicationProgress::Ready { received_op } = progress {
            self.note_backup_ack(header.replica, received_op);
        }
        Some(progress)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PROGRESS_INTERVAL_MICROS, REPLICATION_BATCH_BYTES_MAX, ReplicationFlow, ReplicationWork,
    };
    use crate::{LocalPipeline, Sequencer, VsrConsensus, impls::test_bus::NoopBus};
    use iggy_binary_protocol::{Command, ConsensusHeader, HEADER_SIZE, PrepareProgressHeader};
    use server_common::Message;

    fn reply(nonce: u64, observed_op: u64, received_op: u64) -> Message<PrepareProgressHeader> {
        Message::<PrepareProgressHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut PrepareProgressHeader| {
                header.command = Command::PrepareProgress;
                header.cluster = 1;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.replica = 1;
                header.kind = PrepareProgressHeader::REPLY;
                header.ready = 1;
                header.nonce = nonce;
                header.observed_op = observed_op;
                header.received_op = received_op;
                header.seal();
            },
        )
    }

    #[test]
    fn given_a_full_replay_batch_when_deferred_should_drain_credit_before_history_backoff() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(2);
        assert!(consensus.admit_replication(1, 1, REPLICATION_BATCH_BYTES_MAX, true));
        consensus.record_replication_send(1, 1, REPLICATION_BATCH_BYTES_MAX, true);
        assert!(!consensus.admit_replication(1, 2, 1, true));
        consensus.defer_replication(1);
        let Some(ReplicationWork::Probe { nonce, through, .. }) = consensus.replication_work(1)
        else {
            panic!("a full batch must drain without the unavailable-history delay");
        };
        assert!(
            consensus
                .handle_prepare_progress(reply(nonce, through, 1).header())
                .is_some()
        );
        assert!(
            matches!(
                consensus.replication_work(1),
                Some(ReplicationWork::Replay { from_op: 2, .. })
            ),
            "a drained batch must resume replay without the stale history delay"
        );

        assert!(consensus.admit_replication(2, 1, 1, true));
        consensus.record_replication_failure(2, 1);
        consensus.defer_replication(2);
        assert!(
            consensus.replication_work(2).is_none(),
            "unavailable history without outstanding bytes must still back off"
        );
    }

    #[test]
    fn given_a_full_peer_window_when_its_probe_is_delayed_should_bound_bytes_and_keep_other_peers_sending()
     {
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        assert!(flow.admit(1, 1, REPLICATION_BATCH_BYTES_MAX, false));
        flow.sent(1, 1, REPLICATION_BATCH_BYTES_MAX, false, 0);
        assert!(!flow.admit(1, 2, 1, false));
        let Some(ReplicationWork::Probe { nonce, through, .. }) = flow.work(1, 2, 0) else {
            panic!("full window must probe");
        };
        assert!(!flow.admit(1, 2, 1, true));
        assert!(
            flow.admit(2, 2, 1, false),
            "a delayed peer must not stop another peer"
        );
        assert!(
            matches!(flow.work(1, 3, PROGRESS_INTERVAL_MICROS), Some(ReplicationWork::Probe { nonce: repeated, through: 1, .. }) if repeated == nonce)
        );
        let _ = flow.reply(reply(nonce, through, 0).header(), 3);
        assert!(matches!(
            flow.work(1, 3, PROGRESS_INTERVAL_MICROS),
            Some(ReplicationWork::Replay { from_op: 1, .. })
        ));
        assert!(
            !flow.admit(1, 4, 1, false),
            "live traffic cannot overtake replay"
        );
        for op in 1..=4 {
            assert!(flow.admit(1, op, 1, true));
            flow.sent(1, op, 1, true, PROGRESS_INTERVAL_MICROS);
        }
        assert!(flow.work(1, 4, PROGRESS_INTERVAL_MICROS).is_none());
        assert!(
            flow.admit(1, 5, 1, false),
            "enqueueing through the moving head restores live delivery"
        );
    }

    #[test]
    fn given_an_inflight_local_prepare_when_a_probe_finishes_should_join_replay_without_waiting_for_a_tick()
     {
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        flow.sent(1, 1, 100, false, 0);
        let Some(ReplicationWork::Probe { nonce, through, .. }) =
            flow.work(1, 2, PROGRESS_INTERVAL_MICROS)
        else {
            panic!("the sent batch must be probed");
        };
        let _ = flow.reply(reply(nonce, through, 1).header(), 2);
        assert!(
            !flow.admit(1, 3, 100, false),
            "a fresh op cannot skip replay"
        );
        assert!(
            flow.admit(1, 2, 100, false),
            "the next queued local prepare must join the ordered stream immediately"
        );
        flow.sent(1, 2, 100, false, PROGRESS_INTERVAL_MICROS);
        assert!(
            flow.admit(1, 3, 100, false),
            "continuous live traffic must advance the replay cursor without a timer"
        );
    }

    #[test]
    fn given_an_enqueue_failure_when_a_correlated_probe_finds_a_gap_should_replay_the_committed_suffix()
     {
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        flow.sent(1, 3, 100, false, 0);
        flow.failed(1, 4);
        let Some(ReplicationWork::Probe { nonce, through, .. }) = flow.work(1, 6, 0) else {
            panic!("failed enqueue must probe");
        };
        let _ = flow.reply(reply(nonce + 1, through, 6).header(), 6);
        assert!(
            !flow.admit(1, 7, 1, false),
            "an uncorrelated reply releases no credit"
        );
        let busy =
            reply(nonce, through, 6).transmute_header(|old, header: &mut PrepareProgressHeader| {
                *header = old;
                header.ready = 0;
            });
        assert_eq!(
            flow.reply(busy.header(), 6),
            Some(super::ReplicationProgress::Busy)
        );
        assert!(
            !flow.admit(1, 7, 1, false),
            "a busy replica does not establish progress"
        );
        assert!(
            matches!(flow.work(1, 6, 1), Some(ReplicationWork::Probe { nonce: repeated, through: observed, .. }) if repeated == nonce && observed == through),
            "a busy reply must recheck readiness on the next tick without releasing its batch credit"
        );
        let _ = flow.reply(reply(nonce, through, 2).header(), 6);
        assert!(matches!(
            flow.work(1, 6, 1),
            Some(ReplicationWork::Replay { from_op: 3, .. })
        ));
        flow.in_epoch(1, 1);
        let _ = flow.reply(reply(nonce, through, 2).header(), 6);
        assert!(
            flow.admit(1, 7, 1, false),
            "a new view discards the previous sender window"
        );
    }

    #[test]
    fn given_progress_replies_when_the_view_incarnation_or_nonce_is_stale_should_neither_release_credit_nor_commit()
     {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(2);
        assert!(consensus.admit_replication(1, 1, 1, false));
        consensus.record_replication_send(1, 1, 1, false);
        consensus.record_replication_failure(1, 2);
        let Some(ReplicationWork::Probe { nonce, through, .. }) = consensus.replication_work(1)
        else {
            panic!("failed enqueue must probe");
        };
        for stale in ["view", "incarnation", "nonce", "peer", "cluster", "group"] {
            let message = reply(nonce, through, 2).transmute_header(
                |old, header: &mut PrepareProgressHeader| {
                    *header = old;
                    match stale {
                        "view" => header.view += 1,
                        "incarnation" => header.partition_incarnation += 1,
                        "nonce" => header.nonce += 1,
                        "peer" => header.replica = 2,
                        "cluster" => header.cluster += 1,
                        "group" => header.group += 1,
                        _ => unreachable!(),
                    }
                },
            );
            assert_eq!(consensus.handle_prepare_progress(message.header()), None);
            assert!(
                !consensus.admit_replication(1, 3, 1, false),
                "accepted stale {stale}"
            );
        }
        let _ = consensus.handle_prepare_progress(reply(nonce, through, 2).header());
        assert!(consensus.admit_replication(1, 3, 1, false));
        assert_eq!(consensus.commit_min(), 0);
        assert_eq!(consensus.commit_max(), 0);
    }
}
