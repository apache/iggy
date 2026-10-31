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
use iggy_binary_protocol::{Command, ConsensusHeader, HEADER_SIZE, PrepareProgressHeader};
use message_bus::{MessageBus, SendError};
use server_common::{MESSAGE_ALIGN, Message, iobuf::Frozen};
use std::mem::take;

pub const REPLICATION_BATCH_BYTES_MAX: u64 = 32 * 1024 * 1024;
/// A checkpoint at half the window returns its credit before the window fills,
/// as long as the backup consumes half a window within one round trip.
const CHECKPOINT_BYTES: u64 = REPLICATION_BATCH_BYTES_MAX / 2;
const PROGRESS_INTERVAL_MICROS: u64 = 1_000_000;
/// Probes and their replies are header-only frames.
#[allow(clippy::cast_possible_truncation)]
const PROGRESS_FRAME_BYTES: u32 = HEADER_SIZE as u32;

/// Bounds the prepare bytes in flight to each backup by the credit that
/// checkpoints return.
///
/// A checkpoint is a progress probe on the same FIFO link as the prepares, so
/// its reply proves that the backup consumed every frame sent before it. The
/// reply credits only that prefix, and no send waits for it. Payloads remain in
/// the journal, never in this per-peer state.
#[derive(Debug)]
pub struct ReplicationFlow {
    epoch: Option<(u32, u64)>,
    nonce: u64,
    peers: Vec<PeerReplication>,
    stats: ReplicationStats,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerReplication {
    /// Every copy sent in this epoch, retransmits included.
    sent_bytes: u64,
    /// The prefix of `sent_bytes` that a checkpoint reply proved consumed.
    credited_bytes: u64,
    sent_through: u64,
    /// The next op of the ordered resend. Live sends wait until it reaches them.
    replay_from: Option<u64>,
    checkpoint: Option<Checkpoint>,
    /// The backup drops or cannot acknowledge prepares, so nothing is sent.
    busy: bool,
    /// When the tick sends a new checkpoint or resends the outstanding one.
    due: u64,
    /// Replay waits until then after history was unavailable.
    retry_at: u64,
}

impl PeerReplication {
    const fn outstanding(&self) -> u64 {
        self.sent_bytes.saturating_sub(self.credited_bytes)
    }

    /// Whether the window takes a frame of `size` once `queued` more bytes are
    /// sent. An empty window takes any frame, so one legal frame larger than
    /// the window still makes progress.
    const fn window_takes(&self, queued: u64, size: u64) -> bool {
        let outstanding = self.outstanding().saturating_add(queued);
        outstanding == 0 || outstanding.saturating_add(size) <= REPLICATION_BATCH_BYTES_MAX
    }
}

#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    nonce: u64,
    through: u64,
    bytes: u64,
    /// A gap at or above the cursor of issue time was still waiting for replay,
    /// so the reply cannot move the cursor back to it.
    replay_from: Option<u64>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSend {
    /// A new op from the primary's prepare path.
    Live,
    /// An ordered resend from the replay cursor.
    Replay,
    /// A prepare-timeout resend of one op. It never moves the cursor, so a
    /// healthy backup gets no replay.
    Retransmit,
}

/// Replication events since the previous [`VsrConsensus::take_replication_stats`].
/// Both stay at zero while every backup keeps up.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationStats {
    /// Live prepares that the window, the replay cursor or a busy backup refused.
    pub live_refusals: u64,
    /// Prepares sent by replay.
    pub replayed: u64,
}

/// A backup's answer to a checkpoint.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackupPrefix {
    /// Every op through it is applied, or is in the journal with proof in the
    /// current view. An op above the commit need not be durable yet.
    pub held_op: u64,
    /// The part of the held prefix that also meets the `PrepareOk` rules.
    pub received_op: u64,
}

impl ReplicationFlow {
    pub fn new(replica_count: u8) -> Self {
        Self {
            epoch: None,
            nonce: 0,
            peers: vec![PeerReplication::default(); usize::from(replica_count)],
            stats: ReplicationStats::default(),
        }
    }

    fn in_epoch(&mut self, view: u32, incarnation: u64) {
        if self.epoch != Some((view, incarnation)) {
            self.epoch = Some((view, incarnation));
            self.peers.fill(PeerReplication::default());
        }
    }

    fn admit(&mut self, replica: u8, op: u64, size: u64, send: ReplicationSend) -> bool {
        let peer = &mut self.peers[usize::from(replica)];
        let window_full = !peer.window_takes(0, size);
        let out_of_order = match send {
            ReplicationSend::Live => peer.replay_from.is_some_and(|from| from != op),
            ReplicationSend::Replay => peer.replay_from != Some(op),
            ReplicationSend::Retransmit => false,
        };
        if !peer.busy && !window_full && !out_of_order {
            return true;
        }
        if window_full && peer.checkpoint.is_none() {
            peer.due = 0;
        }
        if send == ReplicationSend::Live {
            peer.replay_from.get_or_insert(op);
            self.stats.live_refusals += 1;
        }
        false
    }

    /// Returns the checkpoint that must follow this frame on its link.
    fn sent(
        &mut self,
        replica: u8,
        op: u64,
        size: u64,
        send: ReplicationSend,
        now: u64,
    ) -> Option<ReplicationWork> {
        let peer = &mut self.peers[usize::from(replica)];
        if peer.outstanding() == 0 && peer.checkpoint.is_none() {
            peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
        }
        peer.sent_bytes = peer.sent_bytes.saturating_add(size);
        peer.sent_through = peer.sent_through.max(op);
        if send != ReplicationSend::Retransmit && peer.replay_from == Some(op) {
            peer.replay_from = Some(op.saturating_add(1));
        }
        if send == ReplicationSend::Replay {
            self.stats.replayed += 1;
        }
        let due = peer.checkpoint.is_none() && peer.outstanding() >= CHECKPOINT_BYTES;
        due.then(|| self.checkpoint(replica, now))
    }

    fn failed(&mut self, replica: u8, op: u64) {
        let peer = &mut self.peers[usize::from(replica)];
        peer.replay_from = Some(peer.replay_from.map_or(op, |from| from.min(op)));
        peer.due = 0;
    }

    fn defer(&mut self, replica: u8, now: u64) {
        let peer = &mut self.peers[usize::from(replica)];
        peer.due = 0;
        peer.retry_at = now.saturating_add(PROGRESS_INTERVAL_MICROS);
    }

    /// A checkpoint counts only once its frame is queued. Dropping one that the
    /// peer queue refused lets the next send issue a new one, instead of the
    /// window waiting for the interval to end.
    fn probe_failed(&mut self, replica: u8, nonce: u64) {
        let peer = &mut self.peers[usize::from(replica)];
        if peer
            .checkpoint
            .is_some_and(|checkpoint| checkpoint.nonce == nonce)
        {
            peer.checkpoint = None;
        }
    }

    fn checkpoint(&mut self, replica: u8, now: u64) -> ReplicationWork {
        self.nonce = self.nonce.wrapping_add(1).max(1);
        let peer = &mut self.peers[usize::from(replica)];
        peer.checkpoint = Some(Checkpoint {
            nonce: self.nonce,
            through: peer.sent_through,
            bytes: peer.sent_bytes,
            replay_from: peer.replay_from,
        });
        peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
        ReplicationWork::Probe {
            replica,
            nonce: self.nonce,
            through: peer.sent_through,
        }
    }

    fn work(&mut self, replica: u8, head: u64, now: u64) -> Option<ReplicationWork> {
        let peer = &mut self.peers[usize::from(replica)];
        let backoff = now < peer.retry_at;
        if backoff && peer.outstanding() == 0 {
            return None;
        }
        if peer.replay_from.is_some_and(|from| from > head) {
            peer.replay_from = None;
        }
        if now >= peer.due {
            if let Some(checkpoint) = peer.checkpoint {
                // A lost checkpoint goes out again with its nonce and prefix.
                peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
                return Some(ReplicationWork::Probe {
                    replica,
                    nonce: checkpoint.nonce,
                    through: checkpoint.through,
                });
            }
            if peer.outstanding() > 0 || peer.replay_from.is_some() || peer.busy {
                return Some(self.checkpoint(replica, now));
            }
        }
        if backoff || peer.busy {
            return None;
        }
        peer.replay_from
            .map(|from_op| ReplicationWork::Replay { replica, from_op })
    }

    fn reply(
        &mut self,
        header: &PrepareProgressHeader,
        head: u64,
        now: u64,
    ) -> Option<ReplicationProgress> {
        let peer = &mut self.peers[usize::from(header.replica)];
        let checkpoint = peer.checkpoint?;
        let ready = header.ready != 0;
        if (checkpoint.nonce, checkpoint.through) != (header.nonce, header.observed_op)
            || (ready && header.received_op > header.held_op)
        {
            return None;
        }
        peer.checkpoint = None;
        // The reply proves FIFO consumption, not readiness, so Busy credits too.
        peer.credited_bytes = peer.credited_bytes.max(checkpoint.bytes);
        if !ready {
            peer.busy = true;
            let next = peer.sent_through.saturating_add(1);
            peer.replay_from.get_or_insert(next);
            // Readiness can change before the interval of a lost probe ends.
            peer.due = 0;
            return Some(ReplicationProgress::Busy);
        }
        peer.busy = false;
        peer.retry_at = 0;
        peer.due = now.saturating_add(PROGRESS_INTERVAL_MICROS);
        let held_op = header.held_op.min(head);
        // A newer head alone never starts a replay.
        let gap = held_op < checkpoint.through
            && checkpoint.replay_from.is_none_or(|from| held_op + 1 < from);
        // Repair or a state transfer can carry the backup past the cursor. The
        // primary can drop the op at the cursor by then, and a replay from it
        // would block live sends for good.
        if gap || peer.replay_from.is_some_and(|from| from <= held_op) {
            peer.replay_from = Some(held_op + 1);
        }
        Some(ReplicationProgress::Ready {
            received_op: header.received_op.min(head),
        })
    }
}

impl<B, P> VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    /// Send one prepare copy to a backup under its replication window. Once
    /// half the window is outstanding, a checkpoint follows the copy.
    ///
    /// Returns `Ok(false)` when the flow refuses the copy.
    ///
    /// # Errors
    ///
    /// Returns the bus error when the copy cannot be enqueued.
    #[allow(clippy::future_not_send)]
    pub async fn send_replicated_prepare(
        &self,
        replica: u8,
        op: u64,
        prepare: Frozen<MESSAGE_ALIGN>,
        send: ReplicationSend,
    ) -> Result<bool, SendError> {
        let size = prepare.len() as u64;
        if !self.admit_replication(replica, op, size, send) {
            return Ok(false);
        }
        if let Err(error) = self.message_bus().send_to_replica(replica, prepare).await {
            self.record_replication_failure(replica, op);
            return Err(error);
        }
        if let Some(ReplicationWork::Probe { nonce, through, .. }) =
            self.record_replication_send(replica, op, size, send)
        {
            self.send_replication_probe(replica, nonce, through).await;
        }
        Ok(true)
    }

    pub fn admit_replication(
        &self,
        replica: u8,
        op: u64,
        size: u64,
        send: ReplicationSend,
    ) -> bool {
        if !self.is_acting_primary() || self.has_observed_newer_view() || self.is_transferring() {
            return false;
        }
        let mut flow = self.replication.borrow_mut();
        flow.in_epoch(self.view(), self.partition_incarnation());
        flow.admit(replica, op, size, send)
    }

    /// Whether the window of `replica` takes a frame of `size` behind `queued`
    /// bytes that are not sent yet. A replay asks before it reads a body, so
    /// it reads no body that the window of [`Self::send_replicated_prepare`]
    /// then refuses.
    pub fn replication_window_takes(&self, replica: u8, queued: u64, size: u64) -> bool {
        self.replication.borrow().peers[usize::from(replica)].window_takes(queued, size)
    }

    /// Returns the checkpoint that must follow the sent copy.
    #[must_use]
    pub fn record_replication_send(
        &self,
        replica: u8,
        op: u64,
        size: u64,
        send: ReplicationSend,
    ) -> Option<ReplicationWork> {
        self.replication.borrow_mut().sent(
            replica,
            op,
            size,
            send,
            self.message_bus().monotonic_micros(),
        )
    }

    pub fn record_replication_failure(&self, replica: u8, op: u64) {
        self.replication.borrow_mut().failed(replica, op);
    }

    pub fn defer_replication(&self, replica: u8) {
        let now = self.message_bus().monotonic_micros();
        self.replication.borrow_mut().defer(replica, now);
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

    /// Send the probe of a [`ReplicationWork::Probe`] behind every frame
    /// already queued for `replica`.
    #[allow(clippy::future_not_send)]
    pub async fn send_replication_probe(&self, replica: u8, nonce: u64, through: u64) {
        let probe = Message::<PrepareProgressHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut PrepareProgressHeader| {
                header.command = Command::PrepareProgress;
                header.size = PROGRESS_FRAME_BYTES;
                header.cluster = self.cluster();
                header.view = self.view();
                header.replica = self.replica();
                header.group = self.group();
                header.partition_incarnation = self.partition_incarnation();
                header.nonce = nonce;
                header.observed_op = through;
                header.commit_op = self.commit_min();
                header.kind = PrepareProgressHeader::PROBE;
                header.seal();
            },
        );
        if self
            .message_bus()
            .send_to_replica(replica, probe.into_generic().into_frozen())
            .await
            .is_err()
        {
            self.replication.borrow_mut().probe_failed(replica, nonce);
        }
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
        let progress = flow.reply(
            header,
            self.sequencer().current_sequence(),
            self.message_bus().monotonic_micros(),
        )?;
        if let ReplicationProgress::Ready { received_op } = progress {
            self.note_backup_ack(header.replica, received_op);
        }
        Some(progress)
    }

    pub fn take_replication_stats(&self) -> ReplicationStats {
        take(&mut self.replication.borrow_mut().stats)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKPOINT_BYTES, PROGRESS_INTERVAL_MICROS, REPLICATION_BATCH_BYTES_MAX, ReplicationFlow,
        ReplicationProgress, ReplicationSend, ReplicationStats, ReplicationWork,
    };
    use crate::{LocalPipeline, Sequencer, VsrConsensus, impls::test_bus::NoopBus};
    use iggy_binary_protocol::{Command, ConsensusHeader, HEADER_SIZE, PrepareProgressHeader};
    use server_common::Message;
    use std::collections::VecDeque;

    const MIB: u64 = 1024 * 1024;

    fn ready(
        replica: u8,
        nonce: u64,
        observed_op: u64,
        held_op: u64,
        received_op: u64,
    ) -> Message<PrepareProgressHeader> {
        Message::<PrepareProgressHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut PrepareProgressHeader| {
                header.command = Command::PrepareProgress;
                header.cluster = 1;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.replica = replica;
                header.kind = PrepareProgressHeader::REPLY;
                header.ready = 1;
                header.nonce = nonce;
                header.observed_op = observed_op;
                header.held_op = held_op;
                header.received_op = received_op;
                header.seal();
            },
        )
    }

    fn busy(replica: u8, nonce: u64, observed_op: u64) -> Message<PrepareProgressHeader> {
        ready(replica, nonce, observed_op, 0, 0).transmute_header(
            |old, header: &mut PrepareProgressHeader| {
                *header = old;
                header.ready = 0;
            },
        )
    }

    /// Sends a copy the flow must admit, and returns the checkpoint behind it.
    fn send(
        flow: &mut ReplicationFlow,
        replica: u8,
        op: u64,
        size: u64,
        send: ReplicationSend,
        now: u64,
    ) -> Option<ReplicationWork> {
        assert!(
            flow.admit(replica, op, size, send),
            "{send:?} op {op} to replica {replica} was refused"
        );
        flow.sent(replica, op, size, send, now)
    }

    /// Replica 2 answers like a persisted topic whose WAL write of the last op
    /// is not durable yet. Replies arrive two ops after their checkpoint, so
    /// each checkpoint is out while the stream continues.
    #[test]
    fn given_healthy_backups_when_the_stream_passes_many_windows_should_never_refuse_or_replay() {
        const SIZE: u64 = MIB;
        const OPS: u64 = 10 * REPLICATION_BATCH_BYTES_MAX / SIZE;
        const REPLY_DELAY_OPS: u64 = 2;
        const OP_INTERVAL_MICROS: u64 = 10_000;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        let mut replies = VecDeque::new();
        for op in 1..=OPS {
            let now = op * OP_INTERVAL_MICROS;
            for replica in 1..3 {
                let checkpoint = send(&mut flow, replica, op, SIZE, ReplicationSend::Live, now);
                for work in [checkpoint, flow.work(replica, op, now)]
                    .into_iter()
                    .flatten()
                {
                    let ReplicationWork::Probe { nonce, through, .. } = work else {
                        panic!("replica {replica} replays at op {op}: {work:?}");
                    };
                    replies.push_back((op + REPLY_DELAY_OPS, replica, nonce, through));
                }
            }
            while let Some(&(at, replica, nonce, through)) = replies.front()
                && at <= op
            {
                replies.pop_front();
                let received = if replica == 2 { through - 1 } else { through };
                let reply = ready(replica, nonce, through, through, received);
                assert!(matches!(
                    flow.reply(reply.header(), op, now),
                    Some(ReplicationProgress::Ready { .. })
                ));
            }
        }
        assert_eq!(flow.stats, ReplicationStats::default());
    }

    #[test]
    fn given_bytes_sent_after_a_checkpoint_when_its_reply_arrives_should_credit_only_that_prefix() {
        const SIZE: u64 = CHECKPOINT_BYTES / 2;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        assert!(send(&mut flow, 1, 1, SIZE, ReplicationSend::Live, 0).is_none());
        let Some(ReplicationWork::Probe { nonce, through, .. }) =
            send(&mut flow, 1, 2, SIZE, ReplicationSend::Live, 0)
        else {
            panic!("half a window outstanding must send a checkpoint");
        };
        assert!(send(&mut flow, 1, 3, SIZE, ReplicationSend::Live, 0).is_none());
        assert!(send(&mut flow, 1, 4, SIZE, ReplicationSend::Live, 0).is_none());
        assert!(!flow.admit(1, 4, 1, ReplicationSend::Retransmit));
        assert!(
            matches!(
                flow.work(1, 4, PROGRESS_INTERVAL_MICROS),
                Some(ReplicationWork::Probe { nonce: resent, through: 2, .. }) if resent == nonce
            ),
            "a lost checkpoint goes out again with its nonce and prefix"
        );
        for stale in [
            ready(1, nonce + 1, through, through, through),
            ready(1, nonce, through + 1, through, through),
            ready(1, nonce, through, through - 1, through),
        ] {
            assert_eq!(flow.reply(stale.header(), 4, 0), None);
        }
        let reply = ready(1, nonce, through, through, through);
        assert!(flow.reply(reply.header(), 4, 0).is_some());
        assert_eq!(
            flow.reply(reply.header(), 4, 0),
            None,
            "a duplicate reply credits nothing"
        );
        assert!(flow.admit(1, 4, CHECKPOINT_BYTES, ReplicationSend::Retransmit));
        assert!(
            !flow.admit(1, 4, CHECKPOINT_BYTES + 1, ReplicationSend::Retransmit),
            "bytes sent after the checkpoint stay charged"
        );
    }

    #[test]
    fn given_a_checkpoint_the_peer_queue_refused_when_a_copy_follows_should_send_a_new_one() {
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        let Some(ReplicationWork::Probe { nonce: refused, .. }) =
            send(&mut flow, 1, 1, CHECKPOINT_BYTES, ReplicationSend::Live, 0)
        else {
            panic!("half a window outstanding must send a checkpoint");
        };
        flow.probe_failed(1, refused);
        let Some(ReplicationWork::Probe {
            nonce, through: 2, ..
        }) = send(&mut flow, 1, 2, 1, ReplicationSend::Live, 0)
        else {
            panic!("the next copy must carry a new checkpoint, not wait for the interval");
        };
        flow.probe_failed(1, refused);
        assert_eq!(flow.reply(ready(1, refused, 1, 1, 1).header(), 2, 0), None);
        assert!(
            flow.reply(ready(1, nonce, 2, 2, 2).header(), 2, 0)
                .is_some(),
            "a failure of an older checkpoint must keep the new one"
        );
        assert_eq!(flow.peers[1].outstanding(), 0);
    }

    #[test]
    fn given_a_lost_prepare_when_a_checkpoint_finds_the_gap_should_replay_it_before_live_sends() {
        const SIZE: u64 = 100;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        for op in 1..=3 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Live, 0).is_none());
        }
        let mut now = PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe {
            nonce, through: 3, ..
        }) = flow.work(1, 3, now)
        else {
            panic!("the timer must checkpoint outstanding bytes");
        };
        let lost_second = ready(1, nonce, 3, 1, 1);
        assert!(flow.reply(lost_second.header(), 3, now).is_some());
        assert!(!flow.admit(1, 4, SIZE, ReplicationSend::Live));
        now += PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe {
            nonce: early,
            through: 3,
            ..
        }) = flow.work(1, 4, now)
        else {
            panic!("the timer must checkpoint a waiting replay");
        };
        assert!(
            matches!(
                flow.work(1, 4, now),
                Some(ReplicationWork::Replay { from_op: 2, .. })
            ),
            "replay must not wait for the checkpoint"
        );
        for op in 2..=4 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Replay, now).is_none());
        }
        let before_replay = ready(1, early, 3, 1, 1);
        assert!(flow.reply(before_replay.header(), 4, now).is_some());
        assert!(
            flow.admit(1, 5, SIZE, ReplicationSend::Live),
            "a checkpoint sent before the replay must not move the cursor back"
        );
        assert_eq!(
            flow.stats,
            ReplicationStats {
                live_refusals: 1,
                replayed: 3
            }
        );
    }

    #[test]
    fn given_a_healthy_backup_when_a_prepare_retransmits_should_charge_it_without_a_replay() {
        const SIZE: u64 = 100;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        for op in 1..=2 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Live, 0).is_none());
        }
        assert!(send(&mut flow, 1, 1, SIZE, ReplicationSend::Retransmit, 0).is_none());
        assert_eq!(flow.peers[1].outstanding(), 3 * SIZE);
        assert!(flow.admit(1, 3, SIZE, ReplicationSend::Live));
        assert!(flow.work(1, 3, 0).is_none());
        assert_eq!(flow.stats, ReplicationStats::default());
    }

    #[test]
    fn given_a_busy_backup_when_prepares_arrive_should_send_nothing_until_a_ready_reply() {
        const SIZE: u64 = 100;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        for op in 1..=2 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Live, 0).is_none());
        }
        let now = PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe { nonce, through, .. }) = flow.work(1, 2, now) else {
            panic!("the timer must checkpoint outstanding bytes");
        };
        assert_eq!(
            flow.reply(busy(1, nonce, through).header(), 2, now),
            Some(ReplicationProgress::Busy)
        );
        assert_eq!(
            flow.peers[1].outstanding(),
            0,
            "a busy reply still proves FIFO consumption"
        );
        assert!(
            !flow.admit(1, 3, SIZE, ReplicationSend::Live),
            "a live op equal to the cursor waits too"
        );
        assert!(!flow.admit(1, 2, SIZE, ReplicationSend::Retransmit));
        let Some(ReplicationWork::Probe {
            nonce: recheck,
            through: 2,
            ..
        }) = flow.work(1, 3, now)
        else {
            panic!("the next tick must ask a busy backup again");
        };
        assert!(
            flow.work(1, 3, now).is_none(),
            "a busy backup gets no replay"
        );
        assert!(
            flow.reply(ready(1, recheck, 2, 2, 2).header(), 3, now)
                .is_some()
        );
        assert!(
            flow.admit(1, 3, SIZE, ReplicationSend::Live),
            "a ready reply resumes after the held prefix"
        );
    }

    /// Busy replies during a state transfer park the cursor on an op that the
    /// primary later drops from memory. A replay from there never succeeds, so
    /// the cursor would refuse every live send.
    #[test]
    fn given_a_backup_that_repair_moved_past_the_cursor_when_it_answers_ready_should_replay_after_its_held_prefix()
     {
        const SIZE: u64 = 100;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        for op in 1..=3 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Live, 0).is_none());
        }
        let mut now = PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe { nonce, through, .. }) = flow.work(1, 3, now) else {
            panic!("the timer must checkpoint outstanding bytes");
        };
        assert_eq!(
            flow.reply(busy(1, nonce, through).header(), 3, now),
            Some(ReplicationProgress::Busy)
        );
        assert!(!flow.admit(1, 4, SIZE, ReplicationSend::Live));
        let Some(ReplicationWork::Probe {
            nonce: repaired,
            through: 3,
            ..
        }) = flow.work(1, 10, now)
        else {
            panic!("the next tick must ask a busy backup again");
        };
        assert_eq!(
            flow.reply(ready(1, repaired, 3, 8, 8).header(), 10, now),
            Some(ReplicationProgress::Ready { received_op: 8 })
        );
        assert!(
            matches!(
                flow.work(1, 10, now),
                Some(ReplicationWork::Replay { from_op: 9, .. })
            ),
            "replay must resume after the held prefix, not at the parked cursor"
        );
        for op in 9..=10 {
            assert!(send(&mut flow, 1, op, SIZE, ReplicationSend::Replay, now).is_none());
        }
        assert!(send(&mut flow, 1, 11, SIZE, ReplicationSend::Live, now).is_none());

        now += PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe {
            nonce: installed,
            through: 11,
            ..
        }) = flow.work(1, 11, now)
        else {
            panic!("the timer must checkpoint outstanding bytes");
        };
        assert!(
            flow.reply(ready(1, installed, 11, 5, 5).header(), 11, now)
                .is_some()
        );
        assert!(
            matches!(
                flow.work(1, 11, now),
                Some(ReplicationWork::Replay { from_op: 6, .. })
            ),
            "an install that drops held ops must restart replay at the first lost op"
        );

        now += PROGRESS_INTERVAL_MICROS;
        let Some(ReplicationWork::Probe {
            nonce: caught_up,
            through: 11,
            ..
        }) = flow.work(1, 11, now)
        else {
            panic!("the timer must checkpoint a waiting replay");
        };
        assert!(
            flow.reply(ready(1, caught_up, 11, 6, 6).header(), 11, now)
                .is_some()
        );
        assert!(
            matches!(
                flow.work(1, 11, now),
                Some(ReplicationWork::Replay { from_op: 7, .. })
            ),
            "a backup that holds the op at the cursor must not get it again"
        );
    }

    #[test]
    fn given_a_silent_backup_when_sends_continue_should_bound_its_bytes_and_keep_other_peers_sending()
     {
        const SIZE: u64 = MIB;
        let mut flow = ReplicationFlow::new(3);
        flow.in_epoch(0, 1);
        let mut op = 1;
        while flow.admit(1, op, SIZE, ReplicationSend::Live) {
            let _ = flow.sent(1, op, SIZE, ReplicationSend::Live, 0);
            op += 1;
        }
        assert_eq!(flow.peers[1].outstanding(), REPLICATION_BATCH_BYTES_MAX);
        assert!(
            send(
                &mut flow,
                2,
                1,
                REPLICATION_BATCH_BYTES_MAX + 1,
                ReplicationSend::Live,
                0
            )
            .is_some(),
            "a silent peer must not stop another peer, and one frame larger than the window passes"
        );
        assert!(!flow.admit(2, 2, 1, ReplicationSend::Live));
    }

    #[test]
    fn given_a_full_replay_window_when_deferred_should_drain_credit_before_history_backoff() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(2);
        assert!(consensus.admit_replication(1, 1, 1, ReplicationSend::Live));
        consensus.record_replication_failure(1, 1);
        assert!(consensus.admit_replication(
            1,
            1,
            REPLICATION_BATCH_BYTES_MAX,
            ReplicationSend::Replay
        ));
        let Some(ReplicationWork::Probe { nonce, through, .. }) = consensus
            .record_replication_send(1, 1, REPLICATION_BATCH_BYTES_MAX, ReplicationSend::Replay)
        else {
            panic!("a full window must send a checkpoint behind its last frame");
        };
        assert!(!consensus.admit_replication(1, 2, 1, ReplicationSend::Replay));
        consensus.defer_replication(1);
        assert!(
            matches!(
                consensus.replication_work(1),
                Some(ReplicationWork::Probe { nonce: resent, .. }) if resent == nonce
            ),
            "a full window must drain without the unavailable-history delay"
        );
        assert!(
            consensus
                .handle_prepare_progress(ready(1, nonce, through, 1, 1).header())
                .is_some()
        );
        assert!(
            matches!(
                consensus.replication_work(1),
                Some(ReplicationWork::Replay { from_op: 2, .. })
            ),
            "a drained window must resume replay without the stale history delay"
        );

        assert!(consensus.admit_replication(2, 1, 1, ReplicationSend::Live));
        consensus.record_replication_failure(2, 1);
        consensus.defer_replication(2);
        assert!(
            consensus.replication_work(2).is_none(),
            "unavailable history without outstanding bytes must still back off"
        );
    }

    #[test]
    fn given_progress_replies_when_the_view_incarnation_or_nonce_is_stale_should_neither_release_credit_nor_commit()
     {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(2);
        assert!(consensus.admit_replication(
            1,
            1,
            REPLICATION_BATCH_BYTES_MAX,
            ReplicationSend::Live
        ));
        let Some(ReplicationWork::Probe { nonce, through, .. }) = consensus
            .record_replication_send(1, 1, REPLICATION_BATCH_BYTES_MAX, ReplicationSend::Live)
        else {
            panic!("a full window must send a checkpoint behind its last frame");
        };
        for stale in ["view", "incarnation", "nonce", "peer", "cluster", "group"] {
            let message = ready(1, nonce, through, 1, 1).transmute_header(
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
                !consensus.admit_replication(1, 2, 1, ReplicationSend::Retransmit),
                "accepted stale {stale}"
            );
        }
        let _ = consensus.handle_prepare_progress(ready(1, nonce, through, 1, 1).header());
        assert!(consensus.admit_replication(1, 2, 1, ReplicationSend::Live));
        assert_eq!(consensus.commit_min(), 0);
        assert_eq!(consensus.commit_max(), 0);
    }

    const MODEL_BACKUP: u8 = 1;

    enum Frame {
        Prepare { op: u64, size: u64 },
        Probe { nonce: u64, through: u64 },
    }

    /// A primary that sends to one backup over a FIFO link, and replays only
    /// its newest ops. While faults are on, the backup drops frames, sends
    /// fail, replies get lost, the backup answers Busy, and repair from another
    /// replica moves the backup forward.
    struct Model {
        flow: ReplicationFlow,
        random: u64,
        faults: bool,
        now: u64,
        head: u64,
        sizes: Vec<u64>,
        link: VecDeque<Frame>,
        replies: VecDeque<Message<PrepareProgressHeader>>,
        pushed_bytes: u64,
        consumed_bytes: u64,
        held: u64,
    }

    impl Model {
        const TICK_MICROS: u64 = 10_000;
        const SIZE_MAX: u64 = 4 * MIB;
        const REPLAY_OPS_MAX: u64 = 8;
        const RETAINED_OPS: u64 = 16;
        const FAULT_ONE_IN: u64 = 10;

        fn new(seed: u64) -> Self {
            let mut flow = ReplicationFlow::new(2);
            flow.in_epoch(0, 1);
            Self {
                flow,
                random: seed,
                faults: true,
                now: 0,
                head: 0,
                sizes: Vec::new(),
                link: VecDeque::new(),
                replies: VecDeque::new(),
                pushed_bytes: 0,
                consumed_bytes: 0,
                held: 0,
            }
        }

        /// Xorshift, so the run needs no new dependency.
        fn below(&mut self, bound: u64) -> u64 {
            self.random ^= self.random << 13;
            self.random ^= self.random >> 7;
            self.random ^= self.random << 17;
            self.random % bound
        }

        fn fault(&mut self) -> bool {
            self.faults && self.below(Self::FAULT_ONE_IN) == 0
        }

        fn step(&mut self) {
            match self.below(6) {
                0 => {
                    self.head += 1;
                    let size = 1 + self.below(Self::SIZE_MAX);
                    self.sizes.push(size);
                    self.send(self.head, ReplicationSend::Live);
                }
                1 if self.head > 0 => {
                    let op = self.head - self.below(self.head.min(Self::RETAINED_OPS));
                    self.send(op, ReplicationSend::Retransmit);
                }
                2 => self.tick(),
                3 => self.deliver_frame(),
                4 if self.fault() => {
                    let behind = self.below(Self::RETAINED_OPS);
                    self.repair(behind);
                }
                _ => self.deliver_reply(),
            }
            self.check();
        }

        /// The backup holds every op up to `behind` ops before the head.
        fn repair(&mut self, behind: u64) {
            self.held = self.held.max(self.head.saturating_sub(behind));
        }

        fn tick(&mut self) {
            self.now += Self::TICK_MICROS;
            if let Some(work) = self.flow.work(MODEL_BACKUP, self.head, self.now) {
                self.run(work);
            }
        }

        fn run(&mut self, work: ReplicationWork) {
            match work {
                ReplicationWork::Probe { nonce, through, .. } => {
                    if self.fault() {
                        self.flow.probe_failed(MODEL_BACKUP, nonce);
                    } else {
                        self.link.push_back(Frame::Probe { nonce, through });
                    }
                }
                ReplicationWork::Replay { from_op, .. }
                    if from_op + Self::RETAINED_OPS <= self.head =>
                {
                    self.flow.defer(MODEL_BACKUP, self.now);
                }
                ReplicationWork::Replay { from_op, .. } => {
                    let to_op = self.head.min(from_op + Self::REPLAY_OPS_MAX - 1);
                    for op in from_op..=to_op {
                        if !self.send(op, ReplicationSend::Replay) {
                            break;
                        }
                    }
                }
            }
        }

        fn send(&mut self, op: u64, send: ReplicationSend) -> bool {
            let size = self.sizes[usize::try_from(op - 1).unwrap()];
            if !self.flow.admit(MODEL_BACKUP, op, size, send) {
                return false;
            }
            if self.fault() {
                self.flow.failed(MODEL_BACKUP, op);
                return false;
            }
            self.link.push_back(Frame::Prepare { op, size });
            self.pushed_bytes += size;
            if let Some(work) = self.flow.sent(MODEL_BACKUP, op, size, send, self.now) {
                self.run(work);
            }
            true
        }

        fn deliver_frame(&mut self) {
            let Some(frame) = self.link.pop_front() else {
                return;
            };
            let dropped = self.fault();
            match frame {
                Frame::Prepare { op, size } => {
                    self.consumed_bytes += size;
                    if !dropped && op == self.held + 1 {
                        self.held = op;
                    }
                }
                Frame::Probe { nonce, through } if !dropped => {
                    let reply = if self.fault() {
                        busy(MODEL_BACKUP, nonce, through)
                    } else {
                        ready(MODEL_BACKUP, nonce, through, self.held, self.held)
                    };
                    self.replies.push_back(reply);
                }
                Frame::Probe { .. } => {}
            }
        }

        fn deliver_reply(&mut self) {
            let Some(reply) = self.replies.pop_front() else {
                return;
            };
            if !self.fault() {
                let _ = self.flow.reply(reply.header(), self.head, self.now);
            }
        }

        fn check(&self) {
            let peer = &self.flow.peers[usize::from(MODEL_BACKUP)];
            assert_eq!(peer.sent_bytes, self.pushed_bytes);
            assert!(peer.credited_bytes <= peer.sent_bytes);
            assert!(
                peer.credited_bytes <= self.consumed_bytes,
                "credit {} passed the {} bytes the backup consumed",
                peer.credited_bytes,
                self.consumed_bytes
            );
            assert!(peer.outstanding() <= REPLICATION_BATCH_BYTES_MAX);
        }
    }

    #[test]
    fn given_random_losses_failures_and_busy_answers_when_the_flow_runs_should_keep_its_invariants_and_deliver_every_op()
     {
        const SEEDS: u64 = 16;
        const STEPS: u32 = 20_000;
        const SETTLE_TICKS: u32 = 2_000;
        for seed in 1..=SEEDS {
            let mut model = Model::new(seed);
            for _ in 0..STEPS {
                model.step();
            }
            let stats = model.flow.stats;
            assert!(
                stats.live_refusals > 0 && stats.replayed > 0,
                "seed {seed}: the faults must force refusals and replays, or the run proves nothing: \
                 {stats:?}"
            );
            model.faults = false;
            // Only repair delivers the ops that the primary no longer holds.
            model.repair(Model::RETAINED_OPS);
            for _ in 0..SETTLE_TICKS {
                if model.held == model.head {
                    break;
                }
                model.tick();
                while !model.link.is_empty() {
                    model.deliver_frame();
                }
                while !model.replies.is_empty() {
                    model.deliver_reply();
                }
                model.check();
            }
            assert_eq!(
                model.held, model.head,
                "seed {seed}: the backup must hold every op once the faults stop"
            );
        }
    }
}
