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

//! When the shard tick can skip a partition.
//!
//! The tick visits only the partitions that are awake. A visit of a quiet
//! partition does nothing, so the tick lets it sleep until its next timeout.
//! The contract has two halves:
//!
//! - [`IggyPartition::quiet_until`] returns a tick only while every check
//!   that the shard tick runs for a partition would find nothing to do,
//!   on every tick before the returned one.
//! - Whatever ends that state reaches the partition through
//!   [`crate::IggyPartitions`], whose accessors wake it. Work that a task
//!   outside the pump finishes (the WAL writer, an I/O job, a transfer)
//!   starts through such an accessor too, and the partition is not quiet
//!   while that work is in flight.
//!
//! Every state that is not plainly idle counts as not quiet: a repair, a
//! transfer, a view change, a held vote, pending I/O and every failure. The
//! tick visits those partitions on every tick, so their retries and their
//! counts of visits behave as they did when the tick visited everything.

use super::IggyPartition;
use crate::GaugeShare;
use consensus::{Consensus, Sequencer, StateTransferStage};
use iggy_common::ConsumerKind;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The tick of the shard clock before which a shard tick has nothing to
    /// do here, `u64::MAX` when no timeout runs. `None` when the next tick
    /// can have work.
    ///
    /// The shard adds the checks it owns: the tombstone and the placement
    /// report.
    #[must_use]
    pub fn quiet_until(&self) -> Option<u64> {
        (self.consensus_is_settled()
            && self.recovery_is_idle()
            && self.storage_is_idle()
            && self.gauges_are_level())
        .then(|| self.consensus.next_timer_deadline())
        .flatten()
    }

    /// The levels this partition adds to the shard gauges that count
    /// sleeping partitions too.
    pub(crate) fn gauge_share(&self) -> GaugeShare {
        let (wal_disk_bytes, wal_retained_bytes) = self
            .persistence
            .as_ref()
            .map_or((0, 0), |persistence| persistence.disk_and_retained_bytes());
        let (repair_ring_entries, repair_ring_bytes) = self.repair_ring_occupancy();
        GaugeShare {
            wal_disk_bytes,
            wal_retained_bytes,
            repair_ring_entries,
            repair_ring_bytes,
        }
    }

    /// Normal status with nothing to commit, replicate or persist of the
    /// view, and no request that waits for a tick.
    fn consensus_is_settled(&self) -> bool {
        let consensus = &self.consensus;
        let commit = consensus.commit_max();
        self.fatal.is_none()
            && consensus.is_normal()
            && consensus.state_transfer_stage() == StateTransferStage::Idle
            && !consensus.has_untrusted_log()
            && !consensus.view_log_unproven()
            && !consensus.is_primary_elect()
            && !consensus.needs_superblock_persist()
            && consensus.commit_min() == commit
            && consensus.sequencer().current_sequence() == commit
            && consensus.replication_is_idle()
            && consensus.with_pipeline(|pipeline| pipeline.request_head().is_none())
    }

    /// No repair, transfer, fence or rebuild, which the tick drives by
    /// counting its visits, and no memory of a proof walk, which the next
    /// visit hands back to the node.
    fn recovery_is_idle(&self) -> bool {
        !self.materialization_missing
            && self.repair.is_none()
            && self.transfer.is_none()
            && self.transfer_rearm.is_none()
            && !self.view_fenced()
            && !self.holds_transfer_tail_lease()
            && self.gap_ticks.get() == 0
            && self
                .proof_walk_memory
                .as_ref()
                .is_none_or(|memory| memory.bytes() == 0)
    }

    /// No work for the I/O lane, the WAL writer or the superblock, and none
    /// that the tick starts for them.
    fn storage_is_idle(&self) -> bool {
        self.io_active.is_idle()
            && !self.has_lane_work()
            && self.maintenance.is_idle()
            && self.superblock_running.get().is_none()
            && self.superblock_write_failures.get() == 0
            && self.persistence.as_ref().is_none_or(|persistence| {
                persistence.is_quiescent()
                    && !persistence.checkpoint_pending()
                    && persistence.failure().is_none()
                    && persistence.certified_log_view() == Some(self.consensus.log_view())
            })
            && !self.needs_persistence_checkpoint()
            && !self.needs_offset_reservation_extension()
            && !self.any_consumer_offset_capacity_uncertain()
    }

    /// The shard counts these states over the partitions it visits, so a
    /// partition in one of them stays awake to be counted.
    fn gauges_are_level(&self) -> bool {
        !self.owes_session_retirement()
            && self.prepare_gap_drops.get() == 0
            && ConsumerKind::ALL
                .into_iter()
                .all(|kind| self.stranded_consumer_offset_count(kind) == 0)
    }
}
