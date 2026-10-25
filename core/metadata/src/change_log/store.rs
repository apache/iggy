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

use crate::applied_frontier::AppliedFrontier;
use crate::change_log::{AppliedChange, ChangeLogSnapshot};
use crate::stm::WriteCell;
use bytes::Bytes;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMetadataChangeCursor, WireMetadataChangeGroup, WireMetadataChangeKind,
};
use iggy_common::{MetadataChangeClass, MetadataChangeGroup};
use left_right::{Absorb, ReadHandle, ReadHandleFactory};
use std::collections::VecDeque;
use std::fmt;
use std::hint;
use std::sync::Arc;

/// Records kept for streams, topics, partitions, consumer groups, users and
/// personal access tokens by default.
pub const DEFAULT_RESOURCE_CAPACITY: usize = 65_536;

/// Records kept for sessions and consumer group membership by default.
pub const DEFAULT_ACTIVITY_CAPACITY: usize = 16_384;

/// Largest ring a snapshot may hold, and the largest configurable capacity.
pub const MAX_CAPACITY: usize = 1 << 20;

/// Record body bytes one ring keeps, so large permission sets cannot grow the
/// snapshot past what state transfer carries.
pub const MAX_RING_BYTES: usize = 128 << 20;

/// Captures a coherent image tries before a read gives up as retryable.
const COHERENT_READ_ATTEMPTS: usize = 64;

/// The change log of one node: the public results of the committed metadata
/// operations it applied, newest last, in two bounded retention classes.
#[derive(Debug, Clone)]
pub struct ChangeLog {
    /// Checksum of op 1, `None` until this node applied or restored it.
    log_id: Option<u128>,
    /// Op of the newest record appended in either class.
    last_record_op: u64,
    resource: Ring,
    activity: Ring,
}

impl ChangeLog {
    #[must_use]
    pub const fn new(capacities: ChangeLogCapacities) -> Self {
        Self {
            log_id: None,
            last_record_op: 0,
            resource: Ring::new(capacities.resource),
            activity: Ring::new(capacities.activity),
        }
    }

    #[must_use]
    pub const fn log_id(&self) -> Option<u128> {
        self.log_id
    }

    #[must_use]
    pub const fn last_record_op(&self) -> u64 {
        self.last_record_op
    }

    #[must_use]
    pub const fn ring(&self, class: MetadataChangeClass) -> &Ring {
        match class {
            MetadataChangeClass::Resource => &self.resource,
            MetadataChangeClass::Activity => &self.activity,
        }
    }

    pub(crate) const fn from_parts(
        log_id: Option<u128>,
        last_record_op: u64,
        resource: Ring,
        activity: Ring,
    ) -> Self {
        Self {
            log_id,
            last_record_op,
            resource,
            activity,
        }
    }

    const fn ring_mut(&mut self, class: MetadataChangeClass) -> &mut Ring {
        match class {
            MetadataChangeClass::Resource => &mut self.resource,
            MetadataChangeClass::Activity => &mut self.activity,
        }
    }

    fn apply(&mut self, command: &ChangeLogCommand) {
        match command {
            ChangeLogCommand::Append {
                log_id,
                op,
                entries,
            } => {
                if let Some(log_id) = log_id {
                    self.log_id = Some(*log_id);
                }
                if entries.is_empty() {
                    return;
                }
                for entry in entries {
                    self.ring_mut(entry.class()).push(Arc::clone(entry));
                }
                self.last_record_op = *op;
            }
            ChangeLogCommand::Restore(log) => *self = (**log).clone(),
        }
    }
}

impl Absorb<ChangeLogCommand> for ChangeLog {
    fn absorb_first(&mut self, command: &mut ChangeLogCommand, _other: &Self) {
        self.apply(command);
    }

    fn absorb_second(&mut self, command: ChangeLogCommand, _other: &Self) {
        self.apply(&command);
    }

    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}

/// One retention class: retained records ascending by `(op, ordinal)`, and
/// the position of the newest record it evicted.
#[derive(Debug, Clone)]
pub struct Ring {
    entries: VecDeque<Arc<ChangeEntry>>,
    /// Body bytes of the retained records.
    bytes: usize,
    evicted_through: (u64, u32),
    evicted_through_timestamp: u64,
    capacity: usize,
}

impl Ring {
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            evicted_through: (0, 0),
            evicted_through_timestamp: 0,
            capacity,
        }
    }

    #[must_use]
    pub const fn entries(&self) -> &VecDeque<Arc<ChangeEntry>> {
        &self.entries
    }

    /// Body bytes of the retained records.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// `(op, ordinal)` of the newest evicted record, `(0, 0)` when none was.
    #[must_use]
    pub const fn evicted_through(&self) -> (u64, u32) {
        self.evicted_through
    }

    /// Timestamp of the newest evicted record, 0 when none was.
    #[must_use]
    pub const fn evicted_through_timestamp(&self) -> u64 {
        self.evicted_through_timestamp
    }

    /// A restored ring, trimmed from the front to `capacity` and
    /// [`MAX_RING_BYTES`].
    pub(crate) fn restored(
        entries: VecDeque<Arc<ChangeEntry>>,
        evicted_through: (u64, u32),
        evicted_through_timestamp: u64,
        capacity: usize,
    ) -> Self {
        let bytes = entries.iter().map(|entry| entry.body.len()).sum();
        let mut ring = Self {
            entries,
            bytes,
            evicted_through,
            evicted_through_timestamp,
            capacity,
        };
        ring.trim();
        ring
    }

    fn push(&mut self, entry: Arc<ChangeEntry>) {
        self.bytes += entry.body.len();
        self.entries.push_back(entry);
        self.trim();
    }

    /// Evicts record by record, so one large operation can stay partly
    /// retained.
    fn trim(&mut self) {
        while self.entries.len() > self.capacity || self.bytes > MAX_RING_BYTES {
            let Some(evicted) = self.entries.pop_front() else {
                return;
            };
            self.bytes -= evicted.body.len();
            self.evicted_through = evicted.position();
            self.evicted_through_timestamp = evicted.timestamp;
        }
    }
}

/// One retained record, stamped with the position of its operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEntry {
    pub op: u64,
    pub ordinal: u32,
    pub checksum: u128,
    pub timestamp: u64,
    pub actor_user_id: Option<u32>,
    pub kind: WireMetadataChangeKind,
    pub body: Bytes,
}

impl ChangeEntry {
    #[must_use]
    pub const fn position(&self) -> (u64, u32) {
        (self.op, self.ordinal)
    }

    #[must_use]
    pub const fn group(&self) -> WireMetadataChangeGroup {
        self.kind.group()
    }

    #[must_use]
    pub fn class(&self) -> MetadataChangeClass {
        MetadataChangeGroup::from(self.group()).class()
    }

    /// The cursor naming this record in the log `log_id`.
    #[must_use]
    pub const fn cursor(&self, log_id: u128) -> WireMetadataChangeCursor {
        WireMetadataChangeCursor {
            log_id,
            op: self.op,
            checksum: self.checksum,
            ordinal: self.ordinal,
        }
    }
}

/// Record budgets of the two retention classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeLogCapacities {
    pub resource: usize,
    pub activity: usize,
}

impl Default for ChangeLogCapacities {
    fn default() -> Self {
        Self {
            resource: DEFAULT_RESOURCE_CAPACITY,
            activity: DEFAULT_ACTIVITY_CAPACITY,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChangeLogCommand {
    /// The records of one operation, in canonical order. Op 1 also carries
    /// the log id.
    Append {
        log_id: Option<u128>,
        op: u64,
        entries: Vec<Arc<ChangeEntry>>,
    },
    /// Replace the whole log, as a restored snapshot does.
    Restore(Box<ChangeLog>),
}

/// The change log behind a left-right pair: the write side on the metadata
/// shard, read handles on every shard.
pub struct MetadataChangeLog {
    write: Option<Writer>,
    read: ReadHandle<ChangeLog>,
}

impl MetadataChangeLog {
    /// An empty log with its write side, for the metadata shard.
    #[must_use]
    pub fn new(capacities: ChangeLogCapacities) -> Self {
        let (write, read) = left_right::new_from_empty(ChangeLog::new(capacities));
        Self {
            write: Some(Writer {
                cell: WriteCell::new(write),
                capacities,
            }),
            read,
        }
    }

    /// A read-only view of the log another shard writes.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn from_factory(factory: ReadHandleFactory<ChangeLog>) -> Self {
        Self {
            write: None,
            read: factory.handle(),
        }
    }

    /// A `Send + Sync` factory other shards build their read handles from.
    #[must_use]
    pub fn factory(&self) -> ReadHandleFactory<ChangeLog> {
        self.read.factory()
    }

    /// The capacities the write side was built with.
    ///
    /// # Errors
    /// [`ChangeLogError::ReaderOnly`] on a read-only view, which does not know them.
    pub fn capacities(&self) -> Result<ChangeLogCapacities, ChangeLogError> {
        self.write
            .as_ref()
            .map(|write| write.capacities)
            .ok_or(ChangeLogError::ReaderOnly)
    }

    /// Runs `read` on the published log, `None` once the write side is gone.
    pub fn read<R>(&self, read: impl FnOnce(&ChangeLog) -> R) -> Option<R> {
        self.read.enter().map(|log| read(&log))
    }

    /// Runs `read` on an image of the log holding every record of the ops up
    /// to the frontier it is given and none above. `None` once the write side
    /// is gone or publications kept racing the capture.
    ///
    /// A frontier read is a lower bound: the frontier is read before and
    /// after entering the log, and a log ahead of it is retried, since
    /// appends publish before the frontier advances.
    pub fn read_coherent<R>(
        &self,
        frontier: &AppliedFrontier,
        read: impl Fn(&ChangeLog, u64) -> R,
    ) -> Option<R> {
        for _ in 0..COHERENT_READ_ATTEMPTS {
            let before = frontier.get();
            let log = self.read.enter()?;
            let head_op = frontier.get();
            if before == head_op && log.last_record_op() <= head_op {
                return Some(read(&log, head_op));
            }
            drop(log);
            hint::spin_loop();
        }
        None
    }

    /// The persisted form of the published log. Empty only on a read-only
    /// view whose write side is gone, and only the write side checkpoints.
    #[must_use]
    pub fn snapshot(&self) -> ChangeLogSnapshot {
        self.read(|log| ChangeLogSnapshot::from(log))
            .unwrap_or_default()
    }

    /// Appends the records of one committed operation and publishes them.
    ///
    /// # Errors
    /// [`ChangeLogError::ReaderOnly`] on a read-only view, and
    /// [`ChangeLogError::NotMonotone`] for records of an operation at or below
    /// the newest appended one, which only a bug can produce.
    pub fn append(
        &self,
        header: &PrepareHeader,
        changes: Vec<AppliedChange>,
    ) -> Result<(), ChangeLogError> {
        let write = self.write.as_ref().ok_or(ChangeLogError::ReaderOnly)?;
        let log_id = (header.op == 1).then_some(header.checksum);
        if changes.is_empty() && log_id.is_none() {
            return Ok(());
        }
        if !changes.is_empty() {
            let last_record_op = self.read(ChangeLog::last_record_op).unwrap_or_default();
            if header.op <= last_record_op {
                return Err(ChangeLogError::NotMonotone {
                    op: header.op,
                    last_record_op,
                });
            }
        }
        let entries = changes
            .into_iter()
            .enumerate()
            .map(|(ordinal, change)| {
                Ok(Arc::new(ChangeEntry {
                    op: header.op,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| ChangeLogError::TooManyRecords { op: header.op })?,
                    checksum: header.checksum,
                    timestamp: header.timestamp,
                    actor_user_id: change.actor_user_id,
                    kind: change.kind,
                    body: change.body,
                }))
            })
            .collect::<Result<Vec<_>, ChangeLogError>>()?;
        write.cell.apply(ChangeLogCommand::Append {
            log_id,
            op: header.op,
            entries,
        });
        Ok(())
    }

    /// Replaces the log with `log` and publishes it.
    ///
    /// # Errors
    /// [`ChangeLogError::ReaderOnly`] on a read-only view.
    pub fn restore(&self, log: ChangeLog) -> Result<(), ChangeLogError> {
        let write = self.write.as_ref().ok_or(ChangeLogError::ReaderOnly)?;
        write.cell.apply(ChangeLogCommand::Restore(Box::new(log)));
        Ok(())
    }
}

impl fmt::Debug for MetadataChangeLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataChangeLog")
            .field("writer", &self.write.is_some())
            .field("capacities", &self.capacities().ok())
            .finish_non_exhaustive()
    }
}

/// The write side of the log and the capacities it was built with.
struct Writer {
    cell: WriteCell<ChangeLog, ChangeLogCommand>,
    capacities: ChangeLogCapacities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeLogError {
    /// A write reached a read-only view of the log.
    ReaderOnly,
    /// Records of an operation at or below the newest appended one.
    NotMonotone { op: u64, last_record_op: u64 },
    /// One operation produced more records than an ordinal can number.
    TooManyRecords { op: u64 },
}

impl fmt::Display for ChangeLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReaderOnly => f.write_str("change log write on a read-only view"),
            Self::NotMonotone { op, last_record_op } => write!(
                f,
                "change log append at op {op} is not above the newest record op {last_record_op}"
            ),
            Self::TooManyRecords { op } => {
                write!(
                    f,
                    "op {op} produced more change records than fit an ordinal"
                )
            }
        }
    }
}

impl std::error::Error for ChangeLogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeKind as Kind;

    #[test]
    fn given_records_of_one_op_when_appending_should_stamp_header_and_consecutive_ordinals() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());

        log.append(
            &header(7, 0xabc, 7_000),
            vec![
                change(Kind::UserDeleted, Some(3)),
                change(Kind::ConsumerGroupMemberLeft, Some(3)),
            ],
        )
        .expect("append");

        let (resource, activity) = log
            .read(|log| {
                (
                    entries(log.ring(MetadataChangeClass::Resource)),
                    entries(log.ring(MetadataChangeClass::Activity)),
                )
            })
            .expect("readable");
        assert_eq!(
            resource,
            [ChangeEntry {
                op: 7,
                ordinal: 0,
                checksum: 0xabc,
                timestamp: 7_000,
                actor_user_id: Some(3),
                kind: Kind::UserDeleted,
                body: Bytes::from_static(b"body"),
            }]
        );
        assert_eq!(activity[0].position(), (7, 1));
        assert_eq!(log.read(ChangeLog::last_record_op), Some(7));
    }

    #[test]
    fn given_op_one_when_appending_should_set_log_id_even_without_records() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        assert_eq!(log.read(ChangeLog::log_id), Some(None));

        log.append(&header(1, 0, 1), Vec::new()).expect("append");
        log.append(&header(2, 0x77, 2), Vec::new()).expect("append");

        assert_eq!(log.read(ChangeLog::log_id), Some(Some(0)));
        assert_eq!(log.read(ChangeLog::last_record_op), Some(0));
    }

    #[test]
    fn given_full_ring_when_appending_should_evict_by_record_and_keep_a_partial_operation() {
        let log = MetadataChangeLog::new(ChangeLogCapacities {
            resource: 3,
            activity: 2,
        });
        log.append(
            &header(4, 4, 400),
            vec![change(Kind::StreamCreated, None); 2],
        )
        .expect("append");

        log.append(
            &header(5, 5, 500),
            vec![change(Kind::StreamCreated, None); 2],
        )
        .expect("append");

        let ring = log
            .read(|log| log.ring(MetadataChangeClass::Resource).clone())
            .expect("readable");
        assert_eq!(
            ring.entries()
                .iter()
                .map(|entry| entry.position())
                .collect::<Vec<_>>(),
            [(4, 1), (5, 0), (5, 1)]
        );
        assert_eq!(ring.evicted_through(), (4, 0));
        assert_eq!(ring.evicted_through_timestamp(), 400);
        let activity = log
            .read(|log| log.ring(MetadataChangeClass::Activity).evicted_through())
            .expect("readable");
        assert_eq!(activity, (0, 0), "the other class evicts on its own budget");
    }

    #[test]
    fn given_op_at_or_below_newest_record_when_appending_should_refuse_it() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        log.append(&header(9, 9, 9), vec![change(Kind::StreamCreated, None)])
            .expect("append");

        for op in [9, 8] {
            assert_eq!(
                log.append(
                    &header(op, op.into(), op),
                    vec![change(Kind::StreamCreated, None)]
                ),
                Err(ChangeLogError::NotMonotone {
                    op,
                    last_record_op: 9
                })
            );
        }
        assert_eq!(
            log.read(|log| log.ring(MetadataChangeClass::Resource).entries().len()),
            Some(1)
        );
    }

    #[test]
    fn given_bodies_past_the_byte_budget_when_appending_should_evict_the_oldest_records() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        for op in 1..=3 {
            log.append(
                &header(op, u128::from(op), op),
                vec![sized_change(MAX_RING_BYTES / 2 + 1)],
            )
            .expect("append");
        }

        let ring = log
            .read(|log| log.ring(MetadataChangeClass::Resource).clone())
            .expect("readable");

        assert_eq!(ring.entries().len(), 1);
        assert_eq!(ring.evicted_through(), (2, 0));
        assert_eq!(ring.bytes(), MAX_RING_BYTES / 2 + 1);
    }

    #[test]
    fn given_snapshot_past_the_byte_budget_when_restoring_should_trim_the_oldest_records() {
        let entries = (1..=3)
            .map(|op| {
                Arc::new(ChangeEntry {
                    op,
                    ordinal: 0,
                    checksum: u128::from(op),
                    timestamp: op,
                    actor_user_id: None,
                    kind: Kind::UserPermissionsUpdated,
                    body: Bytes::from(vec![0; MAX_RING_BYTES / 2 + 1]),
                })
            })
            .collect();

        let ring = Ring::restored(entries, (0, 0), 0, DEFAULT_RESOURCE_CAPACITY);

        assert_eq!(ring.entries().len(), 1);
        assert_eq!(ring.evicted_through(), (2, 0));
    }

    #[test]
    fn given_reader_view_when_asking_capacities_should_refuse_instead_of_guessing() {
        let capacities = ChangeLogCapacities {
            resource: 7,
            activity: 3,
        };
        let writer = MetadataChangeLog::new(capacities);
        let reader = MetadataChangeLog::from_factory(writer.factory());

        assert_eq!(writer.capacities(), Ok(capacities));
        assert_eq!(reader.capacities(), Err(ChangeLogError::ReaderOnly));
    }

    #[test]
    fn given_reader_view_when_writing_should_refuse_and_see_the_writer_state() {
        let writer = MetadataChangeLog::new(ChangeLogCapacities::default());
        let reader = MetadataChangeLog::from_factory(writer.factory());

        writer
            .append(&header(1, 0x11, 1), vec![change(Kind::UserCreated, None)])
            .expect("append");

        assert_eq!(
            reader.append(&header(2, 2, 2), vec![change(Kind::UserCreated, None)]),
            Err(ChangeLogError::ReaderOnly)
        );
        assert_eq!(
            reader.restore(ChangeLog::new(ChangeLogCapacities::default())),
            Err(ChangeLogError::ReaderOnly)
        );
        assert_eq!(reader.read(ChangeLog::log_id), Some(Some(0x11)));
        assert_eq!(reader.read(ChangeLog::last_record_op), Some(1));
    }

    fn header(op: u64, checksum: u128, timestamp: u64) -> PrepareHeader {
        PrepareHeader {
            op,
            checksum,
            timestamp,
            ..PrepareHeader::default()
        }
    }

    fn change(kind: Kind, actor_user_id: Option<u32>) -> AppliedChange {
        AppliedChange {
            kind,
            actor_user_id,
            body: Bytes::from_static(b"body"),
        }
    }

    fn sized_change(body_bytes: usize) -> AppliedChange {
        AppliedChange {
            kind: Kind::UserPermissionsUpdated,
            actor_user_id: None,
            body: Bytes::from(vec![0; body_bytes]),
        }
    }

    fn entries(ring: &Ring) -> Vec<ChangeEntry> {
        ring.entries()
            .iter()
            .map(|entry| (**entry).clone())
            .collect()
    }
}
