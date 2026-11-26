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

//! Whether the object store still holds the segments a tiered partition
//! stored, as its chain records them.
//!
//! The settled primary checks every segment its chain records as stored, in
//! passes [`SEGMENT_VERIFY_INTERVAL`] apart, and in jobs of at most
//! [`SEGMENT_VERIFY_BATCH_MAX`] segments. A check is one HEAD of the
//! segment's `.log` and one of its `.index`. Only an object the store says it
//! lacks, or one whose HEAD disagrees with the record, is a finding. A
//! finding is reported, never repaired.
//!
//! Nothing here does any I/O.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use iggy_common::IggyTimestamp;
use iggy_object_store::{
    ErrorClass, ObjectMeta, ObjectPrefix, ObjectStoreError, SegmentMetadata, SegmentObject,
    TierJobCancelled, TierScope, TierScopeToken,
};

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::segment_chain::SegmentRecord;

#[cfg(test)]
mod tests;

/// The time from the start of one verify pass over a partition to the start
/// of the next.
pub const SEGMENT_VERIFY_INTERVAL: Duration = Duration::from_hours(24);

/// The most segments one job of a verify pass checks.
pub const SEGMENT_VERIFY_BATCH_MAX: usize = 64;

/// The most jobs in a row that ask for a segment the store answers with a
/// transient error. The pass then goes on without it, and asks for it once
/// more after the last stored segment.
pub const SEGMENT_VERIFY_ATTEMPTS: u32 = 3;

/// The segments one job checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentVerifyJob {
    /// The prefix of the partition's incarnation, which names the objects.
    pub prefix: ObjectPrefix,
    /// In offset order.
    pub segments: Vec<SegmentRecord>,
}

/// What the store answered to the HEAD of one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectHead {
    Found(ObjectMeta),
    NotFound,
}

/// The answers to the HEADs of both objects of one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeads {
    /// The record the answers are compared with.
    pub record: SegmentRecord,
    pub log: ObjectHead,
    pub index: ObjectHead,
}

/// A HEAD whose answer says neither that the object is there nor that it is
/// not.
#[derive(Debug)]
pub struct Unanswered {
    pub start_offset: u64,
    pub error: ObjectStoreError,
}

/// What one job found. A job stops at its first HEAD without an answer.
#[derive(Debug, Default)]
pub struct SegmentVerifyReport {
    /// The segments the store answered both HEADs of, in offset order.
    pub answered: Vec<SegmentHeads>,
    pub unanswered: Option<Unanswered>,
}

/// The result of a job: its report, or that its scope was cancelled first.
pub type SegmentVerifyOutcome = Result<SegmentVerifyReport, TierJobCancelled>;

/// How an object in the store differs from what the partition recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Divergence {
    Size {
        stored: u64,
        recorded: u64,
    },
    /// The CRC32C the store computed of the object.
    Crc32c {
        stored: u32,
        recorded: u32,
    },
    /// The store keeps no CRC32C, and the metadata of the object holds
    /// another hash than the record, or none that decodes.
    Hash,
    /// The `.index` is not a whole number of entries, or it is empty.
    IndexEntries {
        stored: u64,
    },
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Size { stored, recorded } => write!(
                f,
                "the store holds {stored} bytes, and the chain records {recorded}"
            ),
            Self::Crc32c { stored, recorded } => write!(
                f,
                "the store's CRC32C is {stored:#010x}, and the chain records {recorded:#010x}"
            ),
            Self::Hash => f.write_str(
                "the store keeps no CRC32C, and the object's metadata does not hold the hash \
                 the chain records",
            ),
            Self::IndexEntries { stored } => write!(
                f,
                "the store holds {stored} bytes, which are not a whole number of index entries"
            ),
        }
    }
}

/// What a check found wrong with a stored segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finding {
    /// The store holds no object under the key.
    Missing(SegmentObject),
    Diverged(SegmentObject, Divergence),
}

impl Finding {
    /// The object the finding is about.
    #[must_use]
    pub const fn object(&self) -> SegmentObject {
        match self {
            Self::Missing(object) | Self::Diverged(object, _) => *object,
        }
    }
}

/// How the object under a segment's `.log` key differs from `record`, `None`
/// when it is the object the record describes: of the recorded size, with the
/// recorded CRC32C, or with the recorded hash in its metadata when the store
/// keeps no CRC32C.
#[must_use]
pub fn log_divergence(held: &ObjectMeta, record: &SegmentRecord) -> Option<Divergence> {
    if held.size != record.size {
        return Some(Divergence::Size {
            stored: held.size,
            recorded: record.size,
        });
    }
    match held.crc32c {
        Some(stored) if stored != record.crc32c => Some(Divergence::Crc32c {
            stored,
            recorded: record.crc32c,
        }),
        Some(_) => None,
        None => {
            let same_hash = SegmentMetadata::try_from(&held.metadata)
                .is_ok_and(|metadata| metadata.hash == record.hash);
            (!same_hash).then_some(Divergence::Hash)
        }
    }
}

/// How the object under a segment's `.index` key differs from an index,
/// `None` when it holds a whole number of entries, at least one. Sparse
/// indexes differ between replicas, so no record says more of it.
#[must_use]
pub fn index_divergence(held: &ObjectMeta) -> Option<Divergence> {
    (held.size == 0 || !held.size.is_multiple_of(IGGY_INDEX_SIZE as u64))
        .then_some(Divergence::IndexEntries { stored: held.size })
}

/// What the answers show of their segment, `None` when the store holds it as
/// recorded. A missing object goes before a diverged one, and the `.log`
/// before the `.index`.
#[must_use]
pub fn finding(heads: &SegmentHeads) -> Option<Finding> {
    let (ObjectHead::Found(log), ObjectHead::Found(index)) = (&heads.log, &heads.index) else {
        let object = if heads.log == ObjectHead::NotFound {
            SegmentObject::Log
        } else {
            SegmentObject::Index
        };
        return Some(Finding::Missing(object));
    };
    log_divergence(log, &heads.record)
        .map(|divergence| Finding::Diverged(SegmentObject::Log, divergence))
        .or_else(|| {
            index_divergence(index)
                .map(|divergence| Finding::Diverged(SegmentObject::Index, divergence))
        })
}

/// A change of what is known of one stored segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// The segment has `finding`, and had none or another one.
    Found {
        record: SegmentRecord,
        finding: Finding,
    },
    /// The segment had `finding`, and the store holds it as recorded again.
    Cleared {
        record: SegmentRecord,
        finding: Finding,
    },
}

/// What the start of the next job of a verify pass decided.
#[derive(Debug)]
pub enum VerifyBegin {
    /// A job runs, or no pass is under way and the next one is not due.
    Idle,
    /// The pass under way had no segment left to check, and ended.
    PassEnded,
    /// Run `job` under `scope`, and hand its result back with `id`.
    Job {
        id: u64,
        job: SegmentVerifyJob,
        scope: TierScopeToken,
    },
}

/// What one job's result changed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct VerifySettled {
    /// The segments whose answers counted.
    pub checked: usize,
    pub transitions: Vec<Transition>,
    /// Whether the segment the store did not answer for waits for the next
    /// pass.
    pub postponed: bool,
}

/// A job in flight.
#[derive(Debug)]
struct RunningJob {
    id: u64,
    /// The scope whose end stops it.
    _scope: TierScope,
    /// Whether the job asks for the segments the pass set aside.
    set_aside: bool,
}

/// What the primary of a tiered partition knows of the store's copy of its
/// stored segments, in one view. Nothing here is persisted.
#[derive(Debug)]
pub struct SegmentVerifies {
    view: u32,
    /// The lowest start offset the pass under way has yet to check, `None`
    /// between passes.
    cursor: Option<u64>,
    /// When the pass under way, or the last one, started.
    pass_started_at: Option<IggyTimestamp>,
    running: Option<RunningJob>,
    /// The segment the next job asks for again after a transient error, and
    /// how many jobs in a row got no answer for it.
    retrying: Option<(u64, u32)>,
    /// The segments the pass under way went on without, by start offset,
    /// which it asks for once more after the last stored segment.
    set_aside: BTreeMap<u64, SegmentRecord>,
    /// What is wrong with each segment, by its start offset, and the record
    /// it was checked against.
    findings: BTreeMap<u64, (SegmentRecord, Finding)>,
    missing: usize,
    diverged: usize,
}

impl SegmentVerifies {
    /// The state of a replica that became the primary in `view`: its first
    /// pass is due at once.
    #[must_use]
    pub const fn new(view: u32) -> Self {
        Self {
            view,
            cursor: None,
            pass_started_at: None,
            running: None,
            retrying: None,
            set_aside: BTreeMap::new(),
            findings: BTreeMap::new(),
            missing: 0,
            diverged: 0,
        }
    }

    #[must_use]
    pub const fn view(&self) -> u32 {
        self.view
    }

    /// The segments with an object the store does not hold.
    #[must_use]
    pub const fn missing(&self) -> usize {
        self.missing
    }

    /// The segments with an object that differs from the record.
    #[must_use]
    pub const fn diverged(&self) -> usize {
        self.diverged
    }

    /// Forget the segments that end at or below `trim_point`, the chain's
    /// trim point, which retention removed. Returns how many findings went.
    pub fn trim(&mut self, trim_point: u64) -> usize {
        let trimmed = |record: &SegmentRecord| record.end_offset <= trim_point;
        while self
            .set_aside
            .first_key_value()
            .is_some_and(|(_, record)| trimmed(record))
        {
            self.set_aside.pop_first();
        }
        let mut dropped = 0;
        while let Some(entry) = self.findings.first_entry()
            && trimmed(&entry.get().0)
        {
            let (_, finding) = entry.remove();
            *self.count_of(finding) -= 1;
            dropped += 1;
        }
        dropped
    }

    /// Start the next job of the pass under way, or a new pass when one is
    /// due at `now`. `stored_from` gives the stored segments from a start
    /// offset on, in offset order. The job takes the first
    /// [`SEGMENT_VERIFY_BATCH_MAX`] of them and runs as number `id`. Once
    /// none is left, it takes the segments the pass set aside.
    pub fn begin<I>(
        &mut self,
        now: IggyTimestamp,
        id: u64,
        prefix: ObjectPrefix,
        stored_from: impl FnOnce(u64) -> I,
    ) -> VerifyBegin
    where
        I: IntoIterator<Item = SegmentRecord>,
    {
        if self.running.is_some() {
            return VerifyBegin::Idle;
        }
        let cursor = match self.cursor {
            Some(cursor) => cursor,
            None if self.pass_due(now) => {
                self.pass_started_at = Some(now);
                0
            }
            None => return VerifyBegin::Idle,
        };
        let mut segments: Vec<SegmentRecord> = stored_from(cursor)
            .into_iter()
            .skip_while(|segment| segment.start_offset < cursor)
            .take(SEGMENT_VERIFY_BATCH_MAX)
            .collect();
        let set_aside = segments.is_empty();
        if set_aside {
            segments.extend(
                self.set_aside
                    .values()
                    .take(SEGMENT_VERIFY_BATCH_MAX)
                    .copied(),
            );
        }
        if segments.is_empty() {
            self.cursor = None;
            return VerifyBegin::PassEnded;
        }
        self.cursor = Some(cursor);
        let scope = TierScope::new();
        let token = scope.token();
        self.running = Some(RunningJob {
            id,
            _scope: scope,
            set_aside,
        });
        VerifyBegin::Job {
            id,
            job: SegmentVerifyJob { prefix, segments },
            scope: token,
        }
    }

    /// Take the result of the job numbered `id`, `None` for a job this state
    /// does not wait for. `stored` gives the record the chain holds now of a
    /// stored segment, and an answer for a segment it does not return as the
    /// job checked it counts for nothing.
    ///
    /// A HEAD without an answer is no finding, and its segment does not count
    /// as checked. After a transient error the next job starts with its
    /// segment, up to [`SEGMENT_VERIFY_ATTEMPTS`] jobs in a row, and then the
    /// pass sets it aside. After any other error, or in a job over the
    /// segments set aside, the segment waits for the next pass.
    pub fn finish(
        &mut self,
        id: u64,
        outcome: &SegmentVerifyOutcome,
        stored: impl Fn(u64) -> Option<SegmentRecord>,
    ) -> Option<VerifySettled> {
        if self.running.as_ref().is_none_or(|running| running.id != id) {
            return None;
        }
        let set_aside = self.running.take().is_some_and(|running| running.set_aside);
        let mut settled = VerifySettled::default();
        let Ok(report) = outcome else {
            return Some(settled);
        };
        for heads in &report.answered {
            let record = heads.record;
            let start_offset = record.start_offset;
            self.set_aside.remove(&start_offset);
            if !set_aside {
                self.cursor = Some(start_offset.saturating_add(1));
            }
            if stored(start_offset) != Some(record) {
                continue;
            }
            settled.checked += 1;
            let previous = self.findings.get(&start_offset).map(|(_, found)| *found);
            match (previous, finding(heads)) {
                (Some(cleared), None) => {
                    self.findings.remove(&start_offset);
                    *self.count_of(cleared) -= 1;
                    settled.transitions.push(Transition::Cleared {
                        record,
                        finding: cleared,
                    });
                }
                (previous, Some(found)) if previous != Some(found) => {
                    self.findings.insert(start_offset, (record, found));
                    if let Some(previous) = previous {
                        *self.count_of(previous) -= 1;
                    }
                    *self.count_of(found) += 1;
                    settled.transitions.push(Transition::Found {
                        record,
                        finding: found,
                    });
                }
                _ => {}
            }
        }
        let retrying = self.retrying.take();
        if let Some(unanswered) = &report.unanswered {
            let failed_before = retrying
                .filter(|(start_offset, _)| *start_offset == unanswered.start_offset)
                .map_or(0, |(_, failed)| failed);
            settled.postponed = self.put_off(unanswered, set_aside, failed_before, stored);
        }
        Some(settled)
    }

    fn pass_due(&self, now: IggyTimestamp) -> bool {
        let interval = u64::try_from(SEGMENT_VERIFY_INTERVAL.as_micros()).unwrap_or(u64::MAX);
        self.pass_started_at.is_none_or(|started_at| {
            now.as_micros() >= started_at.as_micros().saturating_add(interval)
        })
    }

    /// Decide when the pass asks again for the segment the store did not
    /// answer for, after `failed_before` jobs in a row got no answer for it.
    /// Returns whether it waits for the next pass.
    fn put_off(
        &mut self,
        unanswered: &Unanswered,
        set_aside: bool,
        failed_before: u32,
        stored: impl Fn(u64) -> Option<SegmentRecord>,
    ) -> bool {
        let start_offset = unanswered.start_offset;
        if set_aside {
            self.set_aside.remove(&start_offset);
            return true;
        }
        self.cursor = Some(start_offset.saturating_add(1));
        if unanswered.error.class() != ErrorClass::Transient {
            return true;
        }
        let failed = failed_before + 1;
        if failed < SEGMENT_VERIFY_ATTEMPTS {
            self.retrying = Some((start_offset, failed));
            self.cursor = Some(start_offset);
        } else if let Some(record) = stored(start_offset) {
            self.set_aside.insert(start_offset, record);
        }
        false
    }

    fn count_of(&mut self, finding: Finding) -> &mut usize {
        match finding {
            Finding::Missing(_) => &mut self.missing,
            Finding::Diverged(..) => &mut self.diverged,
        }
    }
}
