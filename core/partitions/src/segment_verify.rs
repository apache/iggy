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
    /// The job in flight: its number, and the scope whose end stops it.
    running: Option<(u64, TierScope)>,
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

    /// Drop the finding of each segment that `stored` no longer returns as it
    /// was checked, as after retention trimmed it. Returns how many went.
    pub fn retain_stored(&mut self, stored: impl Fn(u64) -> Option<SegmentRecord>) -> usize {
        let before = self.findings.len();
        self.findings
            .retain(|start_offset, (record, _)| stored(*start_offset) == Some(*record));
        self.recount();
        before - self.findings.len()
    }

    /// Start the next job of the pass under way, or a new pass when one is
    /// due at `now`. `stored_from` gives the stored segments from a start
    /// offset on, in offset order. The job takes the first
    /// [`SEGMENT_VERIFY_BATCH_MAX`] of them and runs as number `id`.
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
        let segments: Vec<SegmentRecord> = stored_from(cursor)
            .into_iter()
            .skip_while(|segment| segment.start_offset < cursor)
            .take(SEGMENT_VERIFY_BATCH_MAX)
            .collect();
        if segments.is_empty() {
            self.cursor = None;
            return VerifyBegin::PassEnded;
        }
        self.cursor = Some(cursor);
        let scope = TierScope::new();
        let token = scope.token();
        self.running = Some((id, scope));
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
    /// A HEAD without an answer is no finding. After a transient error the
    /// next job starts with its segment, and after any other the segment
    /// waits for the next pass.
    pub fn finish(
        &mut self,
        id: u64,
        outcome: &SegmentVerifyOutcome,
        stored: impl Fn(u64) -> Option<SegmentRecord>,
    ) -> Option<VerifySettled> {
        if self
            .running
            .as_ref()
            .is_none_or(|(running, _)| *running != id)
        {
            return None;
        }
        self.running = None;
        let mut settled = VerifySettled::default();
        let Ok(report) = outcome else {
            return Some(settled);
        };
        for heads in &report.answered {
            let record = heads.record;
            let start_offset = record.start_offset;
            self.cursor = Some(start_offset.saturating_add(1));
            if stored(start_offset) != Some(record) {
                continue;
            }
            settled.checked += 1;
            let previous = self.findings.get(&start_offset).map(|(_, found)| *found);
            match (previous, finding(heads)) {
                (Some(cleared), None) => {
                    self.findings.remove(&start_offset);
                    settled.transitions.push(Transition::Cleared {
                        record,
                        finding: cleared,
                    });
                }
                (previous, Some(found)) if previous != Some(found) => {
                    self.findings.insert(start_offset, (record, found));
                    settled.transitions.push(Transition::Found {
                        record,
                        finding: found,
                    });
                }
                _ => {}
            }
        }
        if let Some(unanswered) = &report.unanswered {
            let retry = matches!(unanswered.error.class(), ErrorClass::Transient);
            self.cursor = Some(if retry {
                unanswered.start_offset
            } else {
                unanswered.start_offset.saturating_add(1)
            });
        }
        self.retain_stored(stored);
        Some(settled)
    }

    fn pass_due(&self, now: IggyTimestamp) -> bool {
        let interval = u64::try_from(SEGMENT_VERIFY_INTERVAL.as_micros()).unwrap_or(u64::MAX);
        self.pass_started_at.is_none_or(|started_at| {
            now.as_micros() >= started_at.as_micros().saturating_add(interval)
        })
    }

    fn recount(&mut self) {
        self.missing = self
            .findings
            .values()
            .filter(|(_, found)| matches!(found, Finding::Missing(_)))
            .count();
        self.diverged = self.findings.len() - self.missing;
    }
}
