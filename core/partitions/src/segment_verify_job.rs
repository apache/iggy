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

//! The HEADs of a verify job, see [`crate::segment_verify`], and the
//! partition's side of a pass: which replica runs it, and what it logs.

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ObjectPrefix, ObjectStore, ObjectStoreError, SegmentKey, SegmentObject,
    TierExecutor, TierJobClass, TierJobRequest, TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, error, info, warn};

use crate::IggyPartition;
use crate::segment_verify::{
    Finding, ObjectHead, SegmentHeads, SegmentVerifies, SegmentVerifyJob, SegmentVerifyOutcome,
    SegmentVerifyReport, Transition, Unanswered, VerifyBegin, VerifyTally, log_divergence,
};

/// Runs the jobs of a shard's verify passes, behind a pointer, so the shard
/// does not carry the executor's type.
pub trait SegmentVerifier {
    /// HEAD the objects of `job` under `scope`. The job stops when the scope
    /// is cancelled, as when its partition loses primacy.
    fn verify(
        &self,
        job: SegmentVerifyJob,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, SegmentVerifyOutcome>;
}

/// Verifies through a tier executor, in its housekeeping class, which yields
/// to fetches and uploads. The job never goes through the read cache.
pub struct ExecutorSegmentVerifier<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorSegmentVerifier<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the jobs go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> SegmentVerifier for ExecutorSegmentVerifier<E> {
    fn verify(
        &self,
        job: SegmentVerifyJob,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, SegmentVerifyOutcome> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Housekeeping, scope);
        self.executor
            .submit(request, move |store| async move {
                head_segments(&*store, &layout, &job).await
            })
            .boxed_local()
    }
}

/// HEAD the `.log` and then the `.index` of each segment of `job`, in order,
/// up to the first HEAD that the store answers with an error other than
/// [`ObjectStoreError::NotFound`]. The `.index` of a segment whose `.log` is
/// missing is not asked for. A `.log` that differs from the record is
/// reported even when the HEAD of its `.index` fails.
pub async fn head_segments<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    job: &SegmentVerifyJob,
) -> SegmentVerifyReport {
    let mut report = SegmentVerifyReport {
        answered: Vec::with_capacity(job.segments.len()),
        unanswered: None,
    };
    for record in &job.segments {
        let start_offset = record.start_offset;
        let unanswered = |error| {
            Some(Unanswered {
                start_offset,
                error,
            })
        };
        let log = match head(store, layout, job.prefix, start_offset, SegmentObject::Log).await {
            Ok(log) => log,
            Err(error) => {
                report.unanswered = unanswered(error);
                break;
            }
        };
        let index = match &log {
            ObjectHead::NotFound => None,
            ObjectHead::Found(held) => {
                match head(
                    store,
                    layout,
                    job.prefix,
                    start_offset,
                    SegmentObject::Index,
                )
                .await
                {
                    Ok(index) => Some(index),
                    Err(error) => {
                        if log_divergence(held, record).is_some() {
                            report.answered.push(SegmentHeads {
                                record: *record,
                                log,
                                index: None,
                            });
                        }
                        report.unanswered = unanswered(error);
                        break;
                    }
                }
            }
        };
        report.answered.push(SegmentHeads {
            record: *record,
            log,
            index,
        });
    }
    report
}

async fn head<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: ObjectPrefix,
    start_offset: u64,
    object: SegmentObject,
) -> Result<ObjectHead, ObjectStoreError> {
    let key = layout.segment_key(&SegmentKey {
        prefix,
        start_offset,
        object,
    });
    match store.head(&key).await {
        Ok(meta) => Ok(ObjectHead::Found(meta)),
        Err(ObjectStoreError::NotFound { .. }) => Ok(ObjectHead::NotFound),
        Err(error) => Err(error),
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Whether this replica verifies the segments its partition stored: the
    /// settled primary of a tiered partition whose segments are its own, one
    /// that neither places its batches nor follows another cluster's log.
    pub(crate) fn verifies_segments(&self) -> bool {
        self.deletes_objects() && !self.places_batches() && self.peer_log.is_none()
    }

    /// How many stored segments the passes of this view found with an object
    /// missing, and how many with one that differs. Zero on a replica that
    /// does not verify.
    #[must_use]
    pub fn stored_segment_findings(&self) -> (usize, usize) {
        self.segment_verifies
            .as_deref()
            .filter(|state| self.verifies_segments() && state.view() == self.consensus().view())
            .map_or((0, 0), |state| (state.missing(), state.diverged()))
    }

    /// Whether [`Self::begin_segment_verify`] at `now`, under the verifier
    /// `grant`, would do more than return [`VerifyBegin::Idle`].
    #[must_use]
    pub fn segment_verify_due(&self, now: IggyTimestamp, grant: u64) -> bool {
        self.verifies_segments()
            && self.segment_chain.is_some()
            && self.object_prefix().is_some()
            && self
                .segment_verifies
                .as_deref()
                .filter(|state| state.view() == self.consensus().view() && state.grant() == grant)
                .is_none_or(|state| state.wants_job(now))
    }

    /// Start the next job of the verify pass, or a pass when one is due at
    /// `now`. The first pass of a view, and the first under the shard's
    /// verifier `grant`, is due at once: what a revoked verifier found
    /// counts for nothing. The caller runs the job under its scope and hands
    /// the result to [`Self::finish_segment_verify`] with its number.
    ///
    /// A replica that does not verify drops what it found, which cancels its
    /// job.
    pub fn begin_segment_verify(&mut self, now: IggyTimestamp, grant: u64) -> VerifyBegin {
        if !self.verifies_segments() {
            self.segment_verifies = None;
            return VerifyBegin::Idle;
        }
        let Some(prefix) = self.object_prefix() else {
            return VerifyBegin::Idle;
        };
        let view = self.consensus().view();
        let namespace_raw = self.namespace().inner();
        let trim_point = self.chain_trim_point();
        let Some(chain) = self.segment_chain.as_deref() else {
            return VerifyBegin::Idle;
        };
        let state = match &mut self.segment_verifies {
            Some(state) if state.view() == view && state.grant() == grant => state,
            slot => slot.insert(Box::new(SegmentVerifies::new(view, grant))),
        };
        let dropped = trim_point.map_or(0, |trim_point| state.trim(trim_point));
        if dropped > 0 {
            debug!(
                namespace_raw,
                dropped, "dropped the findings of segments the partition no longer stores"
            );
        }
        let id = self.segment_verify_jobs + 1;
        let begin = state.begin(now, id, prefix, chain.last_confirmed(), |cursor| {
            chain.confirmed_from(cursor)
        });
        if matches!(begin, VerifyBegin::Job { .. }) {
            self.segment_verify_jobs = id;
        }
        begin
    }

    /// Take the result of the verify job numbered `id`, and log each finding
    /// it made or cleared. Returns what it counts for, `None` for a result
    /// this replica does not wait for.
    pub fn finish_segment_verify(
        &mut self,
        id: u64,
        outcome: &SegmentVerifyOutcome,
    ) -> Option<VerifyTally> {
        if !self.verifies_segments() {
            self.segment_verifies = None;
            return None;
        }
        let view = self.consensus().view();
        let namespace_raw = self.namespace().inner();
        let chain = self.segment_chain.as_deref()?;
        let state = self
            .segment_verifies
            .as_deref_mut()
            .filter(|state| state.view() == view)?;
        let settled = state.finish(id, outcome, |start_offset| {
            chain.confirmed_record(start_offset)
        })?;
        let mut tally = VerifyTally {
            checked: settled.checked,
            unanswered: false,
        };
        if let Ok(SegmentVerifyReport {
            unanswered: Some(unanswered),
            ..
        }) = outcome
        {
            tally.unanswered = true;
            log_unanswered(namespace_raw, unanswered, settled.postponed);
        }
        for transition in &settled.transitions {
            log_transition(namespace_raw, transition);
        }
        Some(tally)
    }
}

fn log_unanswered(namespace_raw: u64, unanswered: &Unanswered, postponed: bool) {
    let Unanswered {
        start_offset,
        error,
    } = unanswered;
    if postponed {
        warn!(
            namespace_raw,
            start_offset,
            %error,
            "the object store did not answer for a stored segment, which waits for the next pass"
        );
    } else {
        debug!(
            namespace_raw,
            start_offset,
            %error,
            "the object store did not answer for a stored segment, which this pass asks for again"
        );
    }
}

fn log_transition(namespace_raw: u64, transition: &Transition) {
    match transition {
        Transition::Found {
            record,
            finding: Finding::Missing(object),
        } => error!(
            namespace_raw,
            start_offset = record.start_offset,
            end_offset = record.end_offset,
            object = object.extension(),
            "a segment the partition stored is missing from the object store"
        ),
        Transition::Found {
            record,
            finding: Finding::Diverged(object, divergence),
        } => error!(
            namespace_raw,
            start_offset = record.start_offset,
            end_offset = record.end_offset,
            object = object.extension(),
            %divergence,
            "a segment the partition stored differs in the object store from what the partition \
             recorded"
        ),
        Transition::Cleared { record, finding } => info!(
            namespace_raw,
            start_offset = record.start_offset,
            end_offset = record.end_offset,
            object = finding.object().extension(),
            "a segment the partition stored is in the object store as recorded again"
        ),
    }
}
