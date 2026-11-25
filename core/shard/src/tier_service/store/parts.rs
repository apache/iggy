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

//! Uploads in parts: the parts of a long-term segment go up side by side,
//! and one request then creates the object from them.

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::pin;
use std::time::Duration;

use futures::future::{self, Either, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use iggy_object_store::{
    ObjectStore, ObjectStoreError, PartsUpload, PutBody, PutMode, PutOptions, PutParts, StoredPart,
};
use message_bus::MessageBus;
use tracing::{debug, warn};

use super::{Holder, TierStore, take_slot, worth_another_attempt};

const MIB: u64 = 1024 * 1024;

/// How a tier store uploads a long-term segment in parts.
///
/// Each part is sent, sent again and given a second attempt as a create-only
/// put whose body is held in memory is: a part with the same number and the
/// same bytes replaces itself, so no part needs a check of what the store
/// holds. The request that creates the object is the create-only one. It is
/// sent again after a failure, and an object that an earlier completion may
/// have created is checked as a put checks it. An upload that ends without
/// its object is aborted, and one that a crash left is removed by the sweep
/// of abandoned uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartsPolicy {
    /// The bytes of one part. A long-term segment of at least twice this
    /// goes up in parts, on a provider that takes them: a create-only put
    /// whose body is held in memory and whose [`PutOptions::parts`] allows
    /// it. Every other put goes whole. Zero puts every object whole. The
    /// provider's limits can make a part longer.
    pub part_len: u64,
    /// The most parts in flight at once on one store, over all its uploads
    /// in parts. An upload with no part in flight sends one even when every
    /// slot is taken, so no upload waits for the others to end.
    pub in_flight: usize,
    /// The longest an upload in parts runs from its begin. Past it, the put
    /// aborts the upload and fails as transient, so the sweep of abandoned
    /// uploads, whose grace is longer, never meets a live one. `None` for no
    /// limit.
    pub deadline: Option<Duration>,
}

/// Provisional values: parts of 8 MiB, at most eight at once, and half of
/// the default grace of the sweep.
impl Default for PartsPolicy {
    fn default() -> Self {
        Self {
            part_len: 8 * MIB,
            in_flight: 8,
            deadline: Some(Duration::from_mins(30)),
        }
    }
}

/// How one upload in parts ended without its object.
enum UploadEnd {
    /// The store does not take uploads in parts.
    Refused,
    /// The store lost the upload, so another one can succeed.
    Gone(ObjectStoreError),
    Failed(ObjectStoreError),
}

/// How the request that creates the object ended.
enum Completed {
    /// It created the object and removed what the upload kept.
    Clean,
    /// An earlier attempt created it, which a check found, so what the upload
    /// kept can still be there.
    Found,
}

impl<S, B> TierStore<S, B>
where
    S: ObjectStore,
    B: MessageBus + Clone + Unpin + 'static,
{
    /// The bytes of each part that a put of `body` with `options` goes up in,
    /// `None` when it goes whole.
    pub(super) fn parts_plan(
        &self,
        body: &PutBody,
        options: &PutOptions,
    ) -> Option<Vec<Range<u64>>> {
        let part_len = self.policy.parts.part_len;
        if part_len == 0
            || options.parts != PutParts::Allowed
            || options.mode != PutMode::IfAbsent
            || self.layout.is_none()
            || !body.is_repeatable()
        {
            return None;
        }
        let limits = self.provider.parts_limits()?;
        let len = body.len();
        if len < part_len.saturating_mul(2) {
            return None;
        }
        let part_len = part_len
            .max(limits.min_part_len)
            .max(len.div_ceil(u64::from(limits.max_parts.max(1))));
        let ranges: Vec<Range<u64>> = (0..len)
            .step_by(usize::try_from(part_len).ok()?)
            .map(|start| start..len.min(start.saturating_add(part_len)))
            .collect();
        (ranges.len() >= 2).then_some(ranges)
    }

    /// The put of `body` in the parts `ranges`. `None` when the store does
    /// not take uploads in parts, now or before, so the put goes whole.
    pub(super) async fn put_in_parts(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
        ranges: &[Range<u64>],
    ) -> Option<Result<(), ObjectStoreError>> {
        if self.parts_refused.get() {
            self.counters.record_parts_fallback();
            return None;
        }
        // The request that creates the object checks the CRC32C of all of it.
        let options = PutOptions {
            crc32c: options.crc32c.or_else(|| body.crc32c()),
            ..options.clone()
        };
        let mut upload = 1;
        loop {
            match self.upload_in_parts(key, body, &options, ranges).await {
                Ok(()) => return Some(Ok(())),
                Err(UploadEnd::Refused) => {
                    self.parts_refused.set(true);
                    self.counters.record_parts_fallback();
                    warn!(
                        key,
                        "the object store does not take uploads in parts, so this store puts \
                         every object whole from now on"
                    );
                    return None;
                }
                Err(UploadEnd::Gone(error)) if upload < self.policy.max_attempts => {
                    debug!(key, upload, %error, "the store lost an upload in parts, beginning another");
                    self.counters.record_put_retry();
                    upload += 1;
                }
                Err(UploadEnd::Gone(error) | UploadEnd::Failed(error)) => return Some(Err(error)),
            }
        }
    }

    /// One upload in parts, from its begin to its object, aborted when it
    /// ends without it.
    async fn upload_in_parts(
        &self,
        key: &str,
        body: &PutBody,
        options: &PutOptions,
        ranges: &[Range<u64>],
    ) -> Result<(), UploadEnd> {
        let Some(layout) = self.layout.as_ref() else {
            return Err(UploadEnd::Refused);
        };
        let upload = match self
            .retried("BEGIN", key, || {
                self.provider.begin_parts(key, options, layout)
            })
            .await
        {
            Ok(upload) => upload,
            Err(ObjectStoreError::Unsupported(_)) => return Err(UploadEnd::Refused),
            Err(error) => return Err(UploadEnd::Failed(error)),
        };
        self.counters.record_parts_upload();
        let parts = u32::try_from(ranges.len()).unwrap_or(u32::MAX);
        let work = self
            .parts_and_completion(&upload, body, options, ranges)
            .boxed_local();
        let ended = match self.policy.parts.deadline {
            None => work.await,
            Some(deadline) => {
                let late = pin!(self.bus.sleep(deadline));
                match future::select(work, late).await {
                    Either::Left((ended, _)) => ended,
                    Either::Right(((), work)) => {
                        // The parts in flight go before the abort, which
                        // would otherwise race them.
                        drop(work);
                        Err(UploadEnd::Failed(ObjectStoreError::Transport(format!(
                            "the upload in parts of {key:?} ran past {deadline:?}"
                        ))))
                    }
                }
            }
        };
        match ended {
            Ok(Completed::Clean) => {
                self.counters.record_parts_upload_completed();
                Ok(())
            }
            Ok(Completed::Found) => {
                self.abort(&upload, parts).await;
                self.counters.record_parts_upload_completed();
                Ok(())
            }
            Err(end) => {
                self.abort(&upload, parts).await;
                self.counters.record_parts_upload_aborted();
                Err(end)
            }
        }
    }

    async fn parts_and_completion(
        &self,
        upload: &PartsUpload,
        body: &PutBody,
        options: &PutOptions,
        ranges: &[Range<u64>],
    ) -> Result<Completed, UploadEnd> {
        let parts = self.send_parts(upload, body, ranges).await?;
        self.complete(upload, &parts, options, body.len()).await
    }

    /// Removes what `upload` kept, as far as the store lets it. What it
    /// keeps goes with the sweep of abandoned uploads.
    async fn abort(&self, upload: &PartsUpload, parts: u32) {
        if let Err(error) = self
            .bounded("ABORT", self.provider.abort_parts(upload, parts))
            .await
        {
            debug!(
                key = upload.key,
                %error,
                "aborting an upload in parts failed; the sweep of abandoned uploads removes it"
            );
        }
    }

    /// Sends the parts of `body`, side by side within the budget of the
    /// store, and returns them in order. The first part that fails for good
    /// ends the upload, and the parts in flight are dropped.
    async fn send_parts(
        &self,
        upload: &PartsUpload,
        body: &PutBody,
        ranges: &[Range<u64>],
    ) -> Result<Vec<StoredPart>, UploadEnd> {
        let mut pending: VecDeque<(u32, Range<u64>)> = (1..).zip(ranges.iter().cloned()).collect();
        let mut stored = Vec::with_capacity(ranges.len());
        let mut running = FuturesUnordered::new();
        loop {
            while let Some((number, range)) = pending.pop_front() {
                let slot = take_slot(&self.parts_in_flight, self.policy.parts.in_flight);
                if slot.is_none() && !running.is_empty() {
                    pending.push_front((number, range));
                    break;
                }
                let Some((part, crc32c)) = body
                    .try_slice(range)
                    .and_then(|part| part.crc32c().map(|crc32c| (part, crc32c)))
                else {
                    return Err(UploadEnd::Failed(ObjectStoreError::InvalidRequest(
                        format!("part {number} of {:?} is not in its body", upload.key),
                    )));
                };
                running.push(
                    async move {
                        let _slot = slot;
                        (number, self.send_part(upload, number, &part, crc32c).await)
                    }
                    .boxed_local(),
                );
            }
            let Some((_, sent)) = running.next().await else {
                break;
            };
            match sent {
                Ok(part) => stored.push(part),
                Err(error @ ObjectStoreError::NotFound { .. }) => {
                    return Err(UploadEnd::Gone(error));
                }
                Err(error) => return Err(UploadEnd::Failed(error)),
            }
        }
        stored.sort_by_key(|part| part.number);
        Ok(stored)
    }

    /// One part, sent again under the rules of a create-only put whose body
    /// is held in memory, with a second attempt once it runs past the
    /// threshold for its size.
    async fn send_part(
        &self,
        upload: &PartsUpload,
        number: u32,
        part: &PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        let threshold = self.policy.second_attempts.threshold(part.len());
        let mut delay = self.policy.retry_delay;
        let mut attempt = 1;
        loop {
            let raced = self
                .race(
                    &upload.key,
                    threshold,
                    || {
                        self.part_attempt(upload, number, part, crc32c)
                            .boxed_local()
                    },
                    |_, _| false,
                )
                .await;
            let error = match raced.result {
                Ok(stored) => {
                    if raced.by_second {
                        self.counters.record_second_attempt_win();
                    }
                    return Ok(stored);
                }
                Err(error) => error,
            };
            // The CRC32C of a part is the provider's own, so a mismatch is a
            // part that changed on the way.
            if !worth_another_attempt(&error, &PutOptions::default())
                || attempt >= self.policy.max_attempts
            {
                return Err(error);
            }
            debug!(
                key = upload.key,
                number,
                attempt,
                retry_in_ms = delay.as_millis(),
                %error,
                "a part of an upload in parts failed, sending it again"
            );
            self.counters.record_put_retry();
            self.bus.sleep(delay).await;
            delay = delay.saturating_mul(2).min(self.policy.max_retry_delay);
            attempt += 1;
        }
    }

    /// One attempt at a part, with the deadlines of a create-only put.
    async fn part_attempt(
        &self,
        upload: &PartsUpload,
        number: u32,
        part: &PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        let Some(copy) = part.try_clone() else {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "part {number} of {:?} cannot be sent again",
                upload.key
            )));
        };
        self.counters.record_part_sent();
        self.watched(
            &upload.key,
            copy,
            self.policy.attempt_stall_timeout,
            self.policy.attempt_answer_timeout,
            |watched| self.provider.put_part(upload, number, watched, crc32c),
        )
        .await
    }

    /// The request that creates the object from `parts`, `len` bytes in
    /// all. It is sent again after a failure. Once an attempt may have
    /// reached the store, the object under the key decides before each
    /// attempt and after each refusal: the put's own object ends the upload,
    /// another writer's is a conflict, and no object sends the request again.
    async fn complete(
        &self,
        upload: &PartsUpload,
        parts: &[StoredPart],
        options: &PutOptions,
        len: u64,
    ) -> Result<Completed, UploadEnd> {
        let key = upload.key.as_str();
        let mut delay = self.policy.retry_delay;
        let mut reached = false;
        let mut sent = 0;
        let mut last = None;
        loop {
            if reached {
                match self.holder(key, len, options).await {
                    Ok(Holder::Ours) => {
                        debug!(key, "an upload in parts found the object it created");
                        self.counters.record_own_object_found();
                        return Ok(Completed::Found);
                    }
                    Ok(Holder::Another) => {
                        return Err(UploadEnd::Failed(ObjectStoreError::PreconditionFailed {
                            key: key.to_owned(),
                        }));
                    }
                    Ok(Holder::Nothing) => {}
                    Err(error) => return Err(UploadEnd::Failed(error)),
                }
            }
            if sent >= self.policy.max_attempts.max(1) {
                return Err(match last {
                    Some(error @ ObjectStoreError::NotFound { .. }) => UploadEnd::Gone(error),
                    Some(error) => UploadEnd::Failed(error),
                    None => UploadEnd::Failed(ObjectStoreError::Transport(format!(
                        "the upload in parts of {key:?} was never completed"
                    ))),
                });
            }
            sent += 1;
            let error = match self
                .bounded(
                    "COMPLETE",
                    self.provider.complete_parts(upload, parts, options, len),
                )
                .await
            {
                Ok(()) => return Ok(Completed::Clean),
                Err(error) => error,
            };
            match error {
                // Nothing of this upload can have created the object, so
                // another writer did: a conflict that the caller settles.
                ObjectStoreError::PreconditionFailed { .. } if !reached => {
                    return Err(UploadEnd::Failed(error));
                }
                ObjectStoreError::NotFound { .. } if !reached => {
                    return Err(UploadEnd::Gone(error));
                }
                // An earlier attempt may have created the object, and the
                // object decides at the top of the loop.
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::NotFound { .. } => {
                    last = Some(error);
                }
                error if worth_another_attempt(&error, options) => {
                    reached |= error.put_may_have_stored();
                    debug!(
                        key,
                        sent,
                        retry_in_ms = delay.as_millis(),
                        may_have_created = reached,
                        %error,
                        "completing an upload in parts failed, sending it again"
                    );
                    self.counters.record_put_retry();
                    self.bus.sleep(delay).await;
                    delay = delay.saturating_mul(2).min(self.policy.max_retry_delay);
                    last = Some(error);
                }
                error => return Err(UploadEnd::Failed(error)),
            }
        }
    }
}
