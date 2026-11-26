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
use server_common::yield_to_reactor;
use tracing::{debug, warn};

use super::{Holder, TierStore, take_slot, worth_another_attempt};

const MIB: u64 = 1024 * 1024;

/// Bytes of a body summed between two yields to the shard's other tasks.
const HASH_STRIDE: u64 = MIB;

/// How a tier store uploads a long-term segment in parts.
///
/// Each part is sent, and sent again after a failure, as a create-only put
/// whose body is held in memory is: a part with the same number and the same
/// bytes replaces itself, so no part needs a check of what the store holds.
/// A part gets no second attempt beside its first one. A part replaces what
/// is under its name, so the attempt that loses could still land after the
/// object exists: on GCS it would leave its temporary object behind, or
/// replace the one that the compose names. The request that creates the
/// object is the create-only one. It is
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
        let crc32c = match options.crc32c {
            Some(crc32c) => Some(crc32c),
            None => crc32c_in_strides(body).await,
        };
        let options = PutOptions {
            crc32c,
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
                let Some(part) = body.try_slice(range) else {
                    return Err(UploadEnd::Failed(not_in_body(upload, number)));
                };
                // Summed in the part's own future, so the parts in flight
                // move between the strides of the others.
                running.push(
                    async move {
                        let _slot = slot;
                        let sent = match crc32c_in_strides(&part).await {
                            Some(crc32c) => self.send_part(upload, number, &part, crc32c).await,
                            None => Err(not_in_body(upload, number)),
                        };
                        (number, sent)
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
    /// is held in memory, one attempt at a time.
    async fn send_part(
        &self,
        upload: &PartsUpload,
        number: u32,
        part: &PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        let mut delay = self.policy.retry_delay;
        let mut attempt = 1;
        loop {
            let error = match self.part_attempt(upload, number, part, crc32c).await {
                Ok(stored) => return Ok(stored),
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

/// The CRC32C of a body held in memory, summed [`HASH_STRIDE`] bytes at a
/// time with a yield to the shard's other tasks between two strides. `None`
/// for a body that streams.
async fn crc32c_in_strides(body: &PutBody) -> Option<u32> {
    let len = body.len();
    let mut crc32c = body.try_slice(0..len.min(HASH_STRIDE))?.crc32c()?;
    let mut start = HASH_STRIDE;
    while start < len {
        yield_to_reactor().await;
        let end = len.min(start.saturating_add(HASH_STRIDE));
        let stride = body.try_slice(start..end)?.crc32c()?;
        crc32c = crc32c::crc32c_combine(crc32c, stride, usize::try_from(end - start).ok()?);
        start = end;
    }
    Some(crc32c)
}

fn not_in_body(upload: &PartsUpload, number: u32) -> ObjectStoreError {
    ObjectStoreError::InvalidRequest(format!(
        "part {number} of {:?} is not in its body",
        upload.key
    ))
}

/// On the compio runtime, whose timer [`yield_to_reactor`] needs.
#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::pin;
    use std::rc::Rc;
    use std::task::Poll;

    use bytes::Bytes;
    use iggy_object_store::{
        DeleteOutcome, KeyLayout, ListPage, ListRequest, ObjectMeta, ObjectRange, PartsLimits,
    };
    use message_bus::IggyMessageBus;

    use super::*;
    use crate::tier_service::{StoreCallPolicy, StoreWriter};

    /// A store that takes uploads in parts at once, and keeps the number and
    /// the CRC32C of each part it gets.
    #[derive(Default)]
    struct PartsProbe {
        parts: RefCell<Vec<(u32, u32)>>,
    }

    fn whole_call<T>() -> impl Future<Output = Result<T, ObjectStoreError>> {
        future::ready(Err(ObjectStoreError::Unsupported(
            "anything but an upload in parts".to_owned(),
        )))
    }

    impl ObjectStore for PartsProbe {
        fn put(
            &self,
            _key: &str,
            _body: PutBody,
            _options: &PutOptions,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            whole_call()
        }

        fn get_range(
            &self,
            _key: &str,
            _range: Range<u64>,
        ) -> impl Future<Output = Result<ObjectRange, ObjectStoreError>> {
            whole_call()
        }

        fn head(&self, _key: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>> {
            whole_call()
        }

        fn delete(
            &self,
            _keys: &[String],
        ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>> {
            whole_call()
        }

        fn list(
            &self,
            _request: &ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, ObjectStoreError>> {
            whole_call()
        }

        fn parts_limits(&self) -> Option<PartsLimits> {
            Some(PartsLimits {
                min_part_len: 1,
                max_parts: 32,
            })
        }

        fn begin_parts(
            &self,
            key: &str,
            _options: &PutOptions,
            _layout: &KeyLayout,
        ) -> impl Future<Output = Result<PartsUpload, ObjectStoreError>> {
            future::ready(Ok(PartsUpload {
                key: key.to_owned(),
                id: "upload".to_owned(),
            }))
        }

        fn put_part(
            &self,
            _upload: &PartsUpload,
            number: u32,
            _body: PutBody,
            crc32c: u32,
        ) -> impl Future<Output = Result<StoredPart, ObjectStoreError>> {
            self.parts.borrow_mut().push((number, crc32c));
            future::ready(Ok(StoredPart {
                number,
                tag: format!("etag-{number}"),
                crc32c,
            }))
        }

        fn complete_parts(
            &self,
            _upload: &PartsUpload,
            _parts: &[StoredPart],
            _options: &PutOptions,
            _len: u64,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            future::ready(Ok(()))
        }
    }

    fn bytes_of(len: u64) -> Bytes {
        (0..len)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<u8>>()
            .into()
    }

    #[compio::test]
    async fn given_bodies_of_several_lengths_when_summing_in_strides_should_match_and_yield_between_strides()
     {
        for len in [
            0,
            1,
            HASH_STRIDE,
            HASH_STRIDE + 1,
            3 * HASH_STRIDE + HASH_STRIDE / 2,
        ] {
            let bytes = bytes_of(len);
            let body = PutBody::from_bytes(bytes.clone());
            let mut summing = pin!(crc32c_in_strides(&body));
            let mut yields = 0;

            let summed = loop {
                match futures::poll!(summing.as_mut()) {
                    Poll::Ready(summed) => break summed,
                    Poll::Pending => {
                        yields += 1;
                        compio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            };

            assert_eq!(summed, Some(crc32c::crc32c(&bytes)), "{len}");
            assert_eq!(yields, len.div_ceil(HASH_STRIDE).saturating_sub(1), "{len}");
        }
    }

    /// Every part is longer than one stride, so none goes out in the poll
    /// that begins the upload.
    #[compio::test]
    async fn given_parts_longer_than_a_stride_when_putting_in_parts_should_yield_before_sending_any()
     {
        let part_len = 2 * HASH_STRIDE;
        let bytes = bytes_of(2 * part_len + HASH_STRIDE + HASH_STRIDE / 2);
        let store = TierStore::new(
            PartsProbe::default(),
            Rc::new(IggyMessageBus::new(0)),
            StoreCallPolicy {
                parts: PartsPolicy {
                    part_len,
                    ..PartsPolicy::default()
                },
                ..StoreCallPolicy::default()
            },
            StoreWriter {
                layout: Some(KeyLayout::new("iggy").unwrap()),
                ..StoreWriter::default()
            },
        );
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            parts: PutParts::Allowed,
            crc32c: Some(crc32c::crc32c(&bytes)),
            ..PutOptions::default()
        };
        let mut put = pin!(store.put("a/b", PutBody::from_bytes(bytes.clone()), &options));

        let first = futures::poll!(put.as_mut());

        assert!(first.is_pending(), "every part was summed in one poll");
        assert!(store.provider().parts.borrow().is_empty());
        put.await.unwrap();
        let mut sent = store.provider().parts.borrow().clone();
        sent.sort_unstable();
        let expected: Vec<(u32, u32)> = (1..)
            .zip(
                bytes
                    .chunks(usize::try_from(part_len).unwrap())
                    .map(crc32c::crc32c),
            )
            .collect();
        assert_eq!(sent, expected);
    }
}
