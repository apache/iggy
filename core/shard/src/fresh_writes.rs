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

//! The upload-first produce path of partitions that place their batches.
//!
//! A send to such a partition does not go to the partition. The shard adds
//! its messages to the fresh object of the open upload window, and keeps the
//! request that will commit where they sit in it. A window takes the sends
//! of every placing partition the shard leads, so the number of uploads
//! follows the windows and not the partitions.
//!
//! A window becomes an object when it reaches [`FRESH_OBJECT_BYTES`], or
//! when the upload interval has passed since the last upload started and an
//! upload slot is free. So a send that finds the shard idle is stored at
//! once, a busy shard starts at most one object per interval plus the ones
//! that fill, and the windows grow with the load. Several objects upload at
//! the same time. An upload the store fails for a passing reason is tried
//! again under the same id, which names the same bytes for good.
//!
//! The kept requests reach their partitions only after their object is
//! stored, and each producer is answered when its own request commits.
//! Objects can finish in any order, so the requests of a partition wait in
//! one queue, in the order the sends came. The head of the queue leaves once
//! its object is stored and the partition has room for a request, and no
//! request leaves ahead of the head. When the store refuses an object for
//! good, its sends and the later sends of the same partitions are answered
//! as not accepted, so the producers send them again and no later send of a
//! partition commits ahead of an earlier one. The same answer goes to every
//! waiting send of a partition that stopped being a primary in normal
//! status.
//!
//! The partition checks a request again when it gets it. A send it then
//! refuses, such as a duplicate, leaves its messages in the object with
//! nothing that names them.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, RestorableMetadataStm, Sender, ShardFrame};
use bytes::Bytes;
use consensus::PartitionsHandle;
use iggy_binary_protocol::{GenericHeader, Operation, PrepareHeader, RoutedRequestHeader};
use iggy_common::{IggyError, IggyTimestamp};
use iggy_object_store::{ErrorClass, FreshObject, FreshObjectId, TierScope};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::{FreshObjectUploader, FreshUploadOutcome, IggyPartition};
use server_common::Message;
use server_common::send_messages::{
    ChecksumMode, admitted_blob, convert_request_message, place_request,
};
use server_common::sharding::IggyNamespace;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Bytes of messages at which a window becomes an object without waiting
/// for the upload interval. A send is never split, so an object is larger
/// only when one send is.
pub const FRESH_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

/// The most bytes of messages the shard holds for sends that have not
/// reached their partitions.
///
/// They are in the open window, in the objects that upload or wait to, and
/// stored for a partition that has no room yet. A send past the cap is
/// answered as not accepted, so a store or a partition that stopped
/// answering costs the shard this much memory and no more.
pub const FRESH_PENDING_BYTES_MAX: u64 = 8 * FRESH_OBJECT_BYTES;

/// The most of [`FRESH_PENDING_BYTES_MAX`] that the sends of one partition
/// hold. A partition that stopped draining its requests fills its own share
/// and leaves the rest of the cap to the other partitions of the shard. A
/// send larger than the share still gets in while nothing of its partition
/// waits.
pub const FRESH_PARTITION_PENDING_BYTES_MAX: u64 = FRESH_PENDING_BYTES_MAX / 4;

/// The upload interval of a shard that was given none.
pub const FRESH_UPLOAD_INTERVAL: Duration = Duration::from_millis(10);

/// The uploads a shard that was given no number runs at once.
pub const FRESH_UPLOADS: usize = 4;

/// The most attempts at storing one object, the first one included.
const FRESH_UPLOAD_ATTEMPTS: u32 = 3;

/// The wait before the second attempt at an object, doubled for each one
/// after it.
const FRESH_UPLOAD_RETRY_DELAY: Duration = Duration::from_millis(50);

/// How long an object may take from the start of its first attempt. An
/// attempt that runs past it is cancelled, and none starts past it. The
/// producers wait through the window, the upload and the commit, so this
/// stays well below the 30 s after which a client gives a request up.
pub const FRESH_UPLOAD_DEADLINE: Duration = Duration::from_secs(10);

/// The submit channel a send arrived with.
type SubmitReply = Sender<Option<Message<GenericHeader>>>;

/// A send whose messages are in a fresh object: the request that commits
/// where they sit, and where to answer its producer.
struct StagedSend {
    request: Message<RoutedRequestHeader>,
    reply: SubmitReply,
    /// The number of the object that holds the messages.
    object: u64,
    object_id: FreshObjectId,
    /// Bytes of the messages, which count against
    /// [`FRESH_PENDING_BYTES_MAX`] until the request leaves.
    bytes: u64,
}

/// What the task uploading a fresh object hands back to the pump.
pub struct FinishedFreshUpload {
    /// The number of the object.
    pub sequence: u64,
    pub outcome: FreshUploadOutcome,
}

/// The object that takes the sends that arrive.
struct Window {
    id: FreshObjectId,
    sequence: u64,
    object: FreshObject,
    /// The partitions with a send in the object.
    partitions: BTreeSet<IggyNamespace>,
    /// The bus's monotonic clock when the first send came.
    opened_at: u64,
}

/// An object that takes no more sends and is not stored yet.
struct ClosedObject {
    id: FreshObjectId,
    bytes: Bytes,
    partitions: BTreeSet<IggyNamespace>,
    opened_at: u64,
    /// Attempts started so far. The object holds an upload slot from its
    /// first attempt until it settles.
    attempts: u32,
    /// The bus's monotonic clock when the first attempt started.
    first_started_at: u64,
    /// The scope of the attempt that runs. Dropping it ends the attempt.
    running: Option<TierScope>,
    /// When the next attempt starts, after one that failed.
    retry_at: Option<u64>,
}

/// What a shard keeps for the upload-first produce path.
pub struct FreshWrites {
    uploader: RefCell<Option<Rc<dyn FreshObjectUploader>>>,
    window: RefCell<Option<Window>>,
    /// The objects that wait for an upload slot, upload, or wait to be
    /// tried again, by their number.
    closed: RefCell<BTreeMap<u64, ClosedObject>>,
    /// The sends of each partition that have not reached it, in the order
    /// they came.
    staged: RefCell<BTreeMap<IggyNamespace, VecDeque<StagedSend>>>,
    /// The partitions whose first waiting send has its object stored and
    /// found no room in the partition.
    awaiting_room: RefCell<BTreeSet<IggyNamespace>>,
    /// Bytes of the messages of every waiting send.
    pending_bytes: Cell<u64>,
    /// [`Self::pending_bytes`] of each partition that has a waiting send.
    partition_pending_bytes: RefCell<BTreeMap<IggyNamespace, u64>>,
    staged_sends: Cell<usize>,
    /// Objects that hold an upload slot.
    uploads_in_flight: Cell<usize>,
    uploads_max: Cell<usize>,
    interval_micros: Cell<u64>,
    /// The bus's monotonic clock when the last object started to upload.
    last_upload_started_at: Cell<Option<u64>>,
    /// The next object's number.
    sequence: Cell<u64>,
    /// When this shard started, which keeps its ids apart from those of its
    /// earlier life.
    started_at: u64,
    /// Results of uploads whose frame the inbox refused, for the next tick.
    undelivered: Rc<RefCell<Vec<FinishedFreshUpload>>>,
    /// The objects that hold the messages of a send that is not answered,
    /// each with how many such sends: from the moment a send joins the open
    /// window until it is refused or its request commits. A send in between
    /// may be in no journal yet, so nothing else says that its object is
    /// needed.
    awaited: Rc<RefCell<BTreeMap<u128, usize>>>,
}

impl Default for FreshWrites {
    fn default() -> Self {
        Self {
            uploader: RefCell::new(None),
            window: RefCell::new(None),
            closed: RefCell::default(),
            staged: RefCell::default(),
            awaiting_room: RefCell::default(),
            pending_bytes: Cell::new(0),
            partition_pending_bytes: RefCell::default(),
            staged_sends: Cell::new(0),
            uploads_in_flight: Cell::new(0),
            uploads_max: Cell::new(FRESH_UPLOADS),
            interval_micros: Cell::new(micros(FRESH_UPLOAD_INTERVAL)),
            last_upload_started_at: Cell::new(None),
            sequence: Cell::new(0),
            started_at: IggyTimestamp::now().as_micros(),
            undelivered: Rc::default(),
            awaited: Rc::default(),
        }
    }
}

impl FreshWrites {
    /// The fresh objects of this shard that the sweep must keep whatever
    /// the partitions name: the open window, every object that is not
    /// stored yet, and every object with a send that is not answered,
    /// whether the send waits for its partition or for its commit.
    pub(crate) fn awaited_objects(&self) -> BTreeSet<u128> {
        let mut objects: BTreeSet<u128> = self.awaited.borrow().keys().copied().collect();
        objects.extend(self.window.borrow().as_ref().map(|window| window.id.get()));
        objects.extend(self.closed.borrow().values().map(|object| object.id.get()));
        objects
    }

    /// Keep `send` at the end of the queue of `namespace`, its bytes in the
    /// share of its partition, and its object among the awaited ones.
    fn stage_send(&self, namespace: IggyNamespace, send: StagedSend) {
        *self
            .awaited
            .borrow_mut()
            .entry(send.object_id.get())
            .or_default() += 1;
        self.pending_bytes
            .set(self.pending_bytes.get() + send.bytes);
        self.hold_in_share(namespace, send.bytes);
        self.staged_sends.set(self.staged_sends.get() + 1);
        self.staged
            .borrow_mut()
            .entry(namespace)
            .or_default()
            .push_back(send);
    }

    /// Whether the object `sequence` still takes sends or is not stored.
    fn is_unsettled(&self, sequence: u64) -> bool {
        self.closed.borrow().contains_key(&sequence)
            || self
                .window
                .borrow()
                .as_ref()
                .is_some_and(|window| window.sequence == sequence)
    }

    /// Whether the upload interval has passed since the last upload started.
    fn interval_passed(&self, now: u64) -> bool {
        self.last_upload_started_at
            .get()
            .is_none_or(|started_at| now.saturating_sub(started_at) >= self.interval_micros.get())
    }

    const fn has_free_upload(&self) -> bool {
        self.uploads_in_flight.get() < self.uploads_max.get()
    }

    /// Bytes of the messages of the waiting sends of `namespace`.
    fn partition_pending(&self, namespace: IggyNamespace) -> u64 {
        self.partition_pending_bytes
            .borrow()
            .get(&namespace)
            .copied()
            .unwrap_or(0)
    }

    /// Count `bytes` of a waiting send of `namespace` in the share of its
    /// partition.
    fn hold_in_share(&self, namespace: IggyNamespace, bytes: u64) {
        *self
            .partition_pending_bytes
            .borrow_mut()
            .entry(namespace)
            .or_default() += bytes;
    }

    /// Take `bytes` of a send of `namespace` that left out of the share of
    /// its partition.
    fn release_from_share(&self, namespace: IggyNamespace, bytes: u64) {
        let mut partitions = self.partition_pending_bytes.borrow_mut();
        if let Some(held) = partitions.get_mut(&namespace) {
            *held = held.saturating_sub(bytes);
            if *held == 0 {
                partitions.remove(&namespace);
            }
        }
    }

    /// Move the open window to the closed objects, where it waits for an
    /// upload slot.
    fn close_window(&self) -> Option<u64> {
        let Window {
            id,
            sequence,
            object,
            partitions,
            opened_at,
        } = self.window.borrow_mut().take()?;
        self.closed.borrow_mut().insert(
            sequence,
            ClosedObject {
                id,
                bytes: object.into_bytes(),
                partitions,
                opened_at,
                attempts: 0,
                first_started_at: 0,
                running: None,
                retry_at: None,
            },
        );
        Some(sequence)
    }

    /// When the pump must look at the uploads again without an event: the
    /// open window comes due, an object is tried again, or an attempt runs
    /// out of time. On the bus's monotonic clock.
    fn next_deadline(&self) -> Option<u64> {
        let window_due = (self.window.borrow().is_some() && self.has_free_upload())
            .then(|| self.last_upload_started_at.get())
            .flatten()
            .map(|started_at| started_at.saturating_add(self.interval_micros.get()));
        let closed = self.closed.borrow();
        let objects = closed.values().filter_map(|object| {
            object.retry_at.or_else(|| {
                object.running.is_some().then(|| {
                    object
                        .first_started_at
                        .saturating_add(micros(FRESH_UPLOAD_DEADLINE))
                })
            })
        });
        objects.chain(window_due).min()
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the uploader of its fresh objects, or take it away
    /// with `None`. An upload already in flight ends on the uploader that
    /// started it.
    pub fn set_fresh_uploader(&self, uploader: Option<Rc<dyn FreshObjectUploader>>) {
        *self.fresh_writes.uploader.borrow_mut() = uploader;
    }

    /// The uploader of this shard's fresh objects. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn fresh_uploader(&self) -> Option<Rc<dyn FreshObjectUploader>> {
        self.fresh_writes.uploader.borrow().clone()
    }

    /// Set the shortest time between the starts of two uploads that no full
    /// object forces, and how many uploads run at once. Fewer than one
    /// upload counts as one.
    pub fn set_fresh_upload_pace(&self, interval: Duration, uploads: usize) {
        self.fresh_writes.interval_micros.set(micros(interval));
        self.fresh_writes.uploads_max.set(uploads.max(1));
    }

    /// Whether `request` is a send that goes to the object store before its
    /// partition: a `SendMessages` for a partition here that places its
    /// batches and that this replica leads. A backup answers such a send
    /// itself, with where the primary is.
    pub(crate) fn places_send(&self, request: &RoutedRequestHeader) -> bool {
        request.operation == Operation::SendMessages
            && self
                .plane
                .partitions()
                .get_by_ns(&IggyNamespace::from_raw(request.group))
                .is_some_and(|partition| {
                    partition.places_batches() && partition.consensus().is_primary()
                })
    }

    /// Add the messages of `request` to the open window, and keep the
    /// request that commits where they sit. Answers the producer here when
    /// the send cannot be taken.
    pub(crate) fn offer_fresh_send(
        &self,
        request: Message<RoutedRequestHeader>,
        reply: SubmitReply,
    ) {
        let header = *request.header();
        let deny = |error: &IggyError| {
            self.metrics.record_fresh_send_denied();
            Self::answer_partition_submit_deny(&header, Some(reply.clone()), error);
        };
        let fresh = &self.fresh_writes;
        if fresh.uploader.borrow().is_none() {
            // The node does not pass the store identity check, which it may
            // pass again.
            deny(&IggyError::TransientNotAccepted);
            return;
        }
        let namespace = IggyNamespace::from_raw(header.group);
        // Asked before any work on the messages: the partition would refuse
        // the request after the upload, and the producer would have waited
        // for an object that nothing names.
        if !self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(IggyPartition::takes_placed_sends)
        {
            deny(&IggyError::TransientNotAccepted);
            return;
        }
        // By the size the request declares, which bounds its messages, so a
        // shard that is full does not read the messages it turns away.
        if self.fresh_send_over_pending_bytes(namespace, u64::from(header.size)) {
            deny(&IggyError::TransientNotAccepted);
            return;
        }
        let admitted = match convert_request_message(namespace, request, ChecksumMode::Skip) {
            Ok(admitted) => admitted,
            Err(error) => {
                deny(&error);
                return;
            }
        };
        let Ok(blob) = admitted_blob(&admitted) else {
            deny(&IggyError::InvalidCommand);
            return;
        };
        let bytes = blob.len() as u64;
        // A send is never split, and one that does not fit starts the next
        // object.
        if fresh
            .window
            .borrow()
            .as_ref()
            .is_some_and(|window| window.object.len() + bytes > FRESH_OBJECT_BYTES)
        {
            fresh.close_window();
        }
        let mut slot = fresh.window.borrow_mut();
        if slot.is_none() {
            let sequence = fresh.sequence.get();
            let Some(id) = FreshObjectId::new(
                self.partition_consensus.self_replica_id,
                self.id,
                fresh.started_at,
                sequence,
            ) else {
                deny(&IggyError::TransientNotAccepted);
                return;
            };
            fresh.sequence.set(sequence + 1);
            *slot = Some(Window {
                id,
                sequence,
                object: FreshObject::with_capacity(blob.len()),
                partitions: BTreeSet::new(),
                opened_at: self.bus.monotonic_micros(),
            });
        }
        let Some(window) = slot.as_mut() else {
            return;
        };
        // The position is where the blob will start, so the request can be
        // built before the blob is copied in.
        let placed = match place_request(&admitted, window.id.get(), window.object.len()) {
            Ok(placed) => placed,
            Err(error) => {
                deny(&error);
                return;
            }
        };
        window.object.push(blob);
        window.partitions.insert(namespace);
        let full = window.object.len() >= FRESH_OBJECT_BYTES;
        fresh.stage_send(
            namespace,
            StagedSend {
                request: placed,
                reply,
                object: window.sequence,
                object_id: window.id,
                bytes,
            },
        );
        drop(slot);
        if full {
            fresh.close_window();
        }
        self.drive_fresh_uploads();
    }

    /// Whether a send of `size` bytes to `namespace` would take the waiting
    /// sends of its partition past [`FRESH_PARTITION_PENDING_BYTES_MAX`], or
    /// those of the shard past [`FRESH_PENDING_BYTES_MAX`]. Counts the bound
    /// it passes. A send larger than a bound still gets in while nothing
    /// that counts against it waits.
    fn fresh_send_over_pending_bytes(&self, namespace: IggyNamespace, size: u64) -> bool {
        let fresh = &self.fresh_writes;
        let partition_pending = fresh.partition_pending(namespace);
        if partition_pending > 0
            && partition_pending.saturating_add(size) > FRESH_PARTITION_PENDING_BYTES_MAX
        {
            self.metrics.record_fresh_send_over_partition_share();
            return true;
        }
        let pending = fresh.pending_bytes.get();
        if pending > 0 && pending.saturating_add(size) > FRESH_PENDING_BYTES_MAX {
            self.metrics.record_fresh_send_over_shard_cap();
            return true;
        }
        false
    }

    /// When the pump must call [`Self::drive_fresh_uploads`] again without
    /// an event, on the bus's monotonic clock.
    pub(crate) fn fresh_uploads_deadline(&self) -> Option<u64> {
        self.fresh_writes.next_deadline()
    }

    /// Start every upload that is due and has a slot: the objects that wait
    /// to be tried again, then the closed objects in their order, then the
    /// open window once the upload interval has passed. Gives up the objects
    /// whose attempt ran out of time. The pump calls it after a send, when an upload
    /// ends, at [`Self::fresh_uploads_deadline`] and on its tick.
    pub(crate) fn drive_fresh_uploads(&self) {
        let fresh = &self.fresh_writes;
        let now = self.bus.monotonic_micros();
        let mut starts = Vec::new();
        let mut expired = Vec::new();
        for (sequence, object) in fresh.closed.borrow_mut().iter_mut() {
            if object.retry_at.is_some_and(|retry_at| retry_at <= now) {
                object.retry_at = None;
                starts.push(*sequence);
            } else if object.running.is_some()
                && now.saturating_sub(object.first_started_at) >= micros(FRESH_UPLOAD_DEADLINE)
            {
                expired.push(*sequence);
            }
        }
        for sequence in expired {
            tracing::warn!(
                shard = self.id,
                "storing a fresh object took too long; its sends are answered as not accepted"
            );
            // Dropping the object drops the scope of its attempt, which
            // cancels the attempt and frees its slot in the tier service.
            // The producers are answered here and not when the cancelled
            // attempt reports, which an uploader may never do.
            self.fail_fresh_object(sequence);
        }
        let free = fresh
            .uploads_max
            .get()
            .saturating_sub(fresh.uploads_in_flight.get());
        starts.extend(
            fresh
                .closed
                .borrow()
                .iter()
                .filter(|(_, object)| object.attempts == 0)
                .take(free)
                .map(|(sequence, _)| *sequence),
        );
        for sequence in starts {
            self.start_fresh_upload(sequence, now);
        }
        if fresh.has_free_upload()
            && fresh.interval_passed(now)
            && let Some(sequence) = fresh.close_window()
        {
            self.start_fresh_upload(sequence, now);
        }
        self.metrics
            .set_fresh_load(fresh.uploads_in_flight.get(), fresh.staged_sends.get());
    }

    /// Start an attempt at storing the closed object `sequence`.
    fn start_fresh_upload(&self, sequence: u64, now: u64) {
        let fresh = &self.fresh_writes;
        let uploader = fresh.uploader.borrow().clone();
        let sender = self.senders.get(self.id as usize).cloned();
        let (Some(uploader), Some(sender)) = (uploader, sender) else {
            // Without the uploader the node lost the store identity check,
            // and an upload that cannot report back must not start.
            self.fail_fresh_object(sequence);
            return;
        };
        let scope = TierScope::new();
        let token = scope.token();
        let (id, bytes) = {
            let mut closed = fresh.closed.borrow_mut();
            let Some(object) = closed.get_mut(&sequence) else {
                return;
            };
            if object.attempts == 0 {
                object.first_started_at = now;
                fresh
                    .uploads_in_flight
                    .set(fresh.uploads_in_flight.get() + 1);
                fresh.last_upload_started_at.set(Some(now));
                self.metrics.record_fresh_window_wait(Duration::from_micros(
                    now.saturating_sub(object.opened_at),
                ));
            }
            object.attempts += 1;
            object.running = Some(scope);
            (object.id, object.bytes.clone())
        };
        let undelivered = Rc::clone(&fresh.undelivered);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedFreshUpload {
                sequence,
                outcome: uploader.upload(id, bytes, token).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::FreshUploadFinished(finished));
            // The producers of the object wait for this result, so a refused
            // frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::FreshUploadFinished(finished)) = frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    /// The pump's tick: settle the uploads whose frame the inbox refused,
    /// look at every partition with waiting sends, and start the uploads
    /// that are due. Everything here also runs on its own event, so the
    /// tick only catches what an event missed, such as a partition that
    /// lost primacy while its sends wait for an upload.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn tick_fresh_writes(&self)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let undelivered = std::mem::take(&mut *self.fresh_writes.undelivered.borrow_mut());
        for finished in undelivered {
            self.on_fresh_upload_finished(finished).await;
        }
        let staged: Vec<_> = self.fresh_writes.staged.borrow().keys().copied().collect();
        for namespace in staged {
            self.feed_staged_sends(namespace).await;
        }
        self.drive_fresh_uploads();
    }

    /// Settle an attempt at storing an object: release the sends of a
    /// stored object to their partitions, start the next attempt at one the
    /// store failed for a passing reason, or answer every producer of an
    /// object that failed for good.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_fresh_upload_finished(&self, finished: FinishedFreshUpload)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let fresh = &self.fresh_writes;
        let sequence = finished.sequence;
        let now = self.bus.monotonic_micros();
        let stored = {
            let mut closed = fresh.closed.borrow_mut();
            let Some(object) = closed.get_mut(&sequence) else {
                return;
            };
            object.running = None;
            match &finished.outcome {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    let delay = micros(FRESH_UPLOAD_RETRY_DELAY)
                        .saturating_mul(1_u64 << object.attempts.saturating_sub(1).min(16));
                    let retry_at = now.saturating_add(delay);
                    if error.class() == ErrorClass::Transient
                        && object.attempts < FRESH_UPLOAD_ATTEMPTS
                        && retry_at.saturating_sub(object.first_started_at)
                            < micros(FRESH_UPLOAD_DEADLINE)
                    {
                        tracing::debug!(
                            shard = self.id,
                            attempt = object.attempts,
                            %error,
                            "storing a fresh object failed; trying again under the same id"
                        );
                        object.retry_at = Some(retry_at);
                        self.metrics.record_fresh_upload_retry();
                        return;
                    }
                    tracing::warn!(
                        shard = self.id,
                        attempts = object.attempts,
                        %error,
                        "storing a fresh object failed; its sends are answered as not accepted"
                    );
                    false
                }
                Err(_) => {
                    tracing::warn!(
                        shard = self.id,
                        attempts = object.attempts,
                        "the upload of a fresh object was cancelled; its sends are answered \
                         as not accepted"
                    );
                    false
                }
            }
        };
        let Some(object) = (if stored {
            self.take_settled_fresh_object(sequence, true)
        } else {
            self.fail_fresh_object(sequence)
        }) else {
            return;
        };
        // A consumer at the tail polls these batches next, and this shard
        // holds their messages already.
        if stored && let Some(remote) = self.plane.partitions().remote_segments() {
            remote.cache_fresh_object(object.id.get(), object.bytes);
        }
        for namespace in object.partitions {
            self.feed_staged_sends(namespace).await;
        }
        self.drive_fresh_uploads();
    }

    /// Take the closed object `sequence` out, give its upload slot back and
    /// count its upload.
    fn take_settled_fresh_object(&self, sequence: u64, stored: bool) -> Option<ClosedObject> {
        let fresh = &self.fresh_writes;
        let object = fresh.closed.borrow_mut().remove(&sequence)?;
        if object.attempts > 0 {
            fresh
                .uploads_in_flight
                .set(fresh.uploads_in_flight.get().saturating_sub(1));
            self.metrics.record_fresh_upload(
                object.bytes.len() as u64,
                Duration::from_micros(
                    self.bus
                        .monotonic_micros()
                        .saturating_sub(object.first_started_at),
                ),
                stored,
            );
        }
        Some(object)
    }

    /// Give the object `sequence` up: answer its sends as not accepted, and
    /// with them the later sends of the same partitions. A later send that
    /// committed would sit ahead of the earlier one its producer sends
    /// again.
    fn fail_fresh_object(&self, sequence: u64) -> Option<ClosedObject> {
        let object = self.take_settled_fresh_object(sequence, false)?;
        for namespace in &object.partitions {
            let denied = self
                .fresh_writes
                .staged
                .borrow_mut()
                .get_mut(namespace)
                .map(|queue| {
                    let first = queue.partition_point(|send| send.object < sequence);
                    queue.split_off(first)
                });
            for send in denied.into_iter().flatten() {
                self.deny_staged_send(*namespace, send);
            }
        }
        Some(object)
    }

    /// Hand the waiting sends of `namespace` to its partition, in the order
    /// they came: each one whose object is stored, while the partition has
    /// room for a request. Answers them all as not accepted when the
    /// partition no longer takes placed sends.
    #[allow(clippy::future_not_send)]
    async fn feed_staged_sends(&self, namespace: IggyNamespace)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let fresh = &self.fresh_writes;
        loop {
            let partitions = self.plane.partitions();
            let room = partitions.get_by_ns(&namespace).and_then(|partition| {
                partition
                    .takes_placed_sends()
                    .then(|| partition.has_request_room())
            });
            let Some(room) = room else {
                fresh.awaiting_room.borrow_mut().remove(&namespace);
                let dropped = fresh.staged.borrow_mut().remove(&namespace);
                for send in dropped.into_iter().flatten() {
                    self.deny_staged_send(namespace, send);
                }
                return;
            };
            let send = {
                let mut staged = fresh.staged.borrow_mut();
                let head = staged
                    .get(&namespace)
                    .and_then(|queue| queue.front().map(|send| send.object));
                let Some(object) = head else {
                    staged.remove(&namespace);
                    fresh.awaiting_room.borrow_mut().remove(&namespace);
                    return;
                };
                if fresh.is_unsettled(object) {
                    fresh.awaiting_room.borrow_mut().remove(&namespace);
                    return;
                }
                if !room {
                    fresh.awaiting_room.borrow_mut().insert(namespace);
                    return;
                }
                staged.get_mut(&namespace).and_then(VecDeque::pop_front)
            };
            let Some(StagedSend {
                request,
                reply,
                object_id,
                bytes,
                ..
            }) = send
            else {
                return;
            };
            self.release_staged_send(namespace, bytes);
            let (sender, receiver) = consensus::oneshot_channel();
            partitions
                .on_placed_request_with_reply(request, Some(sender))
                .await;
            // Awaited off the pump, as for any other submit: the commit that
            // fires the receiver needs the pump to keep draining acks.
            let awaited = Rc::clone(&fresh.awaited);
            let object = object_id.get();
            let commit_latency = self.metrics.fresh_commit_latency();
            let handed_at = Instant::now();
            self.bus.spawn(async move {
                let committed = receiver.await.ok().map(Message::into_generic);
                commit_latency.observe(handed_at.elapsed().as_secs_f64());
                settle_awaited(&awaited, object);
                let _ = reply.try_send(committed);
            });
            // An object can hold thousands of sends, and the tasks and
            // timers of the shard must not wait for all of them.
            self.cooperate().await;
        }
    }

    /// Hand over the sends of the partitions that had no room, where room
    /// came since. The pump calls it after every event, because the commit
    /// that frees a partition's room is one.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn feed_fresh_sends_awaiting_room(&self)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        if self.fresh_writes.awaiting_room.borrow().is_empty() {
            return;
        }
        let awaiting: Vec<_> = self
            .fresh_writes
            .awaiting_room
            .borrow()
            .iter()
            .copied()
            .collect();
        for namespace in awaiting {
            self.feed_staged_sends(namespace).await;
        }
        self.metrics.set_fresh_load(
            self.fresh_writes.uploads_in_flight.get(),
            self.fresh_writes.staged_sends.get(),
        );
    }

    /// A waiting send of `namespace` left: its bytes no longer count.
    fn release_staged_send(&self, namespace: IggyNamespace, bytes: u64) {
        let fresh = &self.fresh_writes;
        fresh
            .pending_bytes
            .set(fresh.pending_bytes.get().saturating_sub(bytes));
        fresh.release_from_share(namespace, bytes);
        fresh
            .staged_sends
            .set(fresh.staged_sends.get().saturating_sub(1));
    }

    fn deny_staged_send(&self, namespace: IggyNamespace, send: StagedSend) {
        self.release_staged_send(namespace, send.bytes);
        settle_awaited(&self.fresh_writes.awaited, send.object_id.get());
        self.metrics.record_fresh_send_denied();
        Self::answer_partition_submit_deny(
            send.request.header(),
            Some(send.reply),
            &IggyError::TransientNotAccepted,
        );
    }

    /// Bytes of the messages of the waiting sends of `namespace`, and of
    /// every waiting send.
    #[cfg(test)]
    pub(crate) fn fresh_pending_bytes(&self, namespace: IggyNamespace) -> (u64, u64) {
        let fresh = &self.fresh_writes;
        (
            fresh.partition_pending(namespace),
            fresh.pending_bytes.get(),
        )
    }
}

/// One send of `object` was answered.
fn settle_awaited(awaited: &RefCell<BTreeMap<u128, usize>>, object: u128) {
    let mut awaited = awaited.borrow_mut();
    if let Some(sends) = awaited.get_mut(&object) {
        *sends -= 1;
        if *sends == 0 {
            awaited.remove(&object);
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
