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
//! that fill, and the windows grow with the load. The interval of a window
//! is the shortest of its sends: each send's topic can set its own, the
//! longest time the send waits for its upload to start, and the others take
//! the shard's. A send of a topic with a long interval goes with an earlier
//! upload that a send of another topic starts. Several objects upload at
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
//!
//! An object describes itself: its index names the partition of each batch,
//! and carries the proof rows of the batches that the partitions this shard
//! leads committed since the last object, so a reader of the store alone
//! finds which batches committed and where. The shard builds the index and
//! the requests that commit the batches when it seals the window, as only
//! then is the position of each blob known. The rows an object carried that
//! failed for good go into the next object.
//!
//! A send to a partition whose long-term backlog is past its limit waits a
//! while before the shard takes it, and the later sends to that partition
//! wait behind it, so none is overtaken. It is delayed and never refused,
//! since a producer answered as not accepted sends all its messages again.
//! Its bytes count as pending, in the cap of the shard and in the share of
//! its partition, only once the shard takes it, which is when it is checked
//! against both.

use crate::rewrite_pacing::backlog_delay;
use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, RestorableMetadataStm, Sender, ShardFrame};
use bytes::Bytes;
use consensus::PartitionsHandle;
use iggy_binary_protocol::batch::BatchHeader;
use iggy_binary_protocol::placement::{self, Placement};
use iggy_binary_protocol::{GenericHeader, Operation, PrepareHeader, RoutedRequestHeader};
use iggy_common::IggyError;
use iggy_object_store::{
    ErrorClass, FRESH_OBJECT_BATCHES_MAX, FRESH_OBJECT_DATA_BYTES_MAX, FRESH_OBJECT_PROOFS_MAX,
    FreshBatch, FreshObjectHeader, FreshObjectId, FreshPartition, FreshProducer, FreshProofRow,
    SealedFreshObject, TierScope,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::{
    FreshObjectUploader, FreshProofs, FreshUploadOutcome, IggyPartition, RebuiltProofs,
};
use server_common::Message;
use server_common::send_messages::{
    ChecksumMode, admitted_batch, admitted_blob, convert_request_message, place_request,
};
use server_common::sharding::IggyNamespace;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Bytes of messages at which a window becomes an object without waiting
/// for the upload interval.
///
/// It is the most data an object of more than one batch holds. A send is
/// never split, so an object is larger only when one send is.
pub const FRESH_OBJECT_BYTES: u64 = FRESH_OBJECT_DATA_BYTES_MAX;

/// The most bytes of messages a shard that was given no other cap holds for
/// sends that have not reached their partitions.
///
/// They are in the open window, in the objects that upload or wait to, and
/// stored for a partition that has no room yet. A send past the cap is
/// answered as not accepted, so a store or a partition that stopped
/// answering costs the shard this much memory and no more.
pub const FRESH_PENDING_BYTES_MAX: u64 = 8 * FRESH_OBJECT_BYTES;

/// The most of the cap that the sends of one partition hold, on a shard
/// that was given no other share.
///
/// A partition that stopped draining its requests fills its own share and
/// leaves the rest of the cap to the other partitions of the shard. A send
/// larger than the share still gets in while nothing of its partition waits.
pub const FRESH_PARTITION_PENDING_BYTES_MAX: u64 = FRESH_PENDING_BYTES_MAX / 4;

/// The upload interval of a shard that was given none.
pub const FRESH_UPLOAD_INTERVAL: Duration = Duration::from_millis(10);

/// The uploads a shard that was given no number runs at once.
pub const FRESH_UPLOADS: usize = 16;

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

/// How long the proof rows of a shard wait for an object that carries
/// data before the shard stores them in an object of their own.
///
/// It is also the shortest time between two such objects, and the wait of
/// a shard that was given no other. A shard whose windows close sooner
/// writes none.
pub const FRESH_CONFIRM_AFTER: Duration = Duration::from_millis(250);

/// How long a partition whose walk left proof rows missing waits before it
/// walks again.
const PROOF_WALK_RETRY: Duration = Duration::from_secs(1);

/// The submit channel a send arrived with.
type SubmitReply = Sender<Option<Message<GenericHeader>>>;

/// A send whose messages go into a fresh object, and where to answer its
/// producer.
struct StagedSend {
    /// The send as admitted, with its messages, until its object is sealed,
    /// and from then on the request that commits where they sit.
    request: Message<RoutedRequestHeader>,
    reply: SubmitReply,
    /// The number of the object that holds the messages.
    object: u64,
    object_id: FreshObjectId,
    /// Bytes of the messages, which count against the cap of the shard and
    /// the share of the partition until the request leaves.
    bytes: u64,
    /// The header of its batch, as admitted.
    batch: BatchHeader,
    /// XXH3 of its messages, as its placement records it.
    blob_checksum: u64,
}

/// A send held back because the rewrites of its partition fell behind.
struct DelayedSend {
    request: Message<RoutedRequestHeader>,
    reply: SubmitReply,
    /// The bus's monotonic clock when the shard takes it.
    due_at: u64,
    /// The upload interval of the send, see [`Window::interval_micros`].
    interval_micros: u64,
}

impl DelayedSend {
    const fn new(
        request: Message<RoutedRequestHeader>,
        reply: SubmitReply,
        due_at: u64,
        interval_micros: u64,
    ) -> Self {
        Self {
            request,
            reply,
            due_at,
            interval_micros,
        }
    }
}

/// What the task that rebuilds the proof rows of a partition hands back to
/// the pump.
struct FinishedProofWalk {
    namespace: IggyNamespace,
    created_revision: u64,
    walk: u64,
    rebuilt: RebuiltProofs,
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
    /// Bytes of the messages of its sends.
    bytes: u64,
    sends: u32,
    /// The partitions with a send in the object.
    partitions: BTreeSet<IggyNamespace>,
    /// The bus's monotonic clock when the first send came.
    opened_at: u64,
    /// The shortest upload interval of its sends, in microseconds: the one
    /// that the topic of a send sets, or else the shard's.
    interval_micros: u64,
}

/// An object that takes no more sends and is not stored yet.
struct ClosedObject {
    id: FreshObjectId,
    bytes: Bytes,
    /// The partitions with a send in the object, none for one that carries
    /// proof rows alone.
    partitions: BTreeSet<IggyNamespace>,
    /// Of each partition whose proof rows it carries, the offset the first
    /// of them starts at.
    proofs: Vec<(IggyNamespace, u64)>,
    /// When the oldest of those rows began to wait, on the bus's monotonic
    /// clock.
    proofs_since: Option<u64>,
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

/// Where the sealing of one partition stands, in one view of one of its
/// incarnations.
struct SealCursor {
    created_revision: u64,
    view: u32,
    /// The offset of the first proof row that no object that is stored or
    /// uploading carries.
    next_to_seal: u64,
    /// The offset the writer expects the next batch it seals to commit at.
    /// `None` once a send was refused, until the next seal predicts again.
    next_claim: Option<u64>,
}

impl SealCursor {
    fn of<B: MessageBus, SB: SuperblockStore>(partition: &IggyPartition<B, SB>) -> Self {
        Self {
            created_revision: partition.created_revision(),
            view: partition.consensus().view(),
            next_to_seal: partition
                .fresh_proofs()
                .map_or(0, FreshProofs::first_unproven),
            next_claim: None,
        }
    }

    const fn is_of<B: MessageBus, SB: SuperblockStore>(
        &self,
        partition: &IggyPartition<B, SB>,
    ) -> bool {
        self.created_revision == partition.created_revision()
            && self.view == partition.consensus().view()
    }
}

/// What an object being sealed holds of one partition, besides the blobs.
struct SealedPartition {
    namespace: IggyNamespace,
    created_revision: u64,
    view: u32,
    claimed_first_offset: u64,
    proven_below: u64,
    proofs: Vec<FreshProofRow>,
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
    /// The sends of each partition held back for its rewrite backlog, in
    /// the order they came. None of their bytes count as pending yet.
    delayed: RefCell<BTreeMap<IggyNamespace, VecDeque<DelayedSend>>>,
    /// Bytes of the messages of every waiting send.
    pending_bytes: Cell<u64>,
    /// [`Self::pending_bytes`] of each partition that has a waiting send.
    partition_pending_bytes: RefCell<BTreeMap<IggyNamespace, u64>>,
    /// The cap of [`Self::pending_bytes`].
    pending_bytes_max: Cell<u64>,
    /// The share of the cap that the waiting sends of one partition hold.
    partition_pending_bytes_max: Cell<u64>,
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
    /// Of each partition this shard sealed for, where that stands.
    cursors: RefCell<BTreeMap<IggyNamespace, SealCursor>>,
    /// The partitions whose proof rows wait for an object, each with when
    /// the shard learned of them, on the bus's monotonic clock.
    proofs_waiting: RefCell<BTreeMap<IggyNamespace, u64>>,
    /// `StoreIdentity::cluster_uid` of the store the objects go to.
    cluster_uid: Cell<u128>,
    /// The bus's monotonic clock when the last object of proof rows alone
    /// was sealed.
    last_confirmation_at: Cell<Option<u64>>,
    confirm_after_micros: Cell<u64>,
    /// The partitions whose missing proof rows a walk should rebuild, as the
    /// tick found them.
    proof_walks_wanted: RefCell<BTreeSet<IggyNamespace>>,
    /// The walks that ended, for the next tick.
    proof_walks: Rc<RefCell<Vec<FinishedProofWalk>>>,
    /// When a partition whose walk left rows missing may walk again, on the
    /// bus's monotonic clock.
    proof_walk_retry_at: RefCell<BTreeMap<IggyNamespace, u64>>,
}

impl FreshWrites {
    /// The state of a shard that started at `started_at`, in bus
    /// microseconds since the Unix epoch.
    pub(crate) fn new(started_at: u64) -> Self {
        Self {
            uploader: RefCell::new(None),
            window: RefCell::new(None),
            closed: RefCell::default(),
            staged: RefCell::default(),
            awaiting_room: RefCell::default(),
            delayed: RefCell::default(),
            pending_bytes: Cell::new(0),
            partition_pending_bytes: RefCell::default(),
            pending_bytes_max: Cell::new(FRESH_PENDING_BYTES_MAX),
            partition_pending_bytes_max: Cell::new(FRESH_PARTITION_PENDING_BYTES_MAX),
            staged_sends: Cell::new(0),
            uploads_in_flight: Cell::new(0),
            uploads_max: Cell::new(FRESH_UPLOADS),
            interval_micros: Cell::new(micros(FRESH_UPLOAD_INTERVAL)),
            last_upload_started_at: Cell::new(None),
            sequence: Cell::new(0),
            started_at,
            undelivered: Rc::default(),
            awaited: Rc::default(),
            cursors: RefCell::default(),
            proofs_waiting: RefCell::default(),
            cluster_uid: Cell::new(0),
            last_confirmation_at: Cell::new(None),
            confirm_after_micros: Cell::new(micros(FRESH_CONFIRM_AFTER)),
            proof_walks_wanted: RefCell::default(),
            proof_walks: Rc::default(),
            proof_walk_retry_at: RefCell::default(),
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

    /// Whether the open window can become an object at `now`: its upload
    /// interval passed since the last upload started. False without one.
    fn window_due(&self, now: u64) -> bool {
        let Some(interval) = self
            .window
            .borrow()
            .as_ref()
            .map(|window| window.interval_micros)
        else {
            return false;
        };
        self.last_upload_started_at
            .get()
            .is_none_or(|started_at| now.saturating_sub(started_at) >= interval)
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

    /// The lowest offset of a proof row of `namespace` that no stored object
    /// carries, for a writer whose next row to seal starts at
    /// `next_to_seal`: the rows of an object still uploading count as not
    /// stored.
    fn unstored_from(&self, namespace: IggyNamespace, next_to_seal: u64) -> u64 {
        self.closed
            .borrow()
            .values()
            .flat_map(|object| &object.proofs)
            .filter(|(carried, _)| *carried == namespace)
            .map(|(_, first)| *first)
            .fold(next_to_seal, u64::min)
    }

    /// A send of `namespace` was refused, so the batches sealed after it no
    /// longer commit where they were expected to.
    fn forget_claim(&self, namespace: IggyNamespace) {
        if let Some(cursor) = self.cursors.borrow_mut().get_mut(&namespace) {
            cursor.next_claim = None;
        }
    }

    /// The proof rows of `namespace` from `first` on, which waited since
    /// `since`, are in no object that may still be stored, so the next one
    /// carries them.
    fn reseal_proofs(&self, namespace: IggyNamespace, first: u64, since: u64) {
        if let Some(cursor) = self.cursors.borrow_mut().get_mut(&namespace) {
            cursor.next_to_seal = cursor.next_to_seal.min(first);
        }
        self.proofs_waiting
            .borrow_mut()
            .entry(namespace)
            .and_modify(|waiting| *waiting = (*waiting).min(since))
            .or_insert(since);
    }

    /// When the proof rows that wait are due in an object of their own, if
    /// no window is sealed first, on the bus's monotonic clock.
    fn confirmation_due(&self) -> Option<u64> {
        let confirm_after = self.confirm_after_micros.get();
        let oldest = self.proofs_waiting.borrow().values().copied().min()?;
        self.uploader.borrow().as_ref()?;
        let due = oldest.saturating_add(confirm_after);
        Some(
            self.last_confirmation_at
                .get()
                .map_or(due, |last| due.max(last.saturating_add(confirm_after))),
        )
    }

    /// The oldest proof row this shard has not stored yet began to wait at,
    /// on the bus's monotonic clock.
    fn oldest_unstored_proof(&self) -> Option<u64> {
        let closed = self.closed.borrow();
        let sealed = closed.values().filter_map(|object| object.proofs_since);
        self.proofs_waiting
            .borrow()
            .values()
            .copied()
            .chain(sealed)
            .min()
    }

    /// When the pump must look at the uploads again without an event: the
    /// open window comes due, an object is tried again, an attempt runs out
    /// of time, a delayed send is due, or proof rows waited long enough. On
    /// the bus's monotonic clock.
    fn next_deadline(&self) -> Option<u64> {
        let window_due = self
            .window
            .borrow()
            .as_ref()
            .filter(|_| self.has_free_upload())
            .zip(self.last_upload_started_at.get())
            .map(|(window, started_at)| started_at.saturating_add(window.interval_micros));
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
        let delayed = self.delayed.borrow();
        let delayed = delayed
            .values()
            .filter_map(|sends| sends.front().map(|send| send.due_at));
        objects
            .chain(window_due)
            .chain(delayed)
            .chain(self.confirmation_due())
            .min()
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Note the proof rows `partition` has for this shard to seal, if any
    /// wait since it was last asked, and whether it lacks rows that a walk
    /// should rebuild.
    pub(crate) fn note_fresh_proofs(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        let fresh = &self.fresh_writes;
        let now = self.bus.monotonic_micros();
        if partition.take_proofs_wanted() {
            fresh
                .proofs_waiting
                .borrow_mut()
                .entry(namespace)
                .or_insert(now);
        }
        let retry_at = fresh.proof_walk_retry_at.borrow().get(&namespace).copied();
        if retry_at.is_none_or(|retry_at| retry_at <= now) && partition.needs_proof_walk() {
            fresh.proof_walks_wanted.borrow_mut().insert(namespace);
        }
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

    /// Set the `cluster_uid` of the store that this shard's fresh objects go
    /// to, which each of them names as its writer's.
    pub fn set_fresh_cluster_uid(&self, cluster_uid: u128) {
        self.fresh_writes.cluster_uid.set(cluster_uid);
    }

    /// See [`Self::set_fresh_cluster_uid`]. Zero until it is set.
    #[must_use]
    pub const fn fresh_cluster_uid(&self) -> u128 {
        self.fresh_writes.cluster_uid.get()
    }

    /// Set how long proof rows wait for a window to carry them before they
    /// go out in an object of their own, and the shortest time between two
    /// such objects.
    pub fn set_fresh_confirm_after(&self, wait: Duration) {
        self.fresh_writes.confirm_after_micros.set(micros(wait));
    }

    /// Set the shortest time between the starts of two uploads that no full
    /// object forces, for the sends of a topic that sets none, and how many
    /// uploads run at once. Fewer than one upload counts as one.
    pub fn set_fresh_upload_pace(&self, interval: Duration, uploads: usize) {
        self.fresh_writes.interval_micros.set(micros(interval));
        self.fresh_writes.uploads_max.set(uploads.max(1));
    }

    /// Set the most bytes of messages this shard holds for sends that have
    /// not reached their partitions, and the most of them that the sends of
    /// one partition hold.
    pub fn set_fresh_pending_bytes(&self, shard_bytes: u64, partition_bytes: u64) {
        self.fresh_writes.pending_bytes_max.set(shard_bytes);
        self.fresh_writes
            .partition_pending_bytes_max
            .set(partition_bytes);
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
    /// the send cannot be taken, and holds the send back a while when the
    /// rewrites of its partition fell behind.
    ///
    /// `upload_interval` is the `fresh_upload_interval` of the send's topic,
    /// `None` for one that sets none.
    pub(crate) fn offer_fresh_send(
        &self,
        request: Message<RoutedRequestHeader>,
        reply: SubmitReply,
        upload_interval: Option<Duration>,
    ) {
        let interval_micros =
            upload_interval.map_or_else(|| self.fresh_writes.interval_micros.get(), micros);
        self.admit_fresh_send(request, reply, true, interval_micros);
        self.drive_fresh_uploads();
    }

    /// [`Self::offer_fresh_send`] without starting an upload. `may_delay` is
    /// false for a send that was held back once already.
    fn admit_fresh_send(
        &self,
        request: Message<RoutedRequestHeader>,
        reply: SubmitReply,
        may_delay: bool,
        interval_micros: u64,
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
        // Held before the share of its partition and the cap of the shard are
        // checked, so its messages count in neither while it waits. Never
        // refused: a producer answered as not accepted sends all of them
        // again.
        if may_delay && let Some(due_at) = self.fresh_send_due_at(namespace) {
            self.hold_fresh_send(
                namespace,
                DelayedSend::new(request, reply, due_at, interval_micros),
            );
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
        // Checked here as the seal places it, so that no send in a window
        // keeps it from being sealed.
        let admitted_send = admitted_batch(&admitted).and_then(|batch| {
            let blob = admitted_blob(&admitted)?;
            Ok((batch, placement::blob_checksum(blob), blob.len() as u64))
        });
        let (batch, blob_checksum, bytes) = match admitted_send {
            Ok(admitted_send) => admitted_send,
            Err(error) => {
                deny(&error);
                return;
            }
        };
        // A send is never split, and one that does not fit starts the next
        // object.
        if fresh.window.borrow().as_ref().is_some_and(|window| {
            window.bytes + bytes > FRESH_OBJECT_BYTES || window.sends >= FRESH_OBJECT_BATCHES_MAX
        }) {
            self.close_fresh_window();
        }
        let mut slot = fresh.window.borrow_mut();
        if slot.is_none() {
            let Some(window) = self.open_fresh_window(interval_micros) else {
                deny(&IggyError::TransientNotAccepted);
                return;
            };
            *slot = Some(window);
        }
        let Some(window) = slot.as_mut() else {
            return;
        };
        window.interval_micros = window.interval_micros.min(interval_micros);
        window.bytes += bytes;
        window.sends += 1;
        window.partitions.insert(namespace);
        let full = window.bytes >= FRESH_OBJECT_BYTES || window.sends >= FRESH_OBJECT_BATCHES_MAX;
        fresh.stage_send(
            namespace,
            StagedSend {
                request: admitted,
                reply,
                object: window.sequence,
                object_id: window.id,
                bytes,
                batch,
                blob_checksum,
            },
        );
        drop(slot);
        if full {
            self.close_fresh_window();
        }
    }

    /// A window numbered next, for a first send whose upload interval is
    /// `interval_micros`. `None` when no id names it.
    fn open_fresh_window(&self, interval_micros: u64) -> Option<Window> {
        let fresh = &self.fresh_writes;
        let sequence = fresh.sequence.get();
        let id = FreshObjectId::new(
            self.partition_consensus.self_replica_id,
            self.id,
            fresh.started_at,
            sequence,
        )?;
        fresh.sequence.set(sequence + 1);
        Some(Window {
            id,
            sequence,
            bytes: 0,
            sends: 0,
            partitions: BTreeSet::new(),
            opened_at: self.bus.monotonic_micros(),
            interval_micros,
        })
    }

    /// Seal the open window into an object that waits for an upload slot.
    /// Returns its number, `None` without a window or when none of its sends
    /// is left to seal.
    fn close_fresh_window(&self) -> Option<u64> {
        let window = self.fresh_writes.window.borrow_mut().take()?;
        self.seal_fresh_object(
            window.sequence,
            window.id,
            &window.partitions,
            window.opened_at,
        )
        .then_some(window.sequence)
    }

    /// Seal the object `sequence`: the sends of `sending` that it took, and
    /// the proof rows that wait of the partitions this shard proves, up to
    /// what an object holds. Then build the request of each send at the
    /// position of its blob, and leave the object to wait for an upload
    /// slot. Returns whether there was anything to seal.
    fn seal_fresh_object(
        &self,
        sequence: u64,
        id: FreshObjectId,
        sending: &BTreeSet<IggyNamespace>,
        opened_at: u64,
    ) -> bool {
        let fresh = &self.fresh_writes;
        let now = self.bus.monotonic_micros();
        let candidates: BTreeSet<IggyNamespace> = sending
            .iter()
            .copied()
            .chain(fresh.proofs_waiting.borrow().keys().copied())
            .collect();
        let mut sealed = Vec::with_capacity(candidates.len());
        let mut proofs_since = None;
        let mut proofs_room = FRESH_OBJECT_PROOFS_MAX as usize;
        for namespace in candidates {
            let waited = fresh.proofs_waiting.borrow_mut().remove(&namespace);
            let since = waited.unwrap_or(now);
            let (partition, more_proofs) =
                self.seal_partition(sequence, namespace, sending, proofs_room);
            if more_proofs {
                fresh.proofs_waiting.borrow_mut().insert(namespace, since);
            }
            match partition {
                Some(partition) => {
                    if !partition.proofs.is_empty() {
                        proofs_room -= partition.proofs.len();
                        proofs_since =
                            Some(proofs_since.map_or(since, |earlier: u64| earlier.min(since)));
                    }
                    sealed.push(partition);
                }
                None if sending.contains(&namespace) => self.deny_sends_of(namespace, sequence),
                None => {}
            }
        }
        if sealed.is_empty() {
            return false;
        }
        let header = FreshObjectHeader {
            object_id: id,
            cluster_uid: fresh.cluster_uid.get(),
            sealed_at: self.bus.realtime_micros(),
        };
        let Some(bytes) = self.seal_and_place(sequence, header, &sealed) else {
            for partition in &sealed {
                if let Some(first) = partition.proofs.first() {
                    fresh.reseal_proofs(
                        partition.namespace,
                        first.base_offset,
                        proofs_since.unwrap_or(now),
                    );
                }
                self.deny_sends_of(partition.namespace, sequence);
            }
            return false;
        };
        let proofs = sealed
            .iter()
            .filter_map(|partition| {
                partition
                    .proofs
                    .first()
                    .map(|first| (partition.namespace, first.base_offset))
            })
            .collect();
        let partitions = sealed
            .iter()
            .map(|partition| partition.namespace)
            .filter(|namespace| sending.contains(namespace))
            .collect();
        fresh.closed.borrow_mut().insert(
            sequence,
            ClosedObject {
                id,
                bytes,
                partitions,
                proofs,
                proofs_since,
                opened_at,
                attempts: 0,
                first_started_at: 0,
                running: None,
                retry_at: None,
            },
        );
        true
    }

    /// What the object `sequence` holds of `namespace`: its sends, if it is
    /// in `sending`, and up to `proofs_room` of its proof rows from where
    /// the last object left them, if this replica proves its batches.
    /// Advances where its sealing stands as if the object were stored.
    /// `None` for a partition that is gone or that the object holds nothing
    /// of, with whether rows are left for the next object.
    fn seal_partition(
        &self,
        sequence: u64,
        namespace: IggyNamespace,
        sending: &BTreeSet<IggyNamespace>,
        proofs_room: usize,
    ) -> (Option<SealedPartition>, bool) {
        let fresh = &self.fresh_writes;
        let partitions = self.plane.partitions();
        let Some(partition) = partitions.get_by_ns(&namespace) else {
            return (None, false);
        };
        let mut cursors = fresh.cursors.borrow_mut();
        let cursor = cursors
            .entry(namespace)
            .or_insert_with(|| SealCursor::of(partition));
        if !cursor.is_of(partition) {
            *cursor = SealCursor::of(partition);
        }
        let unstored_from = fresh.unstored_from(namespace, cursor.next_to_seal);
        let proofs: Vec<FreshProofRow> = partition
            .fresh_proofs()
            .filter(|_| partition.proves_placed_batches())
            .map_or_else(Vec::new, |proofs| {
                proofs
                    .rows_from(cursor.next_to_seal)
                    .take(proofs_room)
                    .copied()
                    .collect()
            });
        let (earlier, sealed) = if sending.contains(&namespace) {
            self.staged_messages(namespace, sequence)
        } else {
            (0, 0)
        };
        let more_proofs = |next_to_seal: u64| {
            partition.proves_placed_batches()
                && partition
                    .fresh_proofs()
                    .is_some_and(|proofs| proofs.rows_from(next_to_seal).next().is_some())
        };
        if sealed == 0 && proofs.is_empty() {
            return (None, more_proofs(cursor.next_to_seal));
        }
        let claimed_first_offset = if sealed > 0 {
            let claim = cursor
                .next_claim
                .unwrap_or_else(|| partition.mint_frontier().saturating_add(earlier));
            cursor.next_claim = Some(claim.saturating_add(sealed));
            claim
        } else {
            0
        };
        if let Some(last) = proofs.last() {
            cursor.next_to_seal = last
                .base_offset
                .saturating_add(u64::from(last.message_count));
        }
        (
            Some(SealedPartition {
                namespace,
                created_revision: partition.created_revision(),
                view: partition.consensus().view(),
                claimed_first_offset,
                proven_below: partition.proven_below(unstored_from),
                proofs,
            }),
            more_proofs(cursor.next_to_seal),
        )
    }

    /// The messages of the waiting sends of `namespace`: those of other
    /// objects, and those of the object `sequence`.
    fn staged_messages(&self, namespace: IggyNamespace, sequence: u64) -> (u64, u64) {
        self.fresh_writes
            .staged
            .borrow()
            .get(&namespace)
            .into_iter()
            .flatten()
            .fold((0, 0), |(earlier, sealed), send| {
                let messages = u64::from(send.batch.message_count);
                if send.object == sequence {
                    (earlier, sealed + messages)
                } else {
                    (earlier + messages, sealed)
                }
            })
    }

    /// The bytes of the object `sequence` that holds `partitions`, after
    /// the request of each of its sends is built at the position of its
    /// blob. `None` when the object cannot be sealed, which leaves the sends
    /// as they were.
    fn seal_and_place(
        &self,
        sequence: u64,
        header: FreshObjectHeader,
        partitions: &[SealedPartition],
    ) -> Option<Bytes> {
        let fresh = &self.fresh_writes;
        let sealed = {
            let staged = fresh.staged.borrow();
            let parts: Vec<FreshPartition<&[u8]>> = partitions
                .iter()
                .map(|partition| FreshPartition {
                    namespace: partition.namespace.inner(),
                    created_revision: partition.created_revision,
                    view: partition.view,
                    claimed_first_offset: partition.claimed_first_offset,
                    proven_below: partition.proven_below,
                    batches: sends_of(&staged, partition.namespace, sequence)
                        .map(|send| FreshBatch {
                            message_count: send.batch.message_count,
                            origin_timestamp: send.batch.origin_timestamp,
                            blob_checksum: send.blob_checksum,
                            producer: send.batch.producer.map(|producer| FreshProducer {
                                producer_id: producer.producer_id(),
                                epoch: producer.epoch(),
                                base_sequence: producer.base_sequence(),
                            }),
                            blob: admitted_blob(&send.request).unwrap_or_default(),
                        })
                        .collect(),
                    proofs: partition.proofs.clone(),
                })
                .collect();
            SealedFreshObject::seal(header, &parts)
        };
        let sealed = match sealed {
            Ok(sealed) => sealed,
            Err(error) => {
                tracing::error!(
                    shard = self.id,
                    %error,
                    "a fresh object could not be sealed; its sends are answered as not accepted"
                );
                return None;
            }
        };
        let mut staged = fresh.staged.borrow_mut();
        let mut placed = Vec::with_capacity(sealed.index.batches().len());
        for row in sealed.index.directory() {
            let namespace = IggyNamespace::from_raw(row.namespace);
            let sends = sends_of(&staged, namespace, sequence);
            for ((position, batch), send) in sealed.index.batches_of(row).zip(sends) {
                let placement = Placement {
                    object_id: header.object_id.get(),
                    position,
                    length: u64::from(batch.blob_length),
                    blob_checksum: batch.blob_checksum,
                    proven_below: row.proven_below,
                };
                match place_request(&send.request, placement) {
                    Ok(request) => placed.push(request),
                    Err(error) => {
                        tracing::error!(
                            shard = self.id,
                            %error,
                            "a sealed send could not be placed; the sends of its object are \
                             answered as not accepted"
                        );
                        return None;
                    }
                }
            }
        }
        let mut placed = placed.into_iter();
        for partition in partitions {
            if let Some(queue) = staged.get_mut(&partition.namespace) {
                let first = queue.partition_point(|send| send.object < sequence);
                for send in queue.range_mut(first..) {
                    if let Some(request) = placed.next() {
                        send.request = request;
                    }
                }
            }
        }
        Some(sealed.bytes)
    }

    /// Answer the sends of `namespace` in the object `sequence`, the last
    /// ones of its queue, as not accepted.
    fn deny_sends_of(&self, namespace: IggyNamespace, sequence: u64) {
        let denied = self
            .fresh_writes
            .staged
            .borrow_mut()
            .get_mut(&namespace)
            .map(|queue| {
                let first = queue.partition_point(|send| send.object < sequence);
                queue.split_off(first)
            });
        for send in denied.into_iter().flatten() {
            self.deny_staged_send(namespace, send);
        }
    }

    /// When the shard takes a send to `namespace` that arrives now, if not
    /// at once: after the sends to it that are held already, so none is
    /// overtaken, and after the delay its rewrite backlog asks for.
    fn fresh_send_due_at(&self, namespace: IggyNamespace) -> Option<u64> {
        let now = self.bus.monotonic_micros();
        let behind = self
            .fresh_writes
            .delayed
            .borrow()
            .get(&namespace)
            .and_then(|sends| sends.back().map(|send| send.due_at));
        let backlog = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .and_then(IggyPartition::long_term_backlog)
            .unwrap_or(0);
        let delayed = backlog_delay(backlog, self.rewrite_pacing.borrow().backlog_limit())
            .map(|delay| now.saturating_add(micros(delay)));
        behind.max(delayed)
    }

    fn hold_fresh_send(&self, namespace: IggyNamespace, send: DelayedSend) {
        self.fresh_writes
            .delayed
            .borrow_mut()
            .entry(namespace)
            .or_default()
            .push_back(send);
        self.metrics.record_long_term_delayed_send();
    }

    /// Take the held sends whose delay is over, in the order they came to
    /// each partition.
    fn release_delayed_fresh_sends(&self, now: u64) {
        let released = {
            let mut delayed = self.fresh_writes.delayed.borrow_mut();
            let mut released = Vec::new();
            delayed.retain(|_, sends| {
                while sends.front().is_some_and(|send| send.due_at <= now) {
                    released.extend(sends.pop_front());
                }
                !sends.is_empty()
            });
            released
        };
        for DelayedSend {
            request,
            reply,
            interval_micros,
            ..
        } in released
        {
            self.admit_fresh_send(request, reply, false, interval_micros);
        }
    }

    /// Whether a send of `size` bytes to `namespace` would take the waiting
    /// sends of its partition past their share, or those of the shard past
    /// the cap, as [`Self::set_fresh_pending_bytes`] set them. Counts the
    /// bound it passes. A send larger than a bound still gets in while
    /// nothing that counts against it waits.
    fn fresh_send_over_pending_bytes(&self, namespace: IggyNamespace, size: u64) -> bool {
        let fresh = &self.fresh_writes;
        let partition_pending = fresh.partition_pending(namespace);
        if partition_pending > 0
            && partition_pending.saturating_add(size) > fresh.partition_pending_bytes_max.get()
        {
            self.metrics.record_fresh_send_over_partition_share();
            return true;
        }
        let pending = fresh.pending_bytes.get();
        if pending > 0 && pending.saturating_add(size) > fresh.pending_bytes_max.get() {
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
    /// whose attempt ran out of time. Takes the held sends that are due
    /// first. The pump calls it after a send, when an upload ends, at
    /// [`Self::fresh_uploads_deadline`] and on its tick.
    pub(crate) fn drive_fresh_uploads(&self) {
        let fresh = &self.fresh_writes;
        let now = self.bus.monotonic_micros();
        self.release_delayed_fresh_sends(now);
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
        // Even beside an open window, which keeps to its interval.
        if fresh.confirmation_due().is_some_and(|due| due <= now) {
            self.seal_fresh_confirmation(now);
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
            && fresh.window_due(now)
            && let Some(sequence) = self.close_fresh_window()
        {
            self.start_fresh_upload(sequence, now);
        }
        self.metrics
            .set_fresh_load(fresh.uploads_in_flight.get(), fresh.staged_sends.get());
        self.metrics.set_fresh_proof_lag(Duration::from_micros(
            fresh
                .oldest_unstored_proof()
                .map_or(0, |since| now.saturating_sub(since)),
        ));
    }

    /// Seal the proof rows that wait into an object of their own, which
    /// waits for an upload slot as any other.
    fn seal_fresh_confirmation(&self, now: u64) {
        let fresh = &self.fresh_writes;
        let sequence = fresh.sequence.get();
        let Some(id) = FreshObjectId::new(
            self.partition_consensus.self_replica_id,
            self.id,
            fresh.started_at,
            sequence,
        ) else {
            return;
        };
        if self.seal_fresh_object(sequence, id, &BTreeSet::new(), now) {
            fresh.sequence.set(sequence + 1);
            fresh.last_confirmation_at.set(Some(now));
        }
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
        self.settle_proof_walks();
        self.start_proof_walks();
        self.drive_fresh_uploads();
    }

    /// Hand the partitions the rows their walks rebuilt. One whose rows are
    /// still missing walks again after [`PROOF_WALK_RETRY`].
    fn settle_proof_walks(&self) {
        let fresh = &self.fresh_writes;
        let finished = std::mem::take(&mut *fresh.proof_walks.borrow_mut());
        let now = self.bus.monotonic_micros();
        for FinishedProofWalk {
            namespace,
            created_revision,
            walk,
            rebuilt,
        } in finished
        {
            let partitions = self.plane.partitions();
            let Some(partition) = partitions
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
            else {
                fresh.proof_walk_retry_at.borrow_mut().remove(&namespace);
                continue;
            };
            let mut retry_at = fresh.proof_walk_retry_at.borrow_mut();
            if partition.finish_proof_walk(walk, rebuilt) {
                retry_at.remove(&namespace);
            } else {
                retry_at.insert(namespace, now.saturating_add(micros(PROOF_WALK_RETRY)));
            }
        }
    }

    /// Start the walks the tick found wanted. Each reads placed records off
    /// the pump.
    fn start_proof_walks(&self) {
        let fresh = &self.fresh_writes;
        let wanted = std::mem::take(&mut *fresh.proof_walks_wanted.borrow_mut());
        for namespace in wanted {
            let partitions = self.plane.partitions();
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            let created_revision = partition.created_revision();
            let Some((walk, read)) = partition.begin_proof_walk() else {
                continue;
            };
            let finished = Rc::clone(&fresh.proof_walks);
            self.bus.spawn(async move {
                let rebuilt = read.run().await;
                finished.borrow_mut().push(FinishedProofWalk {
                    namespace,
                    created_revision,
                    walk,
                    rebuilt,
                });
            });
        }
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
        if stored && object.partitions.is_empty() {
            self.metrics.record_fresh_confirmation();
        }
        // A consumer at the tail polls these batches next, and this shard
        // holds their messages already.
        if stored
            && !object.partitions.is_empty()
            && let Some(remote) = self.plane.partitions().remote_segments()
        {
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
        let since = object
            .proofs_since
            .unwrap_or_else(|| self.bus.monotonic_micros());
        for (namespace, first) in &object.proofs {
            self.fresh_writes.reseal_proofs(*namespace, *first, since);
        }
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
        self.fresh_writes.forget_claim(namespace);
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

/// The sends of `namespace` in the object `sequence`, the last ones of its
/// queue.
fn sends_of(
    staged: &BTreeMap<IggyNamespace, VecDeque<StagedSend>>,
    namespace: IggyNamespace,
    sequence: u64,
) -> impl Iterator<Item = &StagedSend> {
    staged.get(&namespace).into_iter().flat_map(move |queue| {
        let first = queue.partition_point(|send| send.object < sequence);
        queue.range(first..)
    })
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
