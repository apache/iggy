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
//! The window closes once it is old or large enough, and the shard stores
//! its object. Only then do the kept requests reach their partitions, in the
//! order the sends came, and each producer is answered when its own request
//! commits. A send whose object the store refused is answered as not
//! accepted, so the producer sends it again.
//!
//! One upload runs at a time. The sends that arrive meanwhile fill the next
//! window, which is what keeps the requests of a partition in order and
//! makes the windows grow with the load.
//!
//! The partition checks a request only after its object is stored. A send
//! it then refuses, such as a duplicate or one that arrived after primacy
//! moved, leaves its messages in the object with nothing that names them.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, RestorableMetadataStm, Sender, ShardFrame};
use bytes::Bytes;
use consensus::PartitionsHandle;
use iggy_binary_protocol::{GenericHeader, Operation, PrepareHeader, RoutedRequestHeader};
use iggy_common::{IggyError, IggyTimestamp};
use iggy_object_store::{FreshObject, FreshObjectId, TierScope};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::{FreshObjectUploader, FreshUploadOutcome};
use server_common::Message;
use server_common::send_messages::{
    ChecksumMode, admitted_blob, convert_request_message, place_request,
};
use server_common::sharding::IggyNamespace;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

/// How long a window stays open after its first send.
pub const FRESH_WINDOW: Duration = Duration::from_millis(50);

/// Bytes of messages at which a window closes without waiting out
/// [`FRESH_WINDOW`].
pub const FRESH_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes of messages the open window may hold while an upload runs. A send
/// past it is answered as not accepted, so a store that stopped answering
/// costs the shard this much memory and no more.
pub const FRESH_PENDING_BYTES_MAX: u64 = 8 * FRESH_OBJECT_BYTES;

/// The submit channel a send arrived with.
type SubmitReply = Sender<Option<Message<GenericHeader>>>;

/// A send whose messages are in a fresh object: the request that commits
/// where they sit, and where to answer its producer.
pub struct PlacedSend {
    request: Message<RoutedRequestHeader>,
    reply: SubmitReply,
}

/// What the task uploading a fresh object hands back to the pump.
pub struct FinishedFreshUpload {
    pub object_id: FreshObjectId,
    /// The object's bytes, which the shard keeps for its polls once the
    /// store holds them.
    pub bytes: Bytes,
    pub sends: Vec<PlacedSend>,
    pub outcome: FreshUploadOutcome,
}

/// The sends that wait for one fresh object.
struct Window {
    id: FreshObjectId,
    object: FreshObject,
    sends: Vec<PlacedSend>,
    /// The bus's monotonic clock when the first send came.
    opened_at: u64,
}

/// What a shard keeps for the upload-first produce path.
pub struct FreshWrites {
    uploader: RefCell<Option<Rc<dyn FreshObjectUploader>>>,
    window: RefCell<Option<Window>>,
    /// Whether an upload is in flight.
    uploading: Cell<bool>,
    /// The next object's number.
    sequence: Cell<u64>,
    /// When this shard started, which keeps its ids apart from those of its
    /// earlier life.
    started_at: u64,
    /// Never cancelled. An upload belongs to the sends of many partitions,
    /// and each of them is checked again once the object is stored.
    scope: TierScope,
    /// Results of uploads whose frame the inbox refused, for the next tick.
    undelivered: Rc<RefCell<Vec<FinishedFreshUpload>>>,
}

impl Default for FreshWrites {
    fn default() -> Self {
        Self {
            uploader: RefCell::new(None),
            window: RefCell::new(None),
            uploading: Cell::new(false),
            sequence: Cell::new(0),
            started_at: IggyTimestamp::now().as_micros(),
            scope: TierScope::new(),
            undelivered: Rc::default(),
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
            Self::answer_partition_submit_deny(&header, Some(reply.clone()), error);
        };
        if self.fresh_writes.uploader.borrow().is_none() {
            // The node does not pass the store identity check, which it may
            // pass again.
            deny(&IggyError::TransientNotAccepted);
            return;
        }
        let namespace = IggyNamespace::from_raw(header.group);
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
        let mut slot = self.fresh_writes.window.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|window| window.object.len() + blob.len() as u64 > FRESH_PENDING_BYTES_MAX)
        {
            deny(&IggyError::TransientNotAccepted);
            return;
        }
        if slot.is_none() {
            let sequence = self.fresh_writes.sequence.get();
            let Some(id) = FreshObjectId::new(
                self.partition_consensus.self_replica_id,
                self.id,
                self.fresh_writes.started_at,
                sequence,
            ) else {
                deny(&IggyError::TransientNotAccepted);
                return;
            };
            self.fresh_writes.sequence.set(sequence + 1);
            *slot = Some(Window {
                id,
                object: FreshObject::new(),
                sends: Vec::new(),
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
        window.sends.push(PlacedSend {
            request: placed,
            reply,
        });
        drop(slot);
        self.upload_fresh_window_if_due();
    }

    /// Start the upload of the open window when it is old or large enough
    /// and no upload runs. The pump calls it on its tick, after a send, and
    /// when an upload ends.
    pub(crate) fn upload_fresh_window_if_due(&self) {
        if self.fresh_writes.uploading.get() {
            return;
        }
        let Some(uploader) = self.fresh_writes.uploader.borrow().clone() else {
            return;
        };
        let window_micros = u64::try_from(FRESH_WINDOW.as_micros()).unwrap_or(u64::MAX);
        let now = self.bus.monotonic_micros();
        let due = self
            .fresh_writes
            .window
            .borrow()
            .as_ref()
            .is_some_and(|window| {
                window.object.len() >= FRESH_OBJECT_BYTES
                    || now.saturating_sub(window.opened_at) >= window_micros
            });
        if !due {
            return;
        }
        // Looked up first: an upload that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let Some(window) = self.fresh_writes.window.borrow_mut().take() else {
            return;
        };
        self.fresh_writes.uploading.set(true);
        let scope = self.fresh_writes.scope.token();
        let undelivered = Rc::clone(&self.fresh_writes.undelivered);
        let Window {
            id, object, sends, ..
        } = window;
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let bytes = object.into_bytes();
            let finished = Box::new(FinishedFreshUpload {
                object_id: id,
                bytes: bytes.clone(),
                sends,
                outcome: uploader.upload(id, bytes, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::FreshUploadFinished(finished));
            // The producers of the window wait for this result, so a refused
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
    /// and close the window when it is due.
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
        self.upload_fresh_window_if_due();
    }

    /// Hand the sends of a stored object to their partitions, in the order
    /// they came, or answer every producer of an object the store refused.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_fresh_upload_finished(&self, finished: FinishedFreshUpload)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        self.fresh_writes.uploading.set(false);
        let stored = match &finished.outcome {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::warn!(
                    shard = self.id,
                    sends = finished.sends.len(),
                    %error,
                    "storing a fresh object failed; its sends are answered as not accepted"
                );
                false
            }
            Err(_) => false,
        };
        // A consumer at the tail polls these batches next, and this shard
        // holds their messages already.
        if stored && let Some(remote) = self.plane.partitions().remote_segments() {
            remote.cache_fresh_object(finished.object_id.get(), finished.bytes);
        }
        for PlacedSend { request, reply } in finished.sends {
            if !stored {
                Self::answer_partition_submit_deny(
                    request.header(),
                    Some(reply),
                    &IggyError::TransientNotAccepted,
                );
                continue;
            }
            let (sender, receiver) = consensus::oneshot_channel();
            self.plane
                .partitions()
                .on_placed_request_with_reply(request, Some(sender))
                .await;
            // Awaited off the pump, as for any other submit: the commit that
            // fires the receiver needs the pump to keep draining acks.
            self.bus.spawn(async move {
                let committed = receiver.await.ok().map(Message::into_generic);
                let _ = reply.try_send(committed);
            });
        }
        self.upload_fresh_window_if_due();
    }
}
