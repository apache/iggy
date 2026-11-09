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

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::{pending, poll_fn};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use async_broadcast::Receiver;
use bytes::Bytes;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    AssignmentVersion, AutoCommit, Checkpoint, CommitAfter, CommitPolicy, Consumer,
    ConsumerGroupClientState, ConsumerKind, DiagnosticEvent, EncryptorKind, Identifier, IggyError,
    MessageClient, NonZeroIggyDuration, PollOptions, PolledMessages, PollingKind, PollingStrategy,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use crate::client_wrappers::client_wrapper::ClientWrapper;
use membership::Membership;
use progress::{Progress, in_background, in_close};
use worker::Worker;
use worker::{lasting_refusal, probe_options};

#[cfg(test)]
mod cluster_tests;
#[cfg(test)]
pub(crate) mod fixtures;
pub(crate) mod membership;
pub(crate) mod progress;
mod worker;

const ORDERING: Ordering = Ordering::SeqCst;
/// The time from the close start in which a refused write of the close reads for the installed
/// owner. So a refusal that stays transient holds the rest of the close at most this long.
const CLOSE_BUDGET: Duration = Duration::from_secs(10);

/// Reads the partitions of a topic in batches, with routing, prefetch and the positions of each
/// partition inside. Build one with [`IggyClient::reader`](crate::prelude::IggyClient::reader).
///
/// - A standalone consumer reads the partitions of its builder. A consumer group reads the
///   assignment of its member, and the reader joins the group, joins again after a session loss
///   and leaves it in [`close`](Self::close).
/// - [`next`](Self::next) returns the next batch of one partition, in offset order within one
///   generation of the partition: from its first read, from each [`seek`](Self::seek), and from
///   each new assignment to this member.
/// - The [`CommitPolicy`] of the options decides who stores offsets. With `Manual`,
///   [`commit`](Self::commit) stores the offset of a checkpoint. With `OnRead`, the server stores
///   the offset of each reply, the reader reads only while [`next`](Self::next) waits and then
///   fills its budget (one fetch per primary that may wait for messages, the partitions with
///   messages left behind spread over the other slots), the start of the builder applies only
///   to a partition without a stored offset, a [`seek`](Self::seek) to an offset above 0 stores
///   the offset before it, a seek to offset 0 deletes the stored offset, and
///   [`close`](Self::close) moves the offset back to the last delivered message. A partition
///   that leaves the assignment still hands over the batches that the server stored for it.
///   `close` does not move the offset back over these batches, also when the partition comes
///   back. So the ones that the application did not take before `close` are lost for the group.
///   A crash, or a reader dropped without `close`, loses the batches that the server stored but
///   the application did not take: up to the effective budget, which holds at least 32 batches.
///   `Auto` and `Manual` store offsets after delivery instead. With `Auto`,
///   the reader counts each batch as its [`CommitAfter`] says, or each [`mark`](Self::mark), and
///   stores the latest counted position at the interval or the event of the policy, before a
///   rebalance takes a partition away, and in [`close`](Self::close).
/// - The reader stays usable after a [`ReadError`] and retries by itself. `HistoryUnavailable`
///   stops its partition until a `seek` when the position came from an earlier read of a history
///   that is gone. A refusal that a quick retry does not change, such as `Unauthorized`,
///   `PartitionNotFound`, or `ConsumerGroupPartitionNotOwned` again after the refresh that each
///   one starts, is retried after a wait that doubles each time. Six in a row give one error.
///   They also stop the partition until a `seek`, except `ConsumerGroupPartitionNotOwned`: the
///   partition is no leave, so it reads on, and the count starts again.
/// - With `OnRead`, the reader reads each partition from its stored offset after a delivery, so
///   the server refuses the commit of an older read once the offset moved. A read whose reply is
///   lost can still have stored its offset. The reader then abandons its operation, and while it
///   holds that lost read, a next read that starts past the last delivered message makes it read
///   the skipped messages again by offset without a commit and hand them over first. Without a
///   lost read, such a read goes on from where it starts, because the application or another
///   session stored that offset. Messages that a lost read committed are lost only when a
///   rebalance moves their partition to another member before that.
/// - Dropping the reader stops its tasks without waiting, and it moves no offset back.
pub struct PartitionReader {
    config: Arc<Config>,
    shared: Arc<Shared>,
    receiver: mpsc::UnboundedReceiver<Reply>,
    /// The batches of the received replies, one partition each, in arrival order.
    queue: VecDeque<Entry>,
    task: Option<JoinHandle<()>>,
    /// With `CommitAfter::Processing`: the last batch that `next()` returned, which counts once
    /// the application asks for the next one or closes the reader.
    uncounted: Option<(Checkpoint, u32)>,
    /// Stores the counted positions under `CommitPolicy::Auto`.
    committer: Option<Committer>,
}

/// The store task of a reader under `CommitPolicy::Auto`. It sends one store at a time, so the
/// stores of one partition go out in order.
struct Committer {
    stop: Arc<Notify>,
    task: JoinHandle<()>,
}

/// An error of a read. The reader stays usable after it and retries by itself.
///
/// Later releases can add fields. Code outside this crate builds one with [`ReadError::new`]
/// and sets the other fields on it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ReadError {
    /// The partition of the error. `None` for an error of the whole reader.
    pub partition_id: Option<u32>,
    pub error: IggyError,
    /// For a message that does not decode: the continuation after it. The reader reads the
    /// message again until a [`seek`](PartitionReader::seek) to this continuation skips it. The
    /// messages before it arrive in a batch before this error.
    pub skip: Option<PollingStrategy>,
    /// For a message that does not decode: its offset, when its frame decoded and a later step
    /// failed. `None` when the framing is malformed, so no record can be isolated.
    pub offset: Option<u64>,
    /// For a message that does not decode: the record as the reply carried it, its frame
    /// header, payload and user headers before any decryption. For malformed framing, the rest
    /// of the records from where decoding stopped. For an HTTP reply that is not JSON, the
    /// response body. `None` for a message that does not decrypt over HTTP, which carries no
    /// records.
    pub record: Option<Bytes>,
    /// For messages that a lost read committed and that the reader could not read again: their
    /// first and last offset. `None` for every other error.
    pub lost: Option<RangeInclusive<u64>>,
}

/// The fixed configuration of a reader.
pub(crate) struct Config {
    pub(super) client: IggyRwLock<ClientWrapper>,
    pub(super) stream: Arc<Identifier>,
    pub(super) topic: Arc<Identifier>,
    pub(super) consumer: Arc<Consumer>,
    /// The partitions of a standalone consumer. Empty for a group.
    pub(super) partitions: Vec<u32>,
    /// The start of a partition without a position. `First` is `Offset(0)`, or `Next` with
    /// `OnRead`. With `OnRead`, a start other than `Next` applies only to a partition without a
    /// stored offset.
    pub(super) start: PollingStrategy,
    pub(super) options: PollOptions,
    /// The replies that the budget holds: reads in flight, queued replies and errors.
    pub(super) slots: usize,
    pub(super) encryptor: Option<Arc<EncryptorKind>>,
    pub(super) retry_interval: Duration,
}

/// The state that the fetch loop shares with the reader handle.
pub(crate) struct Shared {
    pub(super) progress: Arc<Progress>,
    /// A change of the connection moves it. Without `OnRead`, a reply of an older epoch is stale.
    pub(super) epoch: AtomicU64,
    /// With `OnRead`: a `next()` waits, and the queue and the channel are empty.
    pub(super) demand: AtomicBool,
    pub(super) closing: AtomicBool,
    pub(super) can_poll: AtomicBool,
    /// The partitions that the reader reads, for `partitions()` and `seek`.
    pub(super) partitions: Mutex<Vec<u32>>,
    /// Shared with `IggyConsumer`, which creates a missing group.
    pub(super) membership: Option<Arc<Membership>>,
    /// The group state and the key of a group reader.
    pub(super) assignment_state: Option<(Arc<ConsumerGroupClientState>, String)>,
}

/// One slot of the budget, which covers the whole possible reply. Keep it until the batch is
/// handed over: message slices can share its backing allocation.
struct FetchCapacity {
    permit: Option<OwnedSemaphorePermit>,
    wake: Arc<Notify>,
}

/// One item of the channel from the fetch loop: the batches of one reply, each of one
/// partition, in reply order, or an error.
struct Reply {
    epoch: u64,
    assignment: Option<AssignmentVersion>,
    result: Result<Vec<PolledMessages>, Refusal>,
    capacity: Arc<FetchCapacity>,
}

/// An error with the generation of its partition when the reader reported it. An error of a
/// partition counts until a new generation, so the stop of a partition is never lost. An error
/// of the whole reader counts within its epoch.
struct Refusal {
    error: ReadError,
    generation: Option<u64>,
}

struct Entry {
    epoch: u64,
    assignment: Option<AssignmentVersion>,
    result: Result<PolledMessages, Refusal>,
    /// The slot returns when the last batch of its reply leaves the reader.
    capacity: Arc<FetchCapacity>,
}

/// Keeps a batch in the budget of its reader until the last message is handed over.
pub(crate) struct Hold {
    _capacity: Arc<FetchCapacity>,
    shared: Arc<Shared>,
    on_read: bool,
    epoch: u64,
    assignment: Option<AssignmentVersion>,
    partition: u32,
    generation: u64,
    /// The generation changes of the reader when the generation was last found current.
    generation_checked: Option<u64>,
    /// The assignment changes of the group state when the assignment was last found current.
    checked: Option<u64>,
}

impl PartitionReader {
    pub(super) fn start(
        config: Arc<Config>,
        shared: Arc<Shared>,
        events: Option<Receiver<DiagnosticEvent>>,
    ) -> Self {
        let (receiver, task) = spawn_worker(&config, &shared, events);
        let committer = config
            .auto()
            .map(|auto| Committer::spawn(Arc::clone(&shared.progress), auto.interval));
        Self {
            config,
            shared,
            receiver,
            queue: VecDeque::new(),
            task: Some(task),
            uncounted: None,
            committer,
        }
    }

    /// The next batch of one partition. Never empty, and it has no time limit. Cancel safe: a
    /// dropped call leaves its batch in the reader.
    // The design fixes this public signature. An error is rare next to the batches, so its
    // size does not matter.
    #[allow(clippy::result_large_err)]
    pub async fn next(&mut self) -> Result<PolledMessages, ReadError> {
        self.count_uncounted();
        let (batch, hold) = poll_fn(|cx| self.poll_held(cx)).await?;
        if let Some(checkpoint) = batch.checkpoint() {
            self.delivered(&checkpoint);
            let messages = u32::try_from(batch.messages.len()).unwrap_or(u32::MAX);
            match self.config.auto().map(|auto| auto.after) {
                Some(CommitAfter::Delivery) => self.count(checkpoint, messages, true),
                Some(CommitAfter::Processing) => self.uncounted = Some((checkpoint, messages)),
                Some(CommitAfter::Marked) | None => {}
            }
        }
        drop(hold);
        Ok(batch)
    }

    /// The next batch with its budget slot, which returns when the hold drops. It records no
    /// delivery: the caller reports each message that it hands over with
    /// [`delivered`](Self::delivered).
    pub(crate) fn poll_held(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(PolledMessages, Hold), ReadError>> {
        loop {
            if let Some(entry) = self.queue.pop_front() {
                if let Some(ready) = self.hand_over(entry) {
                    return Poll::Ready(ready);
                }
                continue;
            }
            match self.receiver.poll_recv(cx) {
                Poll::Pending => {
                    if self.config.on_read() && !self.shared.demand.swap(true, ORDERING) {
                        self.shared.progress.wake.notify_one();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    let (receiver, task) = spawn_worker(&self.config, &self.shared, None);
                    self.receiver = receiver;
                    self.task = Some(task);
                    return Poll::Ready(Err(ReadError::new(None, IggyError::ClientShutdown)));
                }
                Poll::Ready(Some(reply)) => self.split(reply),
            }
        }
    }

    /// Records the last message that the application took from its partition. With `OnRead`,
    /// [`close`](Self::close) moves the offset back to it.
    pub(crate) fn delivered(&self, checkpoint: &Checkpoint) {
        self.shared.progress.deliver(checkpoint);
    }

    /// Counts `messages` up to `checkpoint` under `CommitPolicy::Auto`. `ends_batch` marks the
    /// last message of a batch.
    pub(crate) fn count(&self, checkpoint: Checkpoint, messages: u32, ends_batch: bool) {
        self.shared.progress.count(checkpoint, messages, ends_batch);
    }

    /// Counts a checkpoint that this reader delivered, under `CommitPolicy::Auto`. It stores
    /// nothing now: the policy stores the latest counted position of the partition. With
    /// `CommitAfter::Marked` only marks count, so batches processed concurrently count once they
    /// are done.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] without `CommitPolicy::Auto`, and for a checkpoint that
    /// this reader did not deliver.
    pub fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.shared.progress.mark(checkpoint)
    }

    /// Stores the offset of a checkpoint that this reader delivered. The commits of one
    /// partition go out in order when the caller awaits each one.
    ///
    /// A checkpoint of an older generation of its partition, or at or below the last commit of
    /// its generation, stores nothing.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] with `CommitPolicy::OnRead`, and for a checkpoint that this
    /// reader did not deliver. The errors of the offset store. After
    /// [`IggyError::OperationUncertain`] the reader abandons the store on the client, so a later
    /// commit of the partition goes out.
    pub async fn commit(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        if self.config.on_read() {
            return Err(IggyError::InvalidCommand);
        }
        self.shared.progress.commit(checkpoint).await
    }

    /// Sets the next read position of `partition` and starts a new generation of it. It drops
    /// the queued batches of the partition, ignores its late replies and resumes a stopped
    /// partition.
    ///
    /// With `OnRead`, a seek to an offset above 0 first stores the offset before it, under the
    /// history of a continuation or else of the last delivered message, so a restart resumes at
    /// the new position. A seek to offset 0 or `First` deletes the stored offset under the same
    /// history when the server holds one, because the commit of a read never moves the offset
    /// back. A seek to a timestamp first reads one message at the timestamp without a commit,
    /// and then acts as a seek to the offset of that message. Without such a message it stores
    /// nothing. A failed read, store, lookup or delete fails the seek and nothing moves. Every
    /// other seek stores nothing and waits only for the seek of the partition in progress. A
    /// seek to an offset or a timestamp holds the reads of the partition until it returns. It
    /// waits for the seek of the partition in progress and for the committing read in flight.
    /// Except over HTTP, it cancels that read, which the server stops if nothing of it committed
    /// yet. The new generation drops the reply of that read, and no read of the partition starts
    /// until the new position takes effect or the seek fails. So no read from the old position
    /// commits after the store or the delete, on any transport. The first read from the new
    /// position commits as usual.
    ///
    /// A seek dropped before its store or delete changes nothing. Once that write starts, the
    /// seek finishes in the background: it keeps the hold, and it moves on success. A later
    /// seek of the partition and [`close`](Self::close) wait for it. A write with an unknown
    /// outcome fails the seek, which keeps the old position. HTTP reports such a write as
    /// [`IggyError::TransientNotCommitted`]. The reader then abandons the operation of the
    /// write, if the client retains one, and reads again what the write may have skipped. A
    /// partition without a delivered message, or whose position names no offset, such as a
    /// start before the first read, makes the seek look the stored offset up first. After an
    /// unknown outcome, a position that names no offset reads on after that offset. A close
    /// before any batch of the partition arrived puts that offset back as it was, or deletes the
    /// offset when the server held none. A group member skips this for a partition that left it
    /// since the write.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidCommand`] for a partition that the reader does not read, and for a
    ///   continuation of another partition. A group reader reads no partition until the first
    ///   refresh of its assignment after `build()`, so it refuses every partition before it.
    /// - [`IggyError::PartitionLaneBlocked`] with `OnRead` while another call on the client left
    ///   a committing read of the partition with an unknown outcome. Call `abandon_operation`
    ///   with its operation first.
    /// - The errors of the offset store, and of the read at a timestamp, with `OnRead`.
    pub async fn seek(&mut self, partition: u32, start: PollingStrategy) -> Result<(), IggyError> {
        start.validate_partition(partition)?;
        if !self.shared.partitions().contains(&partition) {
            return Err(IggyError::InvalidCommand);
        }
        self.move_to(partition, first_as_offset(start.without_zero_context()))
            .await?;
        self.drop_replaced();
        Ok(())
    }

    /// The writes and the move of [`Self::seek`].
    async fn move_to(&self, partition: u32, start: PollingStrategy) -> Result<(), IggyError> {
        let progress = Arc::clone(&self.shared.progress);
        if !self.config.on_read() {
            progress.seek(partition, start);
            self.shared.progress.wake.notify_one();
            return Ok(());
        }
        let writes = matches!(start.kind, PollingKind::Offset | PollingKind::Timestamp);
        let mut hold = if writes {
            progress.hold(partition).await?
        } else {
            progress.take_turn(partition).await?
        };
        // Checked after the hold, because the read that it waited for can leave an unknown
        // outcome.
        progress.unblock_abandoned().await;
        if let Some(operation) = progress.blocked(partition) {
            return Err(IggyError::PartitionLaneBlocked(partition, operation));
        }
        if !writes {
            progress.seek(partition, start);
            return Ok(());
        }
        let start = self.timestamp_as_offset(partition, start).await?;
        if start.kind != PollingKind::Offset {
            progress.seek(partition, start);
            return Ok(());
        }
        // A seek to offset 0 deletes the stored offset, and a seek above 0 stores the offset
        // before it. Both carry the history of a continuation, else that of the last message
        // delivered in the current generation of the partition, else none.
        let context = start.context().or_else(|| {
            progress
                .last_delivered(partition)
                .filter(|delivered| progress.is_current(delivered))
                .map(|delivered| delivered.context())
        });
        let Some(before) = start.value.checked_sub(1) else {
            let stored = progress.stored_offset(partition).await?;
            hold.keep_before(stored);
            return hold
                .finish(async move {
                    progress.delete_stored(partition, stored, context).await?;
                    progress.seek(partition, start);
                    Ok(())
                })
                .await;
        };
        hold.look_up_before().await?;
        let stored = Checkpoint::new(partition, before, context.unwrap_or_default());
        hold.finish(async move {
            match context {
                Some(_) => progress.store(stored).await?,
                None => progress.store_offset(partition, before).await?,
            }
            progress.resume_after(stored, start);
            Ok(())
        })
        .await
    }

    /// Drops the queued batches and errors that a seek replaced, so their slots return at once
    /// and the close does not rewind before them. Those of a newer generation stay.
    fn drop_replaced(&mut self) {
        while let Ok(reply) = self.receiver.try_recv() {
            self.split(reply);
        }
        let progress = &self.shared.progress;
        let on_read = self.config.on_read();
        self.queue.retain(|entry| {
            entry
                .generation()
                .is_none_or(|(of, generation)| progress.keeps(of, generation, on_read))
        });
    }

    /// With `OnRead`, a seek to a timestamp is a seek to the offset of the first message at the
    /// timestamp, which a read without a commit finds. A stored offset ahead would otherwise make
    /// the reads after the first one jump. Without such a message every later message lies after
    /// the stored offset, so the seek keeps the timestamp.
    async fn timestamp_as_offset(
        &self,
        partition: u32,
        start: PollingStrategy,
    ) -> Result<PollingStrategy, IggyError> {
        if start.kind != PollingKind::Timestamp {
            return Ok(start);
        }
        let config = &self.config;
        let probed = config
            .client
            .read()
            .await
            .poll_messages(
                &config.stream,
                &config.topic,
                partition,
                &config.consumer,
                &start,
                PollOptions {
                    commit: CommitPolicy::Manual,
                    ..probe_options(config.options)
                },
            )
            .await?;
        Ok(probed
            .messages
            .first()
            .map_or(start, |first| PollingStrategy::offset(first.header.offset)))
    }

    /// The partitions that the reader reads: those of a standalone consumer, or the current
    /// assignment of a group member. A group reader holds no partition until the first refresh
    /// of its assignment after `build()`.
    pub fn partitions(&self) -> Vec<u32> {
        self.shared.partitions()
    }

    /// Stops the reader. With `OnRead`, it cancels each read in flight and takes its one reply,
    /// which tells what the server stored, at most until the deadline of the read. With
    /// `Manual` it commits nothing. With `Auto`, it counts the last batch under
    /// `CommitAfter::Processing` and stores the latest counted position of each partition.
    ///
    /// With `OnRead`, it moves the offset of each partition with undelivered batches, or with a
    /// read whose reply was lost, back to its last delivered message. A partition that delivered
    /// nothing gets the offset before its first undelivered message, whatever the start, or no
    /// offset at offset 0. It does not move the offset back over the batches of a partition that
    /// left the assignment. A group reader then leaves its group.
    ///
    /// The close has one budget, 10 s from its start. A store or a delete of the close that the
    /// server refuses as not owned goes once more under the owner that a read of the partition
    /// shows, and that read retries a transient refusal until the budget ends. Then the refusal
    /// stands, each store or delete that did not go yet still goes once under its own request
    /// timeout, and the leave follows. A dropped close sends neither the writes that did not go
    /// yet nor the leave, so a group member stays in its group until its session ends.
    ///
    /// # Errors
    ///
    /// The first failed store of the `OnRead` or the `Auto` close. The close abandons a store or
    /// a delete with an unknown outcome on the client, so a later reader of the partition can
    /// write. Otherwise [`IggyError::PartitionLaneBlocked`] for a partition with an unknown
    /// outcome: abandon its operation on the client before a new reader reads the partition.
    pub async fn close(mut self) -> Result<(), IggyError> {
        let budget = Instant::now() + CLOSE_BUDGET;
        self.shared.progress.stop_background();
        // The rewind goes after each seek write, also of a seek dropped after its write started.
        self.shared.progress.close_seeks().await;
        self.shared.closing.store(true, ORDERING);
        self.shared.progress.wake.notify_one();
        if let Some(task) = self.task.take() {
            // A worker that failed already ended, which is all that the close waits for.
            let _ = task.await;
        }
        // A seek that moved after its caller dropped it replaced batches that its call did not
        // drop.
        self.drop_replaced();
        let undelivered: Vec<PolledMessages> = self
            .queue
            .drain(..)
            .filter_map(|entry| entry.result.ok())
            .collect();
        // The worker lifts the block of an abandoned operation only at its next turn.
        self.shared.progress.unblock_abandoned().await;
        let mut result = Ok(());
        if self.config.on_read() {
            let owned = self.shared.owned_partitions();
            result = in_close(
                budget,
                self.shared.progress.rewind(undelivered, owned.as_deref()),
            )
            .await;
        }
        self.count_uncounted();
        if let Some(committer) = self.committer.take() {
            committer.stop().await;
            result = in_close(budget, self.shared.progress.flush_counted()).await;
        }
        // The server refuses a store from a member that left, so the rewind goes first.
        if let Some(membership) = &self.shared.membership {
            membership.leave().await;
        }
        result?;
        match self.shared.progress.blocks().first() {
            Some(&(partition, operation)) => {
                Err(IggyError::PartitionLaneBlocked(partition, operation))
            }
            None => Ok(()),
        }
    }

    /// Under `CommitPolicy::Auto`, stops the store task once its store in flight ends, which ends
    /// the retries of that store, and stores the latest counted position of each partition. The
    /// reader stores nothing more after it, also not in [`close`](Self::close).
    pub(crate) async fn flush(&mut self) -> Result<(), IggyError> {
        let Some(committer) = self.committer.take() else {
            return Ok(());
        };
        self.shared.progress.stop_background();
        committer.stop().await;
        self.shared.progress.flush_counted().await
    }

    /// Under `CommitAfter::Processing`, the batch that the application took last counts once
    /// it asks for the next one.
    fn count_uncounted(&mut self) {
        if let Some((checkpoint, messages)) = self.uncounted.take() {
            self.count(checkpoint, messages, true);
        }
    }

    fn hand_over(&self, entry: Entry) -> Option<Result<(PolledMessages, Hold), ReadError>> {
        let batch = match entry.result {
            Ok(batch) => batch,
            Err(Refusal { error, generation }) => {
                let current = match (error.partition_id, generation) {
                    (Some(partition), Some(generation)) => {
                        self.shared
                            .progress
                            .keeps(partition, generation, self.config.on_read())
                    }
                    _ => entry.epoch == self.shared.epoch.load(ORDERING),
                };
                return current.then_some(Err(error));
            }
        };
        let stamp = batch.stamp()?;
        let mut hold = Hold {
            _capacity: entry.capacity,
            shared: Arc::clone(&self.shared),
            on_read: self.config.on_read(),
            epoch: entry.epoch,
            assignment: entry.assignment,
            partition: batch.partition_id,
            generation: stamp.generation,
            generation_checked: None,
            checked: None,
        };
        if !hold.is_current() {
            return None;
        }
        if hold.on_read
            && let Some(checkpoint) = batch.checkpoint()
        {
            self.shared.progress.record_commit(checkpoint);
        }
        Some(Ok((batch, hold)))
    }

    fn split(&mut self, reply: Reply) {
        let Reply {
            epoch,
            assignment,
            result,
            capacity,
        } = reply;
        match result {
            Err(error) => self.queue.push_back(Entry {
                epoch,
                assignment,
                result: Err(error),
                capacity,
            }),
            Ok(batches) => self.queue.extend(batches.into_iter().map(|batch| Entry {
                epoch,
                assignment,
                result: Ok(batch),
                capacity: Arc::clone(&capacity),
            })),
        }
    }
}

impl Debug for PartitionReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionReader")
            .field("stream", &self.config.stream)
            .field("topic", &self.config.topic)
            .field("consumer", &self.config.consumer)
            .field("options", &self.config.options)
            .field("queued", &self.queue.len())
            .finish_non_exhaustive()
    }
}

impl Drop for PartitionReader {
    fn drop(&mut self) {
        self.shared.progress.stop_background();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Committer {
    fn spawn(progress: Arc<Progress>, interval: Option<NonZeroIggyDuration>) -> Self {
        let stop = Arc::new(Notify::new());
        let stopped = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let mut ticks = interval.map(|interval| {
                let period = interval.get_duration();
                let mut ticks = interval_at(Instant::now() + period, period);
                ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
                ticks
            });
            loop {
                // A flush after the stop would race the final one.
                tokio::select! {
                    biased;
                    () = stopped.notified() => return,
                    () = progress.store_due() => {}
                    () = next_tick(&mut ticks) => {}
                }
                // The flush logs each failure, and the next one stores the position again.
                let _ = in_background(progress.flush_counted()).await;
            }
        });
        Self { stop, task }
    }

    /// Lets a flush in flight finish, so its stores never race the final one.
    async fn stop(mut self) {
        self.stop.notify_one();
        // A task that panicked stored what it could, and the final flush stores the rest.
        let _ = (&mut self.task).await;
    }
}

/// A dropped `JoinHandle` detaches its task, so without this a dropped stop or close would leave
/// the store task running.
impl Drop for Committer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ReadError {
    /// An error of `partition_id`, or of the whole reader for `None`, with no other field set.
    pub fn new(partition_id: Option<u32>, error: IggyError) -> Self {
        Self {
            partition_id,
            error,
            skip: None,
            offset: None,
            record: None,
            lost: None,
        }
    }

    /// Whether the reader stopped the partition of this error after refusals in a row that a
    /// quick retry does not change. Only a [`seek`](PartitionReader::seek) reads it again.
    pub fn is_lasting_refusal(&self) -> bool {
        self.partition_id.is_some() && self.skip.is_none() && lasting_refusal(&self.error)
    }
}

impl Display for ReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.partition_id {
            Some(partition_id) => write!(f, "partition ID: {partition_id}: {}", self.error)?,
            None => Display::fmt(&self.error, f)?,
        }
        if let Some(lost) = &self.lost {
            write!(f, ", lost offsets: {} to {}", lost.start(), lost.end())?;
        }
        Ok(())
    }
}

impl Error for ReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

impl From<ReadError> for IggyError {
    fn from(error: ReadError) -> Self {
        error.error
    }
}

impl Config {
    pub(crate) fn is_group(&self) -> bool {
        self.consumer.kind == ConsumerKind::ConsumerGroup
    }

    pub(crate) fn on_read(&self) -> bool {
        self.options.commit == CommitPolicy::OnRead
    }

    pub(crate) fn auto(&self) -> Option<AutoCommit> {
        match self.options.commit {
            CommitPolicy::Auto(auto) => Some(auto),
            CommitPolicy::Manual | CommitPolicy::OnRead => None,
        }
    }
}

impl Shared {
    fn partitions(&self) -> Vec<u32> {
        self.partitions_guard().clone()
    }

    fn set_partitions(&self, partitions: &[u32]) {
        partitions.clone_into(&mut self.partitions_guard());
    }

    /// The partitions that a group member owns now. `None` for a standalone reader.
    fn owned_partitions(&self) -> Option<Vec<u32>> {
        self.assignment_state
            .as_ref()
            .map(|(state, key)| state.assignment_snapshot(key).1)
    }

    fn partitions_guard(&self) -> MutexGuard<'_, Vec<u32>> {
        self.partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for FetchCapacity {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.wake.notify_one();
    }
}

impl Entry {
    /// The partition and the generation of a batch, or of an error of one partition.
    fn generation(&self) -> Option<(u32, u64)> {
        match &self.result {
            Ok(batch) => batch
                .stamp()
                .map(|stamp| (batch.partition_id, stamp.generation)),
            Err(refusal) => refusal.error.partition_id.zip(refusal.generation),
        }
    }
}

impl Hold {
    /// Whether the batch still counts: the generation of its partition, and without `OnRead`
    /// also the epoch and the assignment of a group member.
    pub(crate) fn is_current(&mut self) -> bool {
        let progress = &self.shared.progress;
        // Lock the generations only after a seek or an assignment changed them.
        let generation_changes = progress.generation_changes();
        if self.generation_checked != Some(generation_changes) {
            if !progress.keeps(self.partition, self.generation, self.on_read) {
                return false;
            }
            self.generation_checked = Some(generation_changes);
        }
        if self.on_read {
            return true;
        }
        if self.epoch != self.shared.epoch.load(ORDERING) {
            return false;
        }
        let Some((state, key)) = &self.shared.assignment_state else {
            return true;
        };
        // Lock the group state only after it changed since the last check.
        let changes = state.assignment_changes();
        if self.checked == Some(changes) {
            return true;
        }
        let current = self
            .assignment
            .is_some_and(|assignment| state.is_current_assignment(key, assignment, self.partition));
        if current {
            self.checked = Some(changes);
        }
        current
    }
}

/// The server reads `First` from offset 0, and a group fetch takes only offsets and `Next`.
pub(super) fn first_as_offset(start: PollingStrategy) -> PollingStrategy {
    if start.kind == PollingKind::First {
        PollingStrategy::offset(0)
    } else {
        start
    }
}

async fn next_tick(ticks: &mut Option<Interval>) {
    match ticks {
        Some(ticks) => {
            ticks.tick().await;
        }
        None => pending().await,
    }
}

fn spawn_worker(
    config: &Arc<Config>,
    shared: &Arc<Shared>,
    events: Option<Receiver<DiagnosticEvent>>,
) -> (mpsc::UnboundedReceiver<Reply>, JoinHandle<()>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let worker = Worker::new(Arc::clone(config), Arc::clone(shared), sender, events);
    (receiver, tokio::spawn(worker.run()))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::poll_fn;
    use std::iter::{once, repeat_n};
    use std::num::NonZeroU32;
    use std::ops::RangeInclusive;
    use std::pin::pin;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::FutureExt;
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::codes::{
        DELETE_CONSUMER_OFFSET_CODE, GET_CONSUMER_OFFSET_CODE, JOIN_CONSUMER_GROUP_CODE,
        POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::primitives::partition_history::PartitionContext;
    use iggy_binary_protocol::requests::consumer_offsets::DeleteConsumerOffsetRequest;
    use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
    use iggy_common::{
        Aes256GcmEncryptor, AutoCommit, Checkpoint, Client, CommitAfter, CommitEvery, CommitPolicy,
        Consumer, ConsumerGroupClient, ConsumerOffsetClient, EncryptorKind, IggyError,
        MessageClient, NonZeroIggyDuration, PollOptions, PolledMessages, PollingKind,
        PollingStrategy, ReaderStamp,
    };
    use tokio::sync::{Semaphore, mpsc};
    use tokio::time::{Instant, sleep, timeout};

    use super::fixtures::{
        CONSUMER_ID, CONTEXT, GROUP_ID, PRIMARY, STREAM_ID, TOPIC_ID, builder_for, builder_on,
        committing, connect_to_one_replica, group, identifier, lookup_answer, message_at,
        partition_and_offsets, polled_answer_in, stored_offsets,
    };
    use super::progress::{PendingCommit, Progress, in_background};
    use super::{
        CLOSE_BUDGET, Committer, FetchCapacity, ORDERING, PartitionReader, ReadError, Refusal,
        Reply,
    };
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::reader_builder::{DEFAULT_PREFETCH_MESSAGES, ReaderBuilder};
    use crate::driver::with_deadline;
    use crate::http::http_client::HttpClient;
    use crate::links::fake_cluster::{Answer, FakeCluster, endpoint, eventually};
    use crate::links::{OPERATION_TIMEOUT, RETRY_BACKOFF_MAX};
    use crate::tcp::tcp_client::TcpClient;

    const PARTITION: u32 = 1;
    const OTHER_PARTITION: u32 = 2;
    const THIRD_PARTITION: u32 = 3;
    const FIRST_COMMIT: u64 = 1;
    const SECOND_COMMIT: u64 = 2;
    const KEY: [u8; 32] = [1; 32];
    const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
    /// Ends a call long before any operation deadline.
    const CANCEL_AFTER: Duration = Duration::from_millis(10);
    const NEW_OWNER: PartitionContext = PartitionContext {
        owner_generation: 2,
        ..CONTEXT
    };
    const RECREATED: PartitionContext = PartitionContext {
        incarnation: 2,
        ..CONTEXT
    };
    /// An owner that the server replaced.
    const REPLACED_OWNER: PartitionContext = PartitionContext {
        owner_generation: 1,
        ..CONTEXT
    };
    /// The owner that the server installed in place of [`REPLACED_OWNER`], at a later op.
    const CURRENT_OWNER: PartitionContext = PartitionContext {
        owner_generation: 3,
        metadata_op: 9,
        ..CONTEXT
    };
    /// The offset that a first commit stores.
    const STORED_OFFSET: u64 = 100;
    /// An offset below [`STORED_OFFSET`] that a second commit asks for.
    const LOWER_OFFSET: u64 = 10;
    const DELIVERED_OFFSET: u64 = 4;
    /// The messages after the delivered one.
    const BATCH: RangeInclusive<u64> = DELIVERED_OFFSET + 1..=DELIVERED_OFFSET + 3;
    /// The first offset of a partition. A rewind before it deletes the offset.
    const START_OFFSET: u64 = 0;
    const SEEK_OFFSET: u64 = 50;
    /// Longer than any test, so only an event or the close stores.
    const NEVER: Duration = Duration::from_secs(24 * 60 * 60);
    /// Far more transient refusals of a read than the driver retries within one operation
    /// timeout: it waits up to `RETRY_BACKOFF_MAX` before each retry, half of that on average.
    const REFUSED_READS: usize =
        10 * (OPERATION_TIMEOUT.as_secs() / RETRY_BACKOFF_MAX.as_secs()) as usize;

    #[tokio::test]
    async fn given_a_commit_of_a_later_poll_when_committing_at_or_below_it_should_store_nothing() {
        let fed = FedReader::new(standalone([PARTITION]));
        let later_poll = PartitionContext {
            metadata_op: CONTEXT.metadata_op + 1,
            ..CONTEXT
        };
        let committed = fed.checkpoint(PARTITION, STORED_OFFSET, later_poll);
        fed.progress().record_commit(committed);

        for offset in [LOWER_OFFSET, STORED_OFFSET] {
            fed.reader
                .commit(fed.checkpoint(PARTITION, offset, CONTEXT))
                .await
                .unwrap();
        }

        // The client never connects, so only a commit that stores nothing succeeds.
        assert_eq!(
            fed.reader
                .commit(fed.checkpoint(PARTITION, STORED_OFFSET + 1, CONTEXT))
                .await,
            Err(IggyError::Disconnected)
        );
        assert_eq!(fed.progress().last_committed(PARTITION), Some(committed));
    }

    #[tokio::test(start_paused = true)]
    async fn given_auto_commit_when_closing_should_store_only_counted_positions_of_the_current_generation_ahead_of_the_last_store()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(
            standalone_on(client, [PARTITION, OTHER_PARTITION, THIRD_PARTITION])
                .options(auto(None, CommitAfter::Delivery)),
        );
        fed.reader.count(
            fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT),
            1,
            true,
        );
        fed.progress()
            .record_commit(fed.checkpoint(OTHER_PARTITION, STORED_OFFSET, CONTEXT));
        fed.reader.count(
            fed.checkpoint(OTHER_PARTITION, LOWER_OFFSET, CONTEXT),
            1,
            true,
        );
        fed.reader.count(
            fed.checkpoint(THIRD_PARTITION, LOWER_OFFSET, CONTEXT),
            1,
            true,
        );
        fed.reader
            .seek(THIRD_PARTITION, PollingStrategy::offset(SEEK_OFFSET))
            .await
            .unwrap();

        fed.reader.close().await.unwrap();

        assert_eq!(
            stored_offsets(&cluster),
            [DELIVERED_OFFSET],
            "a store behind the last one and a count of the generation before a seek are dropped"
        );
    }

    #[tokio::test]
    async fn given_auto_commit_every_n_messages_when_counting_should_make_a_store_due_after_n_messages_of_one_partition()
     {
        const EVERY: u32 = 3;
        let every = CommitEvery::Messages(NonZeroU32::new(EVERY).unwrap());
        let (_, shared) = standalone([PARTITION, OTHER_PARTITION])
            .options(auto(Some(every), CommitAfter::Processing))
            .into_parts()
            .unwrap();
        let progress = shared.progress;
        progress.assign(&[PARTITION, OTHER_PARTITION], &[]);
        let at = |partition, offset| {
            let generation = progress.generation(partition).unwrap();
            Checkpoint::stamped(partition, offset, CONTEXT, Some(progress.stamp(generation)))
        };
        let due = || progress.store_due().now_or_never().is_some();

        progress.count(at(PARTITION, 10), EVERY - 1, false);
        progress.count(at(OTHER_PARTITION, 10), EVERY - 1, false);
        assert!(!due(), "each partition counted fewer than {EVERY} messages");

        progress.count(at(PARTITION, 11), 1, false);
        assert!(
            due(),
            "{EVERY} counted messages of one partition make a store due"
        );
    }

    #[tokio::test]
    async fn given_a_checkpoint_of_the_generation_before_a_seek_when_committing_should_store_nothing()
     {
        let mut fed = FedReader::new(standalone([PARTITION]));
        let before_seek = fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT);
        fed.reader
            .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
            .await
            .unwrap();

        fed.reader.commit(before_seek).await.unwrap();

        // The client never connects, so only a commit that stores nothing succeeds.
        assert_eq!(
            fed.reader
                .commit(fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT))
                .await,
            Err(IggyError::Disconnected)
        );
    }

    /// A dropped commit leaves its request open on the client, so the next commit resolves it.
    #[tokio::test(start_paused = true)]
    async fn given_a_dropped_commit_when_committing_the_same_checkpoint_should_resolve_it_and_store()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        let checkpoint = fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT);
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(SECOND_COMMIT)],
        );

        assert!(
            timeout(CANCEL_AFTER, fed.reader.commit(checkpoint))
                .await
                .is_err()
        );
        assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
        fed.reader.commit(checkpoint).await.unwrap();

        let stores = cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(
            stored_offsets(&cluster),
            [STORED_OFFSET, STORED_OFFSET, STORED_OFFSET]
        );
        assert_eq!(
            stores[0].header.request, stores[1].header.request,
            "the commit resolves the old request first"
        );
        assert_eq!(stores[0].body, stores[1].body);
        assert_ne!(stores[1].header.request, stores[2].header.request);
        assert_eq!(fed.progress().last_committed(PARTITION), Some(checkpoint));
    }

    /// A commit with an unknown outcome returns its error, and the reader abandons it on the
    /// client, so the next commit of the partition goes out at once.
    #[tokio::test(start_paused = true)]
    async fn given_a_commit_with_an_unknown_outcome_when_committing_again_should_send_the_new_checkpoint()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(SECOND_COMMIT)],
        );
        let first = fed
            .reader
            .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
            .await;
        let old = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        let checkpoint = fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT);

        let second = fed.reader.commit(checkpoint).await;

        assert_eq!(first, Err(IggyError::OperationUncertain(old)));
        assert_eq!(second, Ok(()));
        assert_eq!(
            stored_offsets(&cluster),
            [LOWER_OFFSET, STORED_OFFSET],
            "the second commit goes out without the first one"
        );
        assert_eq!(fed.progress().last_committed(PARTITION), Some(checkpoint));
    }

    /// The final store of an `Auto` close with an unknown outcome fails the close, which abandons
    /// it on the client, so a later write of the partition goes out.
    #[tokio::test(start_paused = true)]
    async fn given_auto_commit_when_the_final_store_has_an_unknown_outcome_should_fail_the_close_and_release_the_partition()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(
            standalone_on(client, [PARTITION]).options(auto(None, CommitAfter::Delivery)),
        );
        fed.reader.count(
            fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT),
            1,
            true,
        );
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(SECOND_COMMIT)],
        );
        let client = fed.reader.config.client.clone();

        let closed = fed.reader.close().await;
        let later = client
            .read()
            .await
            .store_consumer_offset(
                &consumer(),
                &identifier(STREAM_ID),
                &identifier(TOPIC_ID),
                Some(PARTITION),
                DELIVERED_OFFSET,
            )
            .await;

        assert!(
            matches!(closed, Err(IggyError::OperationUncertain(_))),
            "{closed:?}"
        );
        assert_eq!(later, Ok(()), "the close abandoned its store on the client");
        assert_eq!(stored_offsets(&cluster), [DELIVERED_OFFSET; 2]);
    }

    /// The store of a partition that a rebalance took away with an unknown outcome fails the
    /// flush, which abandons it on the client, so a later write of the partition goes out.
    #[tokio::test(start_paused = true)]
    async fn given_auto_commit_when_a_retired_store_has_an_unknown_outcome_should_fail_the_flush_and_release_the_partition()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(
            standalone_on(client, [PARTITION]).options(auto(None, CommitAfter::Delivery)),
        );
        fed.reader.count(
            fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT),
            1,
            true,
        );
        fed.progress().retire_counted(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(SECOND_COMMIT)],
        );

        let flushed = fed.progress().flush_counted().await;
        let later = fed
            .reader
            .config
            .client
            .read()
            .await
            .store_consumer_offset(
                &consumer(),
                &identifier(STREAM_ID),
                &identifier(TOPIC_ID),
                Some(PARTITION),
                DELIVERED_OFFSET,
            )
            .await;

        assert!(
            matches!(flushed, Err(IggyError::OperationUncertain(_))),
            "{flushed:?}"
        );
        assert_eq!(later, Ok(()), "the flush abandoned its store on the client");
        assert_eq!(stored_offsets(&cluster), [DELIVERED_OFFSET; 2]);
    }

    /// A dropped close or flush of an `Auto` reader ends its store task with the store in
    /// flight, so no store of the reader goes after it.
    #[tokio::test(start_paused = true)]
    async fn given_a_store_in_flight_when_the_close_or_the_flush_is_dropped_should_end_the_store_task()
     {
        for close in [true, false] {
            let (client, cluster) = connect_to_one_replica().await;
            let mut fed = FedReader::new(
                standalone_on(client, [PARTITION])
                    .options(auto(Some(CommitEvery::Message), CommitAfter::Delivery)),
            );
            let progress = Arc::clone(&fed.reader.shared.progress);
            cluster.hold(STORE_CONSUMER_OFFSET_CODE);
            fed.reader.count(
                fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT),
                1,
                true,
            );
            eventually("the store task stores", || {
                cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
            })
            .await;

            let dropped = if close {
                timeout(CANCEL_AFTER, fed.reader.close()).await.is_err()
            } else {
                timeout(CANCEL_AFTER, fed.reader.flush()).await.is_err()
            };
            sleep(CANCEL_AFTER).await;

            assert!(dropped, "close: {close}");
            // The store task holds the progress until it ends, and so does a reader.
            let holders = if close { 1 } else { 2 };
            assert_eq!(Arc::strong_count(&progress), holders, "close: {close}");
        }
    }

    /// A stop that meets a due store ends the store task without a flush, which would race the
    /// final one.
    #[tokio::test]
    async fn given_a_due_store_when_the_store_task_stops_should_start_no_flush() {
        /// The stop and the due store are both there when the task runs, so a choice between them
        /// that is left to chance fails in some round.
        const ROUNDS: u64 = 32;
        let (client, cluster) = connect_to_one_replica().await;
        let (_, shared) = standalone_on(client, [PARTITION])
            .options(auto(Some(CommitEvery::Message), CommitAfter::Delivery))
            .into_parts()
            .unwrap();
        let progress = shared.progress;
        progress.assign(&[PARTITION], &[]);
        let stamp = progress.stamp(progress.generation(PARTITION).unwrap());

        for offset in 0..ROUNDS {
            let committer = Committer::spawn(Arc::clone(&progress), None);
            progress.count(
                Checkpoint::stamped(PARTITION, offset, CONTEXT, Some(stamp)),
                1,
                true,
            );
            committer.stop().await;
        }

        assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 0);
    }

    #[tokio::test]
    async fn given_a_commit_when_its_owner_or_incarnation_changes_should_store_a_lower_offset_under_the_new_history()
     {
        for changed in [NEW_OWNER, RECREATED] {
            let (client, cluster) = connect_to_one_replica().await;
            let fed = FedReader::new(builder_on(client, group()));
            fed.assign(&[PARTITION]);
            fed.reader
                .commit(fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT))
                .await
                .unwrap();

            fed.reader
                .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, changed))
                .await
                .unwrap();

            assert_eq!(stored_offsets(&cluster), [STORED_OFFSET, LOWER_OFFSET]);
            assert_eq!(
                histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
                [history(CONTEXT), history(changed)]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_dropped_commit_when_its_blocker_is_rejected_should_store_the_new_checkpoint() {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [
                Answer::Silence,
                Answer::CommitRejected(FIRST_COMMIT, IggyError::HistoryUnavailable),
                Answer::Commit(SECOND_COMMIT),
            ],
        );
        assert!(
            timeout(
                CANCEL_AFTER,
                fed.reader
                    .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
            )
            .await
            .is_err()
        );
        let old = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;

        let checkpoint = fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT);
        fed.reader.commit(checkpoint).await.unwrap();

        let resolved = fed
            .reader
            .config
            .client
            .read()
            .await
            .resolve_operation(old, (Instant::now() + OPERATION_TIMEOUT).into_std())
            .await;
        assert_eq!(resolved, Err(IggyError::HistoryUnavailable));
        let stores = cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(
            stored_offsets(&cluster),
            [LOWER_OFFSET, LOWER_OFFSET, STORED_OFFSET]
        );
        assert_eq!(stores[0].header.request, stores[1].header.request);
        assert_eq!(stores[0].body, stores[1].body);
        assert_ne!(stores[1].header.request, stores[2].header.request);
        assert_eq!(fed.progress().last_committed(PARTITION), Some(checkpoint));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_dropped_commit_that_stays_uncertain_when_committing_again_should_leave_the_new_checkpoint_unsent()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Silence],
        );
        assert!(
            timeout(
                CANCEL_AFTER,
                fed.reader
                    .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
            )
            .await
            .is_err()
        );
        let old = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        let started = Instant::now();

        let result = fed
            .reader
            .commit(fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT))
            .await;

        assert_eq!(result, Err(IggyError::LaneBlocked(old)));
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        assert_eq!(stored_offsets(&cluster), [LOWER_OFFSET, LOWER_OFFSET]);
        assert_eq!(fed.progress().last_committed(PARTITION), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_outer_deadline_past_the_default_budget_when_a_commit_is_unanswered_should_end_at_the_outer_deadline()
     {
        const OUTER_DEADLINE: Duration = OPERATION_TIMEOUT.saturating_mul(2);
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence],
        );
        let started = Instant::now();

        let result = with_deadline(
            started + OUTER_DEADLINE,
            fed.reader
                .commit(fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT)),
        )
        .await;

        let op = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(result, Err(IggyError::OperationUncertain(op)));
        assert_eq!(started.elapsed(), OUTER_DEADLINE);
    }

    #[tokio::test]
    async fn given_a_checkpoint_inside_a_batch_when_committing_should_store_its_message_and_skip_a_lower_one()
     {
        const INSIDE: usize = 1;
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(standalone_on(client, [PARTITION]));
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        let batch = fed.reader.next().await.unwrap();

        fed.reader
            .commit(batch.checkpoint_at(INSIDE).unwrap())
            .await
            .unwrap();
        fed.reader
            .commit(batch.checkpoint_at(0).unwrap())
            .await
            .unwrap();

        assert_eq!(
            stored_offsets(&cluster),
            [batch.messages[INSIDE].header.offset]
        );
    }

    #[tokio::test]
    async fn given_a_checkpoint_of_another_reader_or_without_a_stamp_when_committing_should_refuse_it()
     {
        let fed = FedReader::new(standalone([PARTITION]));
        let other = FedReader::new(standalone([PARTITION]));

        for checkpoint in [
            other.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT),
            Checkpoint::new(PARTITION, LOWER_OFFSET, CONTEXT),
        ] {
            assert_eq!(
                fed.reader.commit(checkpoint).await,
                Err(IggyError::InvalidCommand)
            );
        }
    }

    #[tokio::test]
    async fn given_a_server_commit_on_read_when_committing_should_refuse_it() {
        let fed = FedReader::new(committing(standalone([PARTITION])));

        assert_eq!(
            fed.reader
                .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
                .await,
            Err(IggyError::InvalidCommand)
        );
    }

    #[test]
    fn given_revoked_and_stored_partitions_when_listing_unstored_deliveries_should_skip_them() {
        const BEHIND: u32 = PARTITION;
        const REVOKED: u32 = OTHER_PARTITION;
        const STORED: u32 = THIRD_PARTITION;
        let fed = FedReader::new(standalone([BEHIND, REVOKED, STORED]));
        for (partition, delivered) in [
            (BEHIND, LOWER_OFFSET),
            (REVOKED, LOWER_OFFSET),
            (STORED, STORED_OFFSET),
        ] {
            fed.progress()
                .deliver(&fed.checkpoint(partition, delivered, CONTEXT));
            fed.progress()
                .record_commit(fed.checkpoint(partition, STORED_OFFSET, CONTEXT));
        }
        let behind = fed.checkpoint(BEHIND, LOWER_OFFSET, CONTEXT);

        assert_eq!(
            fed.progress().unstored_delivered(Some(&[BEHIND, STORED])),
            [behind]
        );
        let mut unstored = fed.progress().unstored_delivered(None);
        unstored.sort_unstable_by_key(Checkpoint::partition_id);
        assert_eq!(
            unstored,
            [behind, fed.checkpoint(REVOKED, LOWER_OFFSET, CONTEXT)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_blocked_fetch_progress_when_closing_should_skip_its_rewind_and_report_the_block()
    {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
        let operation = lose_committing_poll(&fed, &cluster, PARTITION).await;
        fed.progress()
            .deliver(&fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT));
        fed.progress()
            .record_commit(fed.checkpoint(PARTITION, DELIVERED_OFFSET + 1, CONTEXT));
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        let shared = Arc::clone(&fed.reader.shared);

        let closed = fed.reader.close().await;

        assert_eq!(
            closed,
            Err(IggyError::PartitionLaneBlocked(PARTITION, operation))
        );
        assert!(
            stored_offsets(&cluster).is_empty(),
            "the server can store the offset of the lost read after any store of the close"
        );
        assert!(shared.closing.load(ORDERING));
    }

    /// A committing read with an unknown outcome blocks its partition, so a seek of it fails
    /// until the caller abandons the operation, also a seek that writes nothing.
    #[tokio::test(start_paused = true)]
    async fn given_blocked_fetch_progress_when_seeking_to_next_or_last_should_refuse_the_seek() {
        for start in [PollingStrategy::next(), PollingStrategy::last()] {
            let (client, cluster) = connect_to_one_replica().await;
            let mut fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
            let operation = lose_committing_poll(&fed, &cluster, PARTITION).await;

            let sought = fed.reader.seek(PARTITION, start).await;

            assert_eq!(
                sought,
                Err(IggyError::PartitionLaneBlocked(PARTITION, operation)),
                "{start:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_abandoned_block_when_closing_should_lift_it_and_rewind_its_partition() {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
        let operation = lose_committing_poll(&fed, &cluster, PARTITION).await;
        fed.progress()
            .deliver(&fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT));
        fed.progress()
            .record_commit(fed.checkpoint(PARTITION, DELIVERED_OFFSET + 1, CONTEXT));
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        let abandoned = fed
            .reader
            .config
            .client
            .read()
            .await
            .abandon_operation(operation)
            .await;

        let closed = fed.reader.close().await;

        assert_eq!(abandoned, Ok(()));
        assert_eq!(closed, Ok(()), "the caller abandoned the only block");
        assert_eq!(
            stored_offsets(&cluster),
            [DELIVERED_OFFSET],
            "the close moves the offset back over the queued batch"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_undecoded_healthy_and_blocked_sections_when_closing_should_rewind_only_the_healthy_one()
     {
        const BLOCKED: u32 = OTHER_PARTITION;
        const UNDECODED_OFFSET: u64 = 7;
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(committing(standalone_on(client, [PARTITION, BLOCKED])));
        let progress = Arc::clone(&fed.reader.shared.progress);
        for partition in [PARTITION, BLOCKED] {
            progress.remember_undecoded(Checkpoint::new(partition, UNDECODED_OFFSET, CONTEXT));
        }
        let operation = lose_committing_poll(&fed, &cluster, BLOCKED).await;

        assert_eq!(
            fed.reader.close().await,
            Err(IggyError::PartitionLaneBlocked(BLOCKED, operation))
        );
        assert_eq!(stored_offsets(&cluster), [UNDECODED_OFFSET - 1]);
        assert_eq!(
            histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
            [history(CONTEXT)]
        );
        assert_eq!(progress.undecoded(PARTITION), None);
    }

    #[tokio::test]
    async fn given_a_queued_batch_of_the_delivered_history_when_closing_should_store_the_delivered_offset_again()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
        fed.send(Ok(vec![fed.batch(
            PARTITION,
            DELIVERED_OFFSET..=DELIVERED_OFFSET,
            CONTEXT,
        )]));
        fed.reader.next().await.unwrap();
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));

        fed.reader.close().await.unwrap();

        assert_eq!(
            stored_offsets(&cluster),
            [DELIVERED_OFFSET],
            "the server stored the offset of the queued batch, so the delivered offset is stored again"
        );
    }

    #[tokio::test]
    async fn given_queued_batches_of_a_reply_when_closing_should_rewind_over_them() {
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(standalone_on(
            client,
            [PARTITION, OTHER_PARTITION],
        )));
        fed.send(Ok(vec![
            fed.batch(PARTITION, DELIVERED_OFFSET..=DELIVERED_OFFSET + 1, CONTEXT),
            fed.batch(OTHER_PARTITION, START_OFFSET..=START_OFFSET + 1, CONTEXT),
        ]));
        let (batch, hold) = poll_fn(|cx| fed.reader.poll_held(cx)).await.unwrap();
        fed.reader.delivered(&batch.checkpoint_at(0).unwrap());
        drop(hold);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(OTHER_PARTITION, Some(START_OFFSET + 1))],
        );

        fed.reader.close().await.unwrap();

        assert_eq!(
            stored_offsets(&cluster),
            [DELIVERED_OFFSET],
            "the server stored the offset of the whole batch, so the delivered offset is stored again"
        );
        assert_eq!(
            deleted_partitions(&cluster),
            [Some(OTHER_PARTITION)],
            "nothing of the queued batch was delivered, so the offset goes back before it"
        );
    }

    #[tokio::test]
    async fn given_a_batch_of_a_newer_history_than_the_delivered_message_when_closing_should_store_before_it_and_skip_the_delivered_message()
     {
        const RECREATED_OFFSET: u64 = 2;
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
        fed.send(Ok(vec![fed.batch(
            PARTITION,
            DELIVERED_OFFSET..=DELIVERED_OFFSET,
            CONTEXT,
        )]));
        fed.reader.next().await.unwrap();
        fed.send(Ok(vec![fed.batch(
            PARTITION,
            RECREATED_OFFSET..=RECREATED_OFFSET + 1,
            RECREATED,
        )]));

        fed.reader.close().await.unwrap();

        assert_eq!(stored_offsets(&cluster), [RECREATED_OFFSET - 1]);
        assert_eq!(
            histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
            [history(RECREATED)],
            "a server of the new history refuses the delivered message of the old one, so the close skips it"
        );
    }

    #[tokio::test]
    async fn given_undelivered_batches_of_two_histories_when_closing_should_rewind_each_under_its_context()
     {
        let recreated = PartitionContext {
            owner_generation: NEW_OWNER.owner_generation,
            ..RECREATED
        };
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        fed.send(Ok(vec![fed.batch(
            PARTITION,
            START_OFFSET..=START_OFFSET + 1,
            recreated,
        )]));
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, Some(START_OFFSET + 1))],
        );

        fed.reader.close().await.unwrap();

        assert_eq!(stored_offsets(&cluster), [BATCH.start() - 1]);
        assert_eq!(
            histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
            [history(CONTEXT)]
        );
        assert_eq!(
            histories(&cluster, DELETE_CONSUMER_OFFSET_CODE),
            [history(recreated)],
            "the delete carries the history of its batch, not the one of the route"
        );
    }

    /// The metadata op of the join of a new session, which installs the owner again.
    const JOIN_OP: u64 = 5;

    /// A session change installs the owner again under a higher generation, so a seek before the
    /// first delivery of the new session names owner 0. The server binds owner 0 only under a
    /// metadata op at or after the installation.
    #[tokio::test]
    async fn given_a_session_change_when_seeking_before_the_first_new_delivery_should_store_under_owner_0_at_the_session_watermark()
     {
        let (sought, stores) = seek_after_a_session_change(CONTEXT.incarnation).await;

        sought.unwrap();
        assert_eq!(stores, [(CONTEXT.incarnation, 0, JOIN_OP)]);
    }

    /// The history of the route must not take the place of the history of the delivered message,
    /// or the offset would land in a history that the application never read.
    #[tokio::test]
    async fn given_a_session_change_and_a_later_history_when_seeking_before_the_first_new_delivery_should_not_store_in_the_later_history()
     {
        const LATER_HISTORY: u64 = 2;
        let (sought, stores) = seek_after_a_session_change(LATER_HISTORY).await;

        assert!(sought.is_err());
        assert!(
            stores.iter().all(|(history, ..)| *history != LATER_HISTORY),
            "{stores:?}"
        );
    }

    /// Seeks after the join of a new session installed its owner in `history`, before the first
    /// delivery of that session. Returns the result and the history, owner and metadata op of
    /// each store.
    async fn seek_after_a_session_change(
        history: u64,
    ) -> (Result<(), IggyError>, Vec<(u64, u64, u64)>) {
        const EARLIER_OWNER: PartitionContext = PartitionContext {
            owner_generation: 1,
            ..CONTEXT
        };
        const INSTALLED_OWNER: u64 = 2;
        const TARGET: u64 = 50;
        let (client, cluster) = connect_to_one_replica().await;
        cluster.script(
            &endpoint(PRIMARY),
            JOIN_CONSUMER_GROUP_CODE,
            [Answer::Commit(JOIN_OP)],
        );
        client
            .join_consumer_group(
                &identifier(STREAM_ID),
                &identifier(TOPIC_ID),
                &identifier(GROUP_ID),
            )
            .await
            .unwrap();
        cluster.install_owner(PartitionContext {
            incarnation: history,
            owner_generation: INSTALLED_OWNER,
            metadata_op: JOIN_OP,
        });
        // A route read after the join carries at least its op.
        if let Some(route) = cluster.route.lock().unwrap().as_mut() {
            (route.incarnation, route.metadata_op) = (history, JOIN_OP);
        }
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, EARLIER_OWNER)]));
        fed.reader.next().await.unwrap();
        fed.progress().see_owner(PARTITION, EARLIER_OWNER);
        fed.progress().lose_owners();
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );

        let sought = fed
            .reader
            .seek(PARTITION, PollingStrategy::offset(TARGET))
            .await;
        let stores = cluster
            .received(STORE_CONSUMER_OFFSET_CODE)
            .iter()
            .map(|store| {
                (
                    store.header.partition_incarnation,
                    store.header.owner_generation,
                    store.header.minimum_metadata_op,
                )
            })
            .collect();
        (sought, stores)
    }

    /// A rebalance by another member installs the owner at the op of that member, which the
    /// session watermark does not reach, so the server refuses a seek before the first read of
    /// the partition. A read without a commit shows the installed owner, and the seek goes once
    /// more under it. A skip without a history names the route history, as an offset does.
    #[tokio::test]
    async fn given_a_partition_that_a_rebalance_by_another_member_gave_when_seeking_before_the_first_read_should_store_under_the_installed_owner()
     {
        const INSTALLED_OWNER: PartitionContext = PartitionContext {
            owner_generation: 3,
            metadata_op: 9,
            ..CONTEXT
        };
        const TARGET: u64 = 50;
        for start in [
            PollingStrategy::offset(TARGET),
            PollingStrategy::continuation(
                PARTITION,
                PollingKind::Offset,
                TARGET,
                PartitionContext::default(),
            ),
        ] {
            let (client, cluster) = connect_to_one_replica().await;
            cluster.install_owner(INSTALLED_OWNER);
            let mut fed = FedReader::new(committing(builder_on(client, group())));
            fed.assign(&[PARTITION]);
            cluster.script(
                &endpoint(PRIMARY),
                GET_CONSUMER_OFFSET_CODE,
                [lookup_answer(PARTITION, None)],
            );
            cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [polled_answer_in(PARTITION, &[], INSTALLED_OWNER)],
            );

            let sought = fed.reader.seek(PARTITION, start).await;

            assert_eq!(sought, Ok(()), "{start:?}");
            assert_eq!(
                histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
                [
                    (CONTEXT.incarnation, 0),
                    (CONTEXT.incarnation, INSTALLED_OWNER.owner_generation)
                ],
                "{start:?}"
            );
        }
    }

    #[tokio::test]
    async fn given_a_message_delivered_before_a_leave_when_seeking_after_the_return_should_name_no_owner_of_it()
     {
        const EARLIER_OWNER: PartitionContext = PartitionContext {
            owner_generation: 1,
            ..CONTEXT
        };
        const TARGET: u64 = 50;
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, EARLIER_OWNER)]));
        fed.reader.next().await.unwrap();
        fed.assign(&[]);
        fed.assign(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );

        fed.reader
            .seek(PARTITION, PollingStrategy::offset(TARGET))
            .await
            .unwrap();

        let owners: Vec<u64> = histories(&cluster, STORE_CONSUMER_OFFSET_CODE)
            .into_iter()
            .map(|(_, owner)| owner)
            .collect();
        assert_eq!(
            owners,
            [0],
            "another member can own the partition since the leave"
        );
    }

    /// A partition that left and came back has a new owner, which refuses a store under the old
    /// one, and another member can have read it meanwhile. So a queued batch of the earlier
    /// assignment decides nothing at the close, and the close does not fail because of it.
    #[tokio::test]
    async fn given_a_queued_batch_of_an_earlier_assignment_when_closing_should_not_rewind_before_it()
     {
        const EARLIER_OWNER: PartitionContext = PartitionContext {
            owner_generation: 1,
            ..CONTEXT
        };
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, EARLIER_OWNER)]));
        fed.assign(&[]);
        fed.assign(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Refuse(IggyError::ConsumerGroupPartitionNotOwned(
                0, PARTITION,
            ))],
        );

        let closed = fed.reader.close().await;

        assert_eq!(closed, Ok(()));
        assert!(stored_offsets(&cluster).is_empty());
    }

    /// A read whose reply was lost can have stored past the last delivered message, so the close
    /// stores that message again. Another member can have read a partition that left and came
    /// back, so a message delivered before the leave decides nothing.
    #[tokio::test]
    async fn given_a_lost_read_before_a_leave_when_closing_after_the_return_should_not_store_the_message_delivered_before_it()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, REPLACED_OWNER)]));
        fed.reader.next().await.unwrap();
        drop(PendingCommit::new(
            Arc::clone(&fed.reader.shared.progress),
            vec![PARTITION],
        ));
        fed.assign(&[]);
        fed.assign(&[PARTITION]);

        let closed = fed.reader.close().await;

        assert_eq!(closed, Ok(()));
        assert!(stored_offsets(&cluster).is_empty());
    }

    /// The skip of a message that does not decode continues in the history of the read, which a
    /// read from an offset leaves empty. The server refuses an offset write in the empty history,
    /// so the store or the delete of a seek to that skip names the history of the route.
    #[tokio::test]
    async fn given_a_skip_without_a_history_when_seeking_to_it_should_write_in_the_history_of_the_route()
     {
        const STORED: u64 = 7;
        for (offset, stored, write) in [
            (SEEK_OFFSET, None, STORE_CONSUMER_OFFSET_CODE),
            (0, Some(STORED), DELETE_CONSUMER_OFFSET_CODE),
        ] {
            let (client, cluster) = connect_to_one_replica().await;
            let mut fed = FedReader::new(committing(builder_on(client, group())));
            fed.assign(&[PARTITION]);
            cluster.script(
                &endpoint(PRIMARY),
                GET_CONSUMER_OFFSET_CODE,
                [lookup_answer(PARTITION, stored)],
            );
            let skip = PollingStrategy::continuation(
                PARTITION,
                PollingKind::Offset,
                offset,
                PartitionContext::default(),
            );

            let sought = fed.reader.seek(PARTITION, skip).await;

            assert_eq!(sought, Ok(()), "offset {offset}");
            assert_eq!(
                histories(&cluster, write),
                [(CONTEXT.incarnation, 0)],
                "offset {offset}"
            );
        }
    }

    /// The zero context names no history, so a seek to a continuation that carries it stores in
    /// the history of the delivered message, as a seek to an offset does, and not in the history
    /// of the route.
    #[tokio::test]
    async fn given_a_delivered_message_when_seeking_to_a_continuation_with_the_zero_context_should_store_in_its_history()
     {
        for start in [
            PollingStrategy::offset(SEEK_OFFSET),
            PollingStrategy::continuation(
                PARTITION,
                PollingKind::Offset,
                SEEK_OFFSET,
                PartitionContext::default(),
            ),
        ] {
            let (client, cluster) = connect_to_one_replica().await;
            let mut fed = FedReader::new(committing(standalone_on(client, [PARTITION])));
            fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
            fed.reader.next().await.unwrap();
            if let Some(route) = cluster.route.lock().unwrap().as_mut() {
                route.incarnation = RECREATED.incarnation;
            }

            let sought = fed.reader.seek(PARTITION, start).await;

            assert_eq!(sought, Ok(()), "{start:?}");
            assert_eq!(
                histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
                [history(CONTEXT)],
                "{start:?}"
            );
        }
    }

    /// A fully zero context names no history, so after a session change a write that carries it
    /// takes the context of the route, as a write without a context does. The op of the route
    /// can show an owner that the session watermark does not reach.
    #[tokio::test(start_paused = true)]
    async fn given_a_session_change_when_closing_with_an_undecoded_start_without_a_history_should_rewind_under_the_context_of_the_route()
     {
        /// Installed by a rebalance of another member after the join of the session.
        const INSTALLED_OWNER: PartitionContext = PartitionContext {
            owner_generation: 2,
            metadata_op: JOIN_OP + 1,
            ..CONTEXT
        };
        let (client, cluster) = connect_to_one_replica().await;
        cluster.script(
            &endpoint(PRIMARY),
            JOIN_CONSUMER_GROUP_CODE,
            [Answer::Commit(JOIN_OP)],
        );
        client
            .join_consumer_group(
                &identifier(STREAM_ID),
                &identifier(TOPIC_ID),
                &identifier(GROUP_ID),
            )
            .await
            .unwrap();
        cluster.install_owner(INSTALLED_OWNER);
        if let Some(route) = cluster.route.lock().unwrap().as_mut() {
            route.metadata_op = INSTALLED_OWNER.metadata_op;
        }
        let fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        fed.progress().see_owner(PARTITION, REPLACED_OWNER);
        fed.progress().lose_owners();
        fed.progress().remember_undecoded(Checkpoint::new(
            PARTITION,
            SEEK_OFFSET,
            PartitionContext::default(),
        ));
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, Some(SEEK_OFFSET))],
        );

        let closed = fed.reader.close().await;

        assert_eq!(closed, Ok(()));
        let stores = cluster.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(stores.len(), 1);
        assert_eq!(
            (
                stores[0].header.owner_generation,
                stores[0].header.minimum_metadata_op
            ),
            (0, INSTALLED_OWNER.metadata_op)
        );
    }

    /// Another member can own a partition that left before its write, so the refusal of the
    /// store of its counted position stands without a read for the owner.
    #[tokio::test]
    async fn given_auto_commit_when_the_store_of_a_partition_that_left_is_refused_as_not_owned_should_fail_without_a_read()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed =
            FedReader::new(builder_on(client, group()).options(auto(None, CommitAfter::Delivery)));
        fed.assign(&[PARTITION]);
        fed.reader.count(
            fed.checkpoint(PARTITION, DELIVERED_OFFSET, CONTEXT),
            1,
            true,
        );
        fed.progress().retire_counted(&[PARTITION]);
        fed.assign(&[]);
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Refuse(IggyError::ConsumerGroupPartitionNotOwned(
                0, PARTITION,
            ))],
        );

        let flushed = fed.progress().flush_counted().await;

        assert!(
            matches!(flushed, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
            "{flushed:?}"
        );
        assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 0);
    }

    /// Another member can own a partition that left while its seek store was on the wire, so
    /// the refusal of that store stands without a read for the owner.
    #[tokio::test]
    async fn given_a_seek_store_on_the_wire_when_its_partition_leaves_before_the_refusal_as_not_owned_should_fail_without_a_read()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        let shared = Arc::clone(&fed.reader.shared);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Refuse(IggyError::ConsumerGroupPartitionNotOwned(
                0, PARTITION,
            ))],
        );
        cluster.hold(STORE_CONSUMER_OFFSET_CODE);
        let leave = async {
            eventually("the store is on the wire", || {
                cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
            })
            .await;
            shared.progress.assign(&[], &[]);
            shared.set_partitions(&[]);
            cluster.release(STORE_CONSUMER_OFFSET_CODE);
        };

        let (sought, ()) = tokio::join!(
            fed.reader
                .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET)),
            leave
        );

        assert!(
            matches!(sought, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
            "{sought:?}"
        );
        assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 0);
    }

    /// The read for the owner of a store refused as not owned goes again after each transient
    /// refusal, also after one of a poll over the quota of the server, until the caller of the
    /// write drops it. So it outlasts the deadline of one request.
    #[tokio::test(start_paused = true)]
    async fn given_a_refused_seek_store_when_its_read_for_the_owner_is_refused_for_now_past_an_operation_timeout_should_store_under_the_installed_owner()
     {
        for refusal in [
            IggyError::TransientNotAccepted,
            IggyError::DeferredPollCapacityExceeded,
        ] {
            let (client, cluster) = connect_to_one_replica().await;
            cluster.install_owner(CURRENT_OWNER);
            let mut fed = FedReader::new(committing(builder_on(client, group())));
            fed.assign(&[PARTITION]);
            cluster.script(
                &endpoint(PRIMARY),
                GET_CONSUMER_OFFSET_CODE,
                [lookup_answer(PARTITION, None)],
            );
            cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                repeat_n(Answer::Refuse(refusal.clone()), REFUSED_READS)
                    .chain(once(polled_answer_in(PARTITION, &[], CURRENT_OWNER))),
            );
            let started = Instant::now();

            let sought = fed
                .reader
                .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
                .await;

            assert_eq!(sought, Ok(()), "{refusal}");
            assert!(started.elapsed() > OPERATION_TIMEOUT, "{refusal}");
            assert_eq!(
                histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
                [
                    (CONTEXT.incarnation, 0),
                    (CONTEXT.incarnation, CURRENT_OWNER.owner_generation)
                ],
                "{refusal}"
            );
        }
    }

    /// The store of a seek runs in a task of its own, which the deadline in force of the caller
    /// still reaches. So that deadline ends the read for the owner of a store refused as not
    /// owned, and the refusal stands.
    #[tokio::test(start_paused = true)]
    async fn given_a_caller_deadline_when_the_read_for_the_owner_of_a_refused_seek_store_is_refused_for_now_should_fail_with_the_refusal_at_the_deadline()
     {
        /// Far shorter than the retries that the cluster refuses.
        const CALLER_BUDGET: Duration = Duration::from_secs(10);
        let (client, cluster) = connect_to_one_replica().await;
        cluster.install_owner(CURRENT_OWNER);
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );
        cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            repeat_n(
                Answer::Refuse(IggyError::TransientNotAccepted),
                REFUSED_READS,
            )
            .chain(once(polled_answer_in(PARTITION, &[], CURRENT_OWNER))),
        );
        let started = Instant::now();

        let sought = with_deadline(
            started + CALLER_BUDGET,
            fed.reader
                .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET)),
        )
        .await;

        assert!(
            matches!(sought, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
            "{sought:?}"
        );
        assert!(
            (CALLER_BUDGET..OPERATION_TIMEOUT).contains(&started.elapsed()),
            "the seek took {:?}",
            started.elapsed()
        );
        assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
    }

    /// The caller of a seek that finishes in the background is the reader, so the close ends the
    /// read for the owner of its refused store at once, and the refusal stands.
    #[tokio::test(start_paused = true)]
    async fn given_a_dropped_seek_whose_read_for_the_owner_is_refused_for_now_when_closing_should_end_that_read_at_once()
     {
        /// The seek is still reading for the owner then.
        const DROPPED_AFTER: Duration = Duration::from_secs(1);
        let (client, cluster) = connect_to_one_replica().await;
        cluster.install_owner(CURRENT_OWNER);
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );
        cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            repeat_n(
                Answer::Refuse(IggyError::TransientNotAccepted),
                REFUSED_READS,
            ),
        );
        let dropped = timeout(
            DROPPED_AFTER,
            fed.reader
                .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET)),
        )
        .await;
        assert!(dropped.is_err(), "{dropped:?}");
        assert!(cluster.count(POLL_MESSAGES_DEFERRED_CODE) > 0);
        let started = Instant::now();

        let closed = fed.reader.close().await;

        assert_eq!(closed, Ok(()));
        assert!(
            started.elapsed() < DROPPED_AFTER,
            "the close waited {:?} for the read",
            started.elapsed()
        );
        assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
    }

    /// The read for the owner takes only the context of its reply, so a message there that does
    /// not decode cannot keep the write from going once more under the owner of that reply.
    #[tokio::test]
    async fn given_a_refused_seek_store_when_its_read_for_the_owner_holds_a_message_that_does_not_decode_should_store_under_the_owner_of_the_reply()
     {
        let (client, cluster) = connect_to_one_replica().await;
        cluster.install_owner(CURRENT_OWNER);
        let mut fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION]);
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, None)],
        );
        let Answer::Body(mut reply) = polled_answer_in(PARTITION, &[SEEK_OFFSET], CURRENT_OWNER)
        else {
            unreachable!("a poll reply has a body");
        };
        // The record of the message ends early.
        reply.pop();
        cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Body(reply)],
        );

        let sought = fed
            .reader
            .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
            .await;

        assert_eq!(sought, Ok(()));
        assert_eq!(
            histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
            [(CONTEXT.incarnation, 0), history(CURRENT_OWNER)]
        );
        assert_eq!(
            cluster.received(STORE_CONSUMER_OFFSET_CODE)[1]
                .header
                .minimum_metadata_op,
            CURRENT_OWNER.metadata_op
        );
    }

    /// The end of the close budget ends the read for the owner of a rewind refused as not owned,
    /// and that refusal stands. A rewind that did not go yet still goes, under its own request
    /// timeout.
    #[tokio::test(start_paused = true)]
    async fn given_a_refused_rewind_whose_read_for_the_owner_is_refused_for_now_past_the_close_budget_when_closing_should_still_rewind_the_next_partition()
     {
        const NEXT_BATCH: RangeInclusive<u64> = SEEK_OFFSET..=SEEK_OFFSET + 1;
        let (client, cluster) = connect_to_one_replica().await;
        cluster.install_owner(CURRENT_OWNER);
        let fed = FedReader::new(committing(builder_on(client, group())));
        fed.assign(&[PARTITION, OTHER_PARTITION]);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, REPLACED_OWNER)]));
        fed.send(Ok(vec![fed.batch(
            OTHER_PARTITION,
            NEXT_BATCH,
            CURRENT_OWNER,
        )]));
        cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            repeat_n(
                Answer::Refuse(IggyError::TransientNotAccepted),
                REFUSED_READS,
            ),
        );
        let started = Instant::now();

        let closed = fed.reader.close().await;

        assert!(
            matches!(closed, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
            "{closed:?}"
        );
        assert!(
            (CLOSE_BUDGET..OPERATION_TIMEOUT).contains(&started.elapsed()),
            "the close took {:?}",
            started.elapsed()
        );
        assert_eq!(
            stored_offsets(&cluster),
            [BATCH.start() - 1, NEXT_BATCH.start() - 1]
        );
        assert_eq!(
            histories(&cluster, STORE_CONSUMER_OFFSET_CODE),
            [history(REPLACED_OWNER), history(CURRENT_OWNER)]
        );
    }

    /// The close start ends the retries of a write of the reader itself. Its read for the owner
    /// in flight then can still be answered, but the answer leads to no write, and the refusal
    /// stands.
    #[tokio::test(start_paused = true)]
    async fn given_a_background_write_whose_read_for_the_owner_is_in_flight_when_the_close_starts_should_not_write_again()
     {
        /// The answer and the close start are both there when the write runs again, so a choice
        /// between them that is left to chance fails in some round.
        const ROUNDS: usize = 16;
        for _ in 0..ROUNDS {
            let (client, cluster) = connect_to_one_replica().await;
            cluster.install_owner(CURRENT_OWNER);
            let fed = FedReader::new(committing(builder_on(client, group())));
            fed.assign(&[PARTITION]);
            cluster.script(
                &endpoint(PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [polled_answer_in(PARTITION, &[], CURRENT_OWNER)],
            );
            cluster.hold(POLL_MESSAGES_DEFERRED_CODE);
            let mut write = pin!(in_background(fed.progress().store(fed.checkpoint(
                PARTITION,
                DELIVERED_OFFSET,
                REPLACED_OWNER
            ))));
            tokio::select! {
                written = &mut write => panic!("the write ended before its read: {written:?}"),
                () = eventually("the read for the owner is on the wire", || {
                    cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 1
                }) => {}
            }
            cluster.release(POLL_MESSAGES_DEFERRED_CODE);
            // The answer arrives while nothing runs the write.
            sleep(CANCEL_AFTER).await;
            fed.progress().stop_background();

            let written = write.await;

            assert!(
                matches!(written, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
                "{written:?}"
            );
            assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
        }
    }

    #[tokio::test]
    async fn given_a_reply_of_several_partitions_when_reading_should_hand_over_every_batch_in_reply_order()
     {
        let replied = [OTHER_PARTITION, THIRD_PARTITION, PARTITION];
        let mut fed = FedReader::new(standalone(replied));
        let free = fed.slots.available_permits();
        fed.send(Ok(replied
            .iter()
            .map(|&partition| fed.batch(partition, BATCH, CONTEXT))
            .collect()));

        for partition in replied {
            assert_eq!(
                fed.slots.available_permits(),
                free - 1,
                "the reply holds its slot until its last batch leaves"
            );
            assert_eq!(fed.reader.next().await.unwrap().partition_id, partition);
        }
        assert_eq!(fed.slots.available_permits(), free);
    }

    #[tokio::test]
    async fn given_a_seek_when_reading_should_drop_the_queued_and_late_batches_of_the_old_generation()
     {
        let mut fed = FedReader::new(standalone([PARTITION, OTHER_PARTITION]));
        let free = fed.slots.available_permits();
        let late = fed.batch(PARTITION, BATCH, CONTEXT);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        fed.send(Ok(vec![fed.batch(OTHER_PARTITION, BATCH, CONTEXT)]));

        fed.reader
            .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
            .await
            .unwrap();
        assert_eq!(
            fed.slots.available_permits(),
            free - 1,
            "the dropped batch returns its slot"
        );
        fed.send(Ok(vec![late]));
        fed.send(Ok(vec![fed.batch(
            PARTITION,
            SEEK_OFFSET..=SEEK_OFFSET,
            CONTEXT,
        )]));

        assert_eq!(
            partition_and_offsets(&fed.reader.next().await.unwrap()),
            (OTHER_PARTITION, BATCH.collect())
        );
        assert_eq!(
            partition_and_offsets(&fed.reader.next().await.unwrap()),
            (PARTITION, vec![SEEK_OFFSET])
        );
        assert_eq!(fed.slots.available_permits(), free);
    }

    #[tokio::test]
    async fn given_a_held_batch_when_a_seek_or_an_assignment_starts_a_new_generation_should_stop_counting_it()
     {
        for reassign in [false, true] {
            let mut fed = FedReader::new(builder_for(group()));
            fed.assign(&[PARTITION, OTHER_PARTITION]);
            fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
            let (_, mut hold) = poll_fn(|cx| fed.reader.poll_held(cx)).await.unwrap();
            fed.progress()
                .seek(OTHER_PARTITION, PollingStrategy::offset(SEEK_OFFSET));
            assert!(
                hold.is_current(),
                "a new generation of another partition keeps the batch"
            );

            if reassign {
                fed.assign(&[OTHER_PARTITION]);
            } else {
                fed.progress()
                    .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET));
            }

            assert!(!hold.is_current(), "reassign: {reassign}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_on_read_when_a_partition_leaves_with_an_undelivered_batch_should_still_hand_it_over()
     {
        let mut fed = FedReader::new(committing(builder_for(group())));
        fed.assign(&[PARTITION, OTHER_PARTITION]);
        fed.send(Ok(vec![
            fed.batch(OTHER_PARTITION, BATCH, CONTEXT),
            fed.batch(PARTITION, BATCH, CONTEXT),
        ]));
        assert_eq!(
            fed.reader.next().await.unwrap().partition_id,
            OTHER_PARTITION
        );

        fed.assign(&[OTHER_PARTITION]);

        let batch = timeout(CANCEL_AFTER, fed.reader.next())
            .await
            .expect("the server stored the batch of the partition that left")
            .unwrap();
        assert_eq!(partition_and_offsets(&batch), (PARTITION, BATCH.collect()));
    }

    #[tokio::test(start_paused = true)]
    async fn given_on_read_when_a_new_session_keeps_the_partition_should_still_hand_over_its_undelivered_batch()
     {
        // A new session starts a new generation of every partition, and the batch
        // that the server stored under the old one was dropped.
        let mut fed = FedReader::new(committing(builder_for(group())));
        fed.assign(&[PARTITION, OTHER_PARTITION]);
        fed.send(Ok(vec![
            fed.batch(OTHER_PARTITION, BATCH, CONTEXT),
            fed.batch(PARTITION, BATCH, CONTEXT),
        ]));
        assert_eq!(
            fed.reader.next().await.unwrap().partition_id,
            OTHER_PARTITION
        );

        fed.assign(&[PARTITION, OTHER_PARTITION]);

        let batch = timeout(CANCEL_AFTER, fed.reader.next())
            .await
            .expect("the server stored the batch under the old session")
            .unwrap();
        assert_eq!(partition_and_offsets(&batch), (PARTITION, BATCH.collect()));
    }

    #[tokio::test(start_paused = true)]
    async fn given_on_read_when_a_seek_follows_an_undelivered_batch_should_drop_it() {
        let mut fed = FedReader::new(committing(builder_for(group())));
        fed.assign(&[PARTITION, OTHER_PARTITION]);
        fed.send(Ok(vec![
            fed.batch(OTHER_PARTITION, BATCH, CONTEXT),
            fed.batch(PARTITION, BATCH, CONTEXT),
        ]));
        assert_eq!(
            fed.reader.next().await.unwrap().partition_id,
            OTHER_PARTITION
        );

        fed.progress()
            .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET));

        assert!(
            timeout(CANCEL_AFTER, fed.reader.next()).await.is_err(),
            "a seek reads from its own start, not what came before it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_cancelled_read_when_a_reply_arrives_should_hand_over_the_whole_batch() {
        let mut fed = FedReader::new(standalone([PARTITION]));
        assert!(timeout(CANCEL_AFTER, fed.reader.next()).await.is_err());

        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));

        assert_eq!(
            partition_and_offsets(&fed.reader.next().await.unwrap()),
            (PARTITION, BATCH.collect())
        );
    }

    #[tokio::test]
    async fn given_a_healthy_batch_beside_a_failed_partition_when_reading_should_return_the_batch_then_the_error()
     {
        let mut fed = FedReader::new(standalone([PARTITION, OTHER_PARTITION]));
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        fed.send(Err(
            fed.refusal(OTHER_PARTITION, IggyError::HistoryUnavailable)
        ));

        assert_eq!(fed.reader.next().await.unwrap().partition_id, PARTITION);
        assert_eq!(
            fed.reader.next().await.unwrap_err(),
            ReadError::new(Some(OTHER_PARTITION), IggyError::HistoryUnavailable)
        );
    }

    #[tokio::test]
    async fn given_errors_of_an_older_generation_or_epoch_when_reading_should_drop_them() {
        let mut fed = FedReader::new(standalone([PARTITION]));
        let old_generation = fed.refusal(PARTITION, IggyError::HistoryUnavailable);
        fed.reader
            .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
            .await
            .unwrap();
        let old_epoch = fed.reply(Err(reader_refusal(IggyError::Disconnected)));
        fed.reader.shared.epoch.fetch_add(1, ORDERING);

        fed.send(Err(old_generation));
        fed.sender.send(old_epoch).unwrap();
        fed.send(Err(reader_refusal(IggyError::StaleClient)));

        assert_eq!(
            fed.reader.next().await.unwrap_err(),
            ReadError::new(None, IggyError::StaleClient)
        );
    }

    #[tokio::test]
    async fn given_a_message_that_does_not_decode_when_reading_should_return_its_prefix_then_a_skip_to_seek()
     {
        let mut fed = FedReader::new(standalone([PARTITION]));
        let undecoded = BATCH.end() + 1;
        let skip =
            PollingStrategy::continuation(PARTITION, PollingKind::Offset, undecoded + 1, CONTEXT);
        fed.send(Ok(vec![fed.batch(PARTITION, BATCH, CONTEXT)]));
        fed.send(Err(Refusal {
            error: ReadError {
                partition_id: Some(PARTITION),
                error: IggyError::CannotDecryptData,
                skip: Some(skip),
                offset: None,
                record: None,
                lost: None,
            },
            generation: fed.progress().generation(PARTITION),
        }));

        assert_eq!(
            partition_and_offsets(&fed.reader.next().await.unwrap()),
            (PARTITION, BATCH.collect())
        );
        assert_eq!(fed.reader.next().await.unwrap_err().skip, Some(skip));
        fed.reader.seek(PARTITION, skip).await.unwrap();
        assert_eq!(
            fed.progress().take_seeks(),
            [(
                PARTITION,
                skip,
                fed.progress().generation(PARTITION).unwrap()
            )]
        );
    }

    #[tokio::test]
    async fn given_a_partition_the_reader_does_not_read_when_seeking_should_refuse_it() {
        let mut fed = FedReader::new(standalone([PARTITION]));
        let generation = fed.progress().generation(PARTITION);
        let continuation_of_other = PollingStrategy::continuation(
            OTHER_PARTITION,
            PollingKind::Offset,
            SEEK_OFFSET,
            CONTEXT,
        );

        assert_eq!(
            fed.reader
                .seek(OTHER_PARTITION, PollingStrategy::next())
                .await,
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            fed.reader.seek(PARTITION, continuation_of_other).await,
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(fed.progress().generation(PARTITION), generation);
    }

    #[test]
    fn given_a_lost_range_when_displaying_a_read_error_should_print_its_first_and_last_offset() {
        const OPERATION: u64 = 1;
        let mut error = ReadError::new(
            Some(PARTITION),
            IggyError::FetchProgressUncertain(OPERATION),
        );
        let without_range = error.to_string();
        error.lost = Some(BATCH);

        assert_eq!(
            error.to_string(),
            format!(
                "{without_range}, lost offsets: {} to {}",
                BATCH.start(),
                BATCH.end()
            )
        );
    }

    #[test]
    fn given_an_invalid_configuration_when_building_should_refuse_it() {
        let continuation =
            PollingStrategy::continuation(PARTITION, PollingKind::Offset, SEEK_OFFSET, CONTEXT);
        let no_messages = PollOptions {
            count: 0,
            ..PollOptions::default()
        };

        for (builder, error) in [
            (
                builder_for(Consumer::external_group(identifier(GROUP_ID))),
                IggyError::FeatureUnavailable,
            ),
            (
                builder_for(group()).partitions([PARTITION]),
                IggyError::InvalidCommand,
            ),
            (builder_for(consumer()), IggyError::InvalidCommand),
            (standalone([]), IggyError::InvalidCommand),
            (
                standalone([PARTITION]).start(continuation),
                IggyError::InvalidCommand,
            ),
            (
                standalone([PARTITION]).options(no_messages),
                IggyError::InvalidMessagesCount,
            ),
        ] {
            let described = format!("{builder:?}");
            assert_eq!(builder.into_parts().err(), Some(error), "{described}");
        }
    }

    #[test]
    fn given_an_encryptor_when_building_should_refuse_only_a_server_commit_on_read() {
        let encrypted = || {
            let encryptor = EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&KEY).unwrap());
            builder_with(
                ClientWrapper::Tcp(TcpClient::default()),
                consumer(),
                Some(Arc::new(encryptor)),
            )
            .partitions([PARTITION])
        };

        assert_eq!(
            committing(encrypted()).into_parts().err(),
            Some(IggyError::InvalidConfiguration)
        );
        assert!(encrypted().into_parts().is_ok());
    }

    /// The zero context names no history, so a start that carries it starts each partition as
    /// the same start without it.
    #[test]
    fn given_a_start_with_the_zero_context_when_building_should_drop_the_context() {
        let start: PollingStrategy = serde_json::from_value(serde_json::json!({
            "kind": "next",
            "context": serde_json::to_string(&PartitionContext::default()).unwrap(),
        }))
        .unwrap();

        let (config, _) = standalone([PARTITION]).start(start).into_parts().unwrap();

        assert_eq!(config.start, PollingStrategy::next());
    }

    #[test]
    fn given_duplicate_partitions_when_building_should_read_each_once_in_order() {
        let fed = FedReader::new(standalone([OTHER_PARTITION, PARTITION, OTHER_PARTITION]));

        assert_eq!(fed.reader.partitions(), [PARTITION, OTHER_PARTITION]);
    }

    #[tokio::test]
    async fn given_a_group_reader_on_http_when_building_should_refuse_it() {
        let client = ClientWrapper::Http(HttpClient::new(UNREACHABLE_API_URL).unwrap());

        let built = builder_with(client, group(), None).build().await;

        assert_eq!(built.err(), Some(IggyError::FeatureUnavailable));
    }

    #[test]
    fn given_a_count_above_the_default_prefetch_when_building_should_keep_the_minimum_batch_budget()
    {
        const MINIMUM_SLOTS: usize = 32;
        let long_batches = PollOptions {
            count: DEFAULT_PREFETCH_MESSAGES + 1,
            ..PollOptions::default()
        };

        for builder in [
            standalone([PARTITION]).options(long_batches),
            standalone([PARTITION])
                .options(long_batches)
                .prefetch_messages(DEFAULT_PREFETCH_MESSAGES),
        ] {
            let described = format!("{builder:?}");
            let (config, _) = builder.into_parts().unwrap();
            assert_eq!(config.slots, MINIMUM_SLOTS, "{described}");
        }
    }

    #[test]
    fn given_a_larger_prefetch_budget_when_building_should_preserve_room_beyond_the_request_limit()
    {
        const BATCHES: u32 = 64;
        let options = PollOptions::default();
        let (config, _) = standalone([PARTITION])
            .options(options)
            .prefetch_messages(options.count * BATCHES)
            .prefetch_bytes(options.max_bytes * BATCHES)
            .into_parts()
            .unwrap();

        assert_eq!(config.slots, BATCHES as usize);
    }

    /// A reader without its fetch loop. The test sends the replies of the loop, each with a slot
    /// of `slots`. `sender` stays open, because a closed channel starts a real fetch loop.
    struct FedReader {
        reader: PartitionReader,
        sender: mpsc::UnboundedSender<Reply>,
        slots: Arc<Semaphore>,
    }

    impl FedReader {
        fn new(builder: ReaderBuilder) -> Self {
            let (config, shared) = builder.into_parts().unwrap();
            // The fetch loop of a standalone reader starts its generations at once.
            shared.progress.assign(&config.partitions, &[]);
            let (sender, receiver) = mpsc::unbounded_channel();
            let slots = Arc::new(Semaphore::new(config.slots));
            let config = Arc::new(config);
            let shared = Arc::new(shared);
            let committer = config
                .auto()
                .map(|auto| Committer::spawn(Arc::clone(&shared.progress), auto.interval));
            let reader = PartitionReader {
                config,
                shared,
                receiver,
                queue: VecDeque::new(),
                task: None,
                uncounted: None,
                committer,
            };
            Self {
                reader,
                sender,
                slots,
            }
        }

        /// Assigns `partitions` to a group member, as a refresh of its fetch loop does.
        fn assign(&self, partitions: &[u32]) {
            self.progress().assign(partitions, &[]);
            self.reader.shared.set_partitions(partitions);
        }

        fn progress(&self) -> &Progress {
            &self.reader.shared.progress
        }

        fn stamp(&self, partition: u32) -> ReaderStamp {
            let progress = self.progress();
            progress.stamp(progress.generation(partition).unwrap())
        }

        /// A checkpoint of the current generation of `partition`.
        fn checkpoint(&self, partition: u32, offset: u64, context: PartitionContext) -> Checkpoint {
            Checkpoint::stamped(partition, offset, context, Some(self.stamp(partition)))
        }

        /// A batch of the current generation of `partition` with one message at each offset.
        fn batch(
            &self,
            partition: u32,
            offsets: RangeInclusive<u64>,
            context: PartitionContext,
        ) -> PolledMessages {
            let current_offset = *offsets.end();
            let messages: Vec<_> = offsets.map(message_at).collect();
            let mut batch = PolledMessages::new(
                context,
                partition,
                current_offset,
                u32::try_from(messages.len()).unwrap(),
                messages,
            );
            batch.set_stamp(self.stamp(partition));
            batch
        }

        /// A refusal of `partition` in its current generation.
        fn refusal(&self, partition: u32, error: IggyError) -> Refusal {
            Refusal {
                error: ReadError::new(Some(partition), error),
                generation: self.progress().generation(partition),
            }
        }

        /// A reply of the current epoch that holds a slot.
        fn reply(&self, result: Result<Vec<PolledMessages>, Refusal>) -> Reply {
            Reply {
                epoch: self.reader.shared.epoch.load(ORDERING),
                assignment: None,
                result,
                capacity: Arc::new(FetchCapacity {
                    permit: Some(Arc::clone(&self.slots).try_acquire_owned().unwrap()),
                    wake: Arc::clone(&self.reader.shared.progress.wake),
                }),
            }
        }

        fn send(&self, result: Result<Vec<PolledMessages>, Refusal>) {
            self.sender.send(self.reply(result)).unwrap();
        }
    }

    fn reader_refusal(error: IggyError) -> Refusal {
        Refusal {
            error: ReadError::new(None, error),
            generation: None,
        }
    }

    fn consumer() -> Consumer {
        Consumer::new(identifier(CONSUMER_ID))
    }

    /// A standalone reader of `partitions` on a client that never connects.
    /// `CommitPolicy::Auto` that stores on `every` and at the close, never on an interval.
    fn auto(every: Option<CommitEvery>, after: CommitAfter) -> PollOptions {
        let interval = (every.is_none()).then_some(NonZeroIggyDuration::new(NEVER).unwrap());
        PollOptions {
            commit: CommitPolicy::Auto(AutoCommit {
                interval,
                every,
                after,
            }),
            ..PollOptions::default()
        }
    }

    fn standalone(partitions: impl IntoIterator<Item = u32>) -> ReaderBuilder {
        builder_for(consumer()).partitions(partitions)
    }

    fn standalone_on(
        client: TcpClient,
        partitions: impl IntoIterator<Item = u32>,
    ) -> ReaderBuilder {
        builder_on(client, consumer()).partitions(partitions)
    }

    /// A reader on `client`, as `IggyClient::reader` builds it.
    fn builder_with(
        client: ClientWrapper,
        consumer: Consumer,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> ReaderBuilder {
        ReaderBuilder::new(
            IggyRwLock::new(client),
            identifier(STREAM_ID),
            identifier(TOPIC_ID),
            consumer,
            encryptor,
        )
    }

    /// Loses the reply of a committing poll of `partition`, so the client of `fed` retains its
    /// operation, and blocks the partition with it, as the fetch loop does.
    async fn lose_committing_poll(fed: &FedReader, cluster: &FakeCluster, partition: u32) -> u64 {
        cluster.script(
            &endpoint(PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Silence],
        );
        let config = &fed.reader.config;
        let lost = config
            .client
            .read()
            .await
            .poll_messages(
                &config.stream,
                &config.topic,
                partition,
                &config.consumer,
                &PollingStrategy::next(),
                PollOptions {
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await;
        let operation = cluster
            .received(POLL_MESSAGES_DEFERRED_CODE)
            .last()
            .unwrap()
            .header
            .request;
        assert_eq!(
            lost.unwrap_err(),
            IggyError::FetchProgressUncertain(operation)
        );
        fed.progress().block(partition, operation);
        operation
    }

    fn deleted_partitions(cluster: &FakeCluster) -> Vec<Option<u32>> {
        cluster
            .received(DELETE_CONSUMER_OFFSET_CODE)
            .iter()
            .map(|delete| {
                DeleteConsumerOffsetRequest::decode_from(&delete.body)
                    .unwrap()
                    .partition_id
            })
            .collect()
    }

    /// The history that each request of `code` names.
    fn histories(cluster: &FakeCluster, code: u32) -> Vec<(u64, u64)> {
        cluster
            .received(code)
            .iter()
            .map(|request| {
                (
                    request.header.partition_incarnation,
                    request.header.owner_generation,
                )
            })
            .collect()
    }

    fn history(context: PartitionContext) -> (u64, u64) {
        (context.incarnation, context.owner_generation)
    }
}
