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
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use async_broadcast::Receiver;
use iggy_common::locking::IggyRwLock;
use iggy_common::{
    AssignmentVersion, Checkpoint, Consumer, ConsumerGroupClientState, ConsumerKind,
    DiagnosticEvent, EncryptorKind, Identifier, IggyError, PollOptions, PolledMessages,
    PollingKind, PollingStrategy, ServerCommitPolicy,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::driver::with_deadline;
use crate::links::OPERATION_TIMEOUT;
use membership::Membership;
use progress::Progress;
use worker::Worker;

#[cfg(test)]
mod cluster_tests;
#[cfg(test)]
pub(crate) mod fixtures;
pub(crate) mod membership;
pub(crate) mod progress;
mod worker;

const ORDERING: Ordering = Ordering::SeqCst;

/// Reads the partitions of a topic in batches, with routing, prefetch and the positions of each
/// partition inside. Build one with [`IggyClient::reader`](crate::prelude::IggyClient::reader).
///
/// - A standalone consumer reads the partitions of its builder. A consumer group reads the
///   assignment of its member, and the reader joins the group, joins again after a session loss
///   and leaves it in [`close`](Self::close).
/// - [`next`](Self::next) returns the next batch of one partition, in offset order within one
///   generation of the partition: from its first read, from each [`seek`](Self::seek), and from
///   each new assignment to this member.
/// - Without `ServerCommitPolicy::OnRead`, [`commit`](Self::commit) stores the offset of a
///   checkpoint. With it, the server stores the offset of each reply, and [`close`](Self::close)
///   moves the offset back to the last delivered message.
/// - The reader stays usable after a [`ReadError`] and retries by itself. `HistoryUnavailable`
///   stops its partition until a `seek`. With `OnRead`, an unknown outcome stops its partition
///   until the caller abandons the operation and seeks.
/// - Dropping the reader stops its tasks without waiting, and it moves no offset back.
pub struct PartitionReader {
    config: Arc<Config>,
    shared: Arc<Shared>,
    receiver: mpsc::Receiver<Reply>,
    /// The batches of the received replies, one partition each, in arrival order.
    queue: VecDeque<Entry>,
    task: Option<JoinHandle<()>>,
}

/// An error of a read. The reader stays usable after it and retries by itself.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadError {
    /// The partition of the error. `None` for an error of the whole reader.
    pub partition_id: Option<u32>,
    pub error: IggyError,
    /// For a message that does not decode: the continuation after it. The reader reads the
    /// message again until a [`seek`](PartitionReader::seek) to this continuation skips it. The
    /// messages before it arrive in a batch before this error.
    pub skip: Option<PollingStrategy>,
}

/// The fixed configuration of a reader.
pub(crate) struct Config {
    pub(super) client: IggyRwLock<ClientWrapper>,
    pub(super) stream: Arc<Identifier>,
    pub(super) topic: Arc<Identifier>,
    pub(super) consumer: Arc<Consumer>,
    /// The partitions of a standalone consumer. Empty for a group.
    pub(super) partitions: Vec<u32>,
    /// The start of a partition without a position. `First` is `Offset(0)`.
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
    pub(super) wake: Arc<Notify>,
    /// With `OnRead`: a `next()` waits, and the queue and the channel are empty.
    pub(super) demand: AtomicBool,
    pub(super) closing: AtomicBool,
    pub(super) can_poll: AtomicBool,
    /// The partitions that the reader reads, for `partitions()` and `seek`.
    pub(super) partitions: Mutex<Vec<u32>>,
    /// Shared with `IggyConsumer`, which keeps an uncertain create or join across builds.
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
    capacity: FetchCapacity,
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
        Self {
            config,
            shared,
            receiver,
            queue: VecDeque::new(),
            task: Some(task),
        }
    }

    /// The next batch of one partition. Never empty, and it has no time limit. Cancel safe: a
    /// dropped call leaves its batch in the reader.
    // The design fixes this public signature. An error is rare next to the batches, so its
    // size does not matter.
    #[allow(clippy::result_large_err)]
    pub async fn next(&mut self) -> Result<PolledMessages, ReadError> {
        let (batch, hold) = poll_fn(|cx| self.poll_held(cx)).await?;
        if let Some(checkpoint) = batch.checkpoint() {
            self.delivered(&checkpoint);
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
                        self.shared.wake.notify_one();
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

    /// Stores the offset of a checkpoint that this reader delivered. The commits of one
    /// partition go out in order when the caller awaits each one.
    ///
    /// A checkpoint of an older generation of its partition, or at or below the last commit of
    /// its generation, stores nothing.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] with `ServerCommitPolicy::OnRead`, and for a checkpoint
    /// that this reader did not deliver. The errors of the offset store.
    pub async fn commit(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        if self.config.on_read() {
            return Err(IggyError::InvalidCommand);
        }
        self.shared.progress.commit(checkpoint).await
    }

    /// Sets the next read position of `partition` and starts a new generation of it. It stores
    /// no offset, drops the queued batches of the partition and ignores its late replies. It
    /// also resumes a stopped partition.
    ///
    /// With `OnRead`, the new position waits for the end of the committing read in flight.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidCommand`] for a partition that the reader does not read, and for a
    ///   continuation of another partition. A group reader reads no partition until the first
    ///   refresh of its assignment after `build()`, so it refuses every partition before it.
    /// - [`IggyError::PartitionLaneBlocked`] with `OnRead` while a read of the partition has an
    ///   unknown outcome. Call `abandon_operation` with its operation first.
    pub async fn seek(&mut self, partition: u32, start: PollingStrategy) -> Result<(), IggyError> {
        start.validate_partition(partition)?;
        if !self.shared.partitions().contains(&partition) {
            return Err(IggyError::InvalidCommand);
        }
        let progress = &self.shared.progress;
        progress.unblock_abandoned().await;
        if self.config.on_read()
            && let Some(operation) = progress.blocked(partition)
        {
            return Err(IggyError::PartitionLaneBlocked(partition, operation));
        }
        while let Ok(reply) = self.receiver.try_recv() {
            self.split(reply);
        }
        self.queue
            .retain(|entry| entry.partition() != Some(partition));
        self.shared.progress.seek(partition, start);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// The partitions that the reader reads: those of a standalone consumer, or the current
    /// assignment of a group member. A group reader holds no partition until the first refresh
    /// of its assignment after `build()`.
    pub fn partitions(&self) -> Vec<u32> {
        self.shared.partitions()
    }

    /// Stops the reader. It waits at most until the deadlines of the reads in flight, and it
    /// commits nothing.
    ///
    /// With `OnRead`, it moves the offset of each partition with undelivered batches back to its
    /// last delivered message. A partition that delivered nothing and started at `Next` gets the
    /// offset before its first undelivered message, or no offset at offset 0. It leaves the offset
    /// of a partition alone while a read of it has an unknown outcome that the caller did not
    /// abandon. A group reader then leaves its group.
    ///
    /// # Errors
    ///
    /// The first failed store of the `OnRead` close. Otherwise
    /// [`IggyError::PartitionLaneBlocked`] for a partition with an unknown outcome: abandon its
    /// operation on the client before a new reader reads the partition.
    pub async fn close(mut self) -> Result<(), IggyError> {
        self.shared.closing.store(true, ORDERING);
        self.shared.wake.notify_one();
        if let Some(task) = self.task.take() {
            // A worker that failed already ended, which is all that the close waits for.
            let _ = task.await;
        }
        while let Ok(reply) = self.receiver.try_recv() {
            self.split(reply);
        }
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
            let resumes_next = self.config.start.kind == PollingKind::Next;
            result = with_deadline(
                Instant::now() + OPERATION_TIMEOUT,
                self.shared
                    .progress
                    .rewind(undelivered, owned.as_deref(), resumes_next),
            )
            .await;
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
        let capacity = Arc::new(capacity);
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
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl ReadError {
    pub(crate) fn new(partition_id: Option<u32>, error: IggyError) -> Self {
        Self {
            partition_id,
            error,
            skip: None,
        }
    }
}

impl Display for ReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.partition_id {
            Some(partition_id) => write!(f, "partition ID: {partition_id}: {}", self.error),
            None => Display::fmt(&self.error, f),
        }
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
        self.options.server_commit == ServerCommitPolicy::OnRead
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
    fn partition(&self) -> Option<u32> {
        match &self.result {
            Ok(batch) => Some(batch.partition_id),
            Err(refusal) => refusal.error.partition_id,
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

fn spawn_worker(
    config: &Arc<Config>,
    shared: &Arc<Shared>,
    events: Option<Receiver<DiagnosticEvent>>,
) -> (mpsc::Receiver<Reply>, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(config.slots);
    let worker = Worker::new(Arc::clone(config), Arc::clone(shared), sender, events);
    (receiver, tokio::spawn(worker.run()))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::poll_fn;
    use std::ops::RangeInclusive;
    use std::sync::Arc;
    use std::time::Duration;

    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::codes::{
        DELETE_CONSUMER_OFFSET_CODE, POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::primitives::partition_history::PartitionContext;
    use iggy_binary_protocol::requests::consumer_offsets::DeleteConsumerOffsetRequest;
    use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
    use iggy_common::{
        Aes256GcmEncryptor, Checkpoint, Client, Consumer, EncryptorKind, IggyError, MessageClient,
        PollOptions, PolledMessages, PollingKind, PollingStrategy, ReaderStamp, ServerCommitPolicy,
    };
    use tokio::sync::{Semaphore, mpsc};
    use tokio::time::{Instant, timeout};

    use super::fixtures::{
        CONSUMER_ID, CONTEXT, GROUP_ID, PRIMARY, STREAM_ID, TOPIC_ID, builder_for, builder_on,
        committing, connect_to_one_replica, group, identifier, message_at, partition_and_offsets,
        stored_offsets,
    };
    use super::progress::Progress;
    use super::{FetchCapacity, ORDERING, PartitionReader, ReadError, Refusal, Reply};
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::reader_builder::{DEFAULT_PREFETCH_MESSAGES, ReaderBuilder};
    use crate::driver::with_deadline;
    use crate::http::http_client::HttpClient;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::fake_cluster::{Answer, FakeCluster, endpoint};
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

    #[tokio::test(start_paused = true)]
    async fn given_an_unconfirmed_commit_when_committing_the_same_checkpoint_should_resolve_it_and_store()
     {
        for cancel in [false, true] {
            let (client, cluster) = connect_to_one_replica().await;
            let fed = FedReader::new(standalone_on(client, [PARTITION]));
            let checkpoint = fed.checkpoint(PARTITION, STORED_OFFSET, CONTEXT);
            cluster.script(
                &endpoint(PRIMARY),
                STORE_CONSUMER_OFFSET_CODE,
                [Answer::Silence, Answer::Commit(SECOND_COMMIT)],
            );

            if cancel {
                assert!(
                    timeout(CANCEL_AFTER, fed.reader.commit(checkpoint))
                        .await
                        .is_err()
                );
            } else {
                assert!(matches!(
                    fed.reader.commit(checkpoint).await,
                    Err(IggyError::OperationUncertain(_))
                ));
            }
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
    async fn given_an_uncertain_commit_when_the_blocker_is_rejected_should_store_the_new_checkpoint()
     {
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
        let first = fed
            .reader
            .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
            .await;
        let old = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(first, Err(IggyError::OperationUncertain(old)));

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
    async fn given_a_still_uncertain_commit_when_committing_again_should_leave_the_new_checkpoint_unsent()
     {
        let (client, cluster) = connect_to_one_replica().await;
        let fed = FedReader::new(standalone_on(client, [PARTITION]));
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Silence],
        );
        assert!(matches!(
            fed.reader
                .commit(fed.checkpoint(PARTITION, LOWER_OFFSET, CONTEXT))
                .await,
            Err(IggyError::OperationUncertain(_))
        ));
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
        fed.sender.try_send(old_epoch).unwrap();
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
    fn given_a_count_above_the_default_prefetch_when_building_should_fit_one_reply() {
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
            assert_eq!(config.slots, 1, "{described}");
        }
    }

    /// A reader without its fetch loop. The test sends the replies of the loop, each with a slot
    /// of `slots`. `sender` stays open, because a closed channel starts a real fetch loop.
    struct FedReader {
        reader: PartitionReader,
        sender: mpsc::Sender<Reply>,
        slots: Arc<Semaphore>,
    }

    impl FedReader {
        fn new(builder: ReaderBuilder) -> Self {
            let (config, shared) = builder.into_parts().unwrap();
            // The fetch loop of a standalone reader starts its generations at once.
            shared.progress.assign(&config.partitions, &[]);
            let (sender, receiver) = mpsc::channel(config.slots);
            let slots = Arc::new(Semaphore::new(config.slots));
            let reader = PartitionReader {
                config: Arc::new(config),
                shared: Arc::new(shared),
                receiver,
                queue: VecDeque::new(),
                task: None,
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
                capacity: FetchCapacity {
                    permit: Some(Arc::clone(&self.slots).try_acquire_owned().unwrap()),
                    wake: Arc::clone(&self.reader.shared.wake),
                },
            }
        }

        fn send(&self, result: Result<Vec<PolledMessages>, Refusal>) {
            self.sender.try_send(self.reply(result)).unwrap();
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
                    server_commit: ServerCommitPolicy::OnRead,
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
