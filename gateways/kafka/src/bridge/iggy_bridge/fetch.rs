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

//! Fetch-side bridge calls.
//!
//! Separate from `produce.rs` so the two never edit one file.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use iggy::prelude::{
    Client, Consumer, IggyClient, IggyError, MIN_DEFERRED_POLL_MAX_BYTES, MessageClient, Partition,
    PollOptions, PolledMessages, PollingStrategy, TopicClient,
};
use iggy_common::with_binary_deadline;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout_at};

use super::offsets::high_watermark;
use super::{
    IggyBridge, REQUEST_TIMEOUT, SDK_STOP_ROOM, TopicTarget, connect_client, permit_by,
    sign_out_and_shut_down,
};
use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

/// The byte cap of a poll of one message: the smallest Iggy takes. The first message comes
/// whatever its size, and a larger cap only makes its owner reserve more read budget.
#[allow(clippy::cast_possible_truncation)]
pub const ONE_MESSAGE_MAX_BYTES: u32 = MIN_DEFERRED_POLL_MAX_BYTES as u32;

/// Re-checks that run at once on one slot's client. With the polls of the request that holds the
/// slot, they stay well below the 64 requests that the SDK keeps in flight per client. A poll past
/// those would wait for a place and could run out of its time.
const RECHECKS_PER_SLOT: usize = 32;

/// The Fetch read slots, with a client for each. A slot reads for one request at a time.
pub(super) struct FetchPool {
    permits: Arc<Semaphore>,
    /// The clients of the free slots.
    idle: Mutex<Vec<LazyClient>>,
    /// Every slot's client, for `close` and for re-checks.
    all: Vec<LazyClient>,
    /// The re-check places of each slot's client, in the order of `all`.
    rechecks: Vec<Arc<Semaphore>>,
    /// The slot whose client the next re-check tries first.
    next_recheck: AtomicUsize,
}

impl FetchPool {
    pub(super) fn new(slots: NonZeroUsize) -> Self {
        let slots = slots.get();
        let all: Vec<LazyClient> = (0..slots).map(|_| LazyClient::default()).collect();
        Self {
            permits: Arc::new(Semaphore::new(slots)),
            idle: Mutex::new(all.clone()),
            all,
            rechecks: (0..slots)
                .map(|_| Arc::new(Semaphore::new(RECHECKS_PER_SLOT)))
                .collect(),
            next_recheck: AtomicUsize::new(0),
        }
    }

    /// A slot's client with a re-check place free, and that place. The slots take turns, so a
    /// slot that lost its link to a node does not re-check every partition of that node. `None`
    /// if every place is taken.
    fn recheck_place(&self) -> Option<RecheckPlace> {
        let first = self.next_recheck.fetch_add(1, Ordering::Relaxed);
        (0..self.all.len()).find_map(|turn| {
            let slot = first.wrapping_add(turn) % self.all.len();
            let permit = Arc::clone(&self.rechecks[slot]).try_acquire_owned().ok()?;
            Some(RecheckPlace {
                client: FetchClient(self.all[slot].clone()),
                _permit: permit,
            })
        })
    }

    /// A free slot. `None` if none frees before `deadline`: a poll started then would time out at
    /// once and drop the slot's client.
    async fn take(self: &Arc<Self>, deadline: Instant) -> Option<FetchSlot> {
        let permit = permit_by(&self.permits, deadline).await?;
        let client = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()?;
        Some(FetchSlot {
            client,
            pool: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Closes each client that connected. See [`LazyClient::close_all`].
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        LazyClient::close_all(&self.all).await
    }
}

/// An Iggy client that connects at its first call, so a bridge that never needs it opens no
/// extra connection.
///
/// A call through [`Self::until`] that times out takes the client with it, and the next call
/// connects a new one. Fetch and `ListOffsets` polls keep theirs. See [`IggyBridge::poll_in`].
#[derive(Clone, Default)]
pub(super) struct LazyClient(Arc<AsyncMutex<Option<Arc<IggyClient>>>>);

impl LazyClient {
    /// A client already in hand, so a test needs no server.
    #[cfg(test)]
    pub(super) fn holding(client: Arc<IggyClient>) -> Self {
        Self(Arc::new(AsyncMutex::new(Some(client))))
    }

    /// Puts `client` in hand, so a test needs no server.
    #[cfg(test)]
    pub(super) async fn hold(&self, client: Arc<IggyClient>) {
        *self.0.lock().await = Some(client);
    }

    /// The client, connected at the first call. Callers that come during a connect wait for it.
    pub(super) async fn connected(
        &self,
        config: &IggyBridgeConfig,
    ) -> Result<Arc<IggyClient>, BridgeError> {
        let mut held = self.0.lock().await;
        if let Some(client) = held.as_ref() {
            return Ok(Arc::clone(client));
        }
        let connected = Arc::new(connect_client(config).await?);
        *held = Some(Arc::clone(&connected));
        drop(held);
        Ok(connected)
    }

    /// The client, if it connected.
    pub(super) async fn held(&self) -> Option<Arc<IggyClient>> {
        self.0.lock().await.clone()
    }

    /// Forgets `client` and closes its connection once the call running on it ends. For a send
    /// that was given up on: the SDK goes on writing it, and it must not land after a later send
    /// on a new connection.
    ///
    /// No timeout. The SDK ends a call by its 30 s read deadline, and a new connection opened
    /// before that could send ahead of it.
    pub(super) async fn retire(&self, client: &IggyClient) {
        self.0.lock().await.take();
        if let Err(error) = client.shutdown().await {
            tracing::debug!(%error, "failed to shut down a retired Iggy client");
        }
    }

    /// Connects, then runs `call` on the client until `deadline`. A call that times out drops
    /// the client it ran on.
    pub(super) async fn until<T, Call, Fut>(
        &self,
        config: &IggyBridgeConfig,
        deadline: Instant,
        call: Call,
    ) -> Result<T, BridgeError>
    where
        Call: FnOnce(Arc<IggyClient>) -> Fut,
        Fut: Future<Output = Result<T, BridgeError>>,
    {
        let mut used = None;
        let run = async {
            let client = self.connected(config).await?;
            used = Some(Arc::clone(&client));
            call(client).await
        };
        let result = timeout_at(deadline, run)
            .await
            .unwrap_or(Err(BridgeError::Timeout));
        let timed_out = matches!(result, Err(BridgeError::Timeout));
        if let Some(used) = used.filter(|_| timed_out) {
            self.forget(&used).await;
        }
        result
    }

    /// Drops `client` from the slot, unless another call already replaced it with a new one.
    async fn forget(&self, client: &Arc<IggyClient>) {
        let mut held = self.0.lock().await;
        if held
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, client))
        {
            held.take();
        }
    }

    /// [`Self::until`] on a task of its own, so the deadline still drops a stuck client when the
    /// caller stops waiting first.
    pub(super) async fn until_detached<T, Call, Fut>(
        &self,
        config: IggyBridgeConfig,
        deadline: Instant,
        call: Call,
    ) -> Result<T, BridgeError>
    where
        T: Send + 'static,
        Call: FnOnce(Arc<IggyClient>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, BridgeError>> + Send,
    {
        let client = self.clone();
        tokio::spawn(async move { client.until(&config, deadline, call).await })
            .await
            .unwrap_or(Err(BridgeError::Timeout))
    }

    /// Takes the client out of the slot without signing it out, so `close` does not wait behind
    /// a call still running on it. Leaves a client that is connecting right now.
    pub(super) fn discard(&self) {
        if let Ok(mut held) = self.0.try_lock() {
            held.take();
        }
    }

    /// Signs the client out and shuts it down, if it connected.
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        let Some(client) = self.0.lock().await.take() else {
            return Ok(());
        };
        sign_out_and_shut_down(&client).await
    }

    /// Closes each client that connected, and returns the first error. A client that fails to
    /// shut down stops none of the others.
    pub(super) async fn close_all(
        clients: impl IntoIterator<Item = &Self>,
    ) -> Result<(), BridgeError> {
        let mut closed = Ok(());
        for client in clients {
            let shutdown = client.close().await;
            closed = closed.and(shutdown);
        }
        closed
    }
}

/// One Fetch read slot. Dropping it frees the slot.
pub struct FetchSlot {
    client: LazyClient,
    pool: Arc<FetchPool>,
    _permit: OwnedSemaphorePermit,
}

impl FetchSlot {
    /// The slot's client, for the polls that its request runs at once.
    pub(crate) fn client(&self) -> FetchClient {
        FetchClient(self.client.clone())
    }
}

/// The client of one read slot. Polls on it share one connection.
#[derive(Clone)]
pub struct FetchClient(LazyClient);

/// One re-check place on a slot's client. Dropping it frees the place.
///
/// A re-check runs beside the requests, so it holds no read slot, only this place.
pub struct RecheckPlace {
    client: FetchClient,
    _permit: OwnedSemaphorePermit,
}

impl RecheckPlace {
    pub(crate) const fn client(&self) -> &FetchClient {
        &self.client
    }
}

impl Drop for FetchSlot {
    fn drop(&mut self) {
        // Before the permit goes, so the next holder finds a client.
        self.pool
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.client.clone());
    }
}

/// One partition as a Fetch sees it before it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionProbe {
    /// Same value `high_watermarks` reports.
    pub high_watermark: u64,
    /// Stored bytes per retained message. 0 when the partition retains none.
    pub average_size: u64,
    /// 0 when retention removed every message, or when the server has no stats for the partition.
    pub messages_count: u64,
}

impl PartitionProbe {
    /// Iggy counts the messages of a partition it loads before it restores the offset, so the high
    /// watermark reads too low until the load ends.
    ///
    /// A commit between Iggy's reads of the offset and the count looks the same for one probe.
    /// [`IggyBridge::probe`] reads twice to tell them apart.
    #[must_use]
    pub const fn is_loading(&self) -> bool {
        self.messages_count > self.high_watermark
    }
}

impl From<&Partition> for PartitionProbe {
    fn from(partition: &Partition) -> Self {
        Self {
            high_watermark: high_watermark(partition),
            average_size: partition
                .size
                .as_bytes_u64()
                .checked_div(partition.messages_count)
                .unwrap_or(0),
            messages_count: partition.messages_count,
        }
    }
}

/// Every partition of one topic, by id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicProbe {
    partitions: Vec<(u32, PartitionProbe)>,
}

impl TopicProbe {
    /// The probe of partition `id`, if the topic has it.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<PartitionProbe> {
        self.partitions
            .binary_search_by_key(&id, |&(found, _)| found)
            .ok()
            .map(|found| self.partitions[found].1)
    }

    #[must_use]
    pub fn partitions_count(&self) -> u32 {
        u32::try_from(self.partitions.len()).unwrap_or(u32::MAX)
    }
}

impl FromIterator<(u32, PartitionProbe)> for TopicProbe {
    fn from_iter<I: IntoIterator<Item = (u32, PartitionProbe)>>(partitions: I) -> Self {
        let mut partitions: Vec<_> = partitions.into_iter().collect();
        partitions.sort_unstable_by_key(|&(id, _)| id);
        Self { partitions }
    }
}

impl IggyBridge {
    /// A Fetch read slot, with its own client. `None` if none frees by `deadline`.
    ///
    /// Connects the slot's client first, by `deadline`, so a slow sign-in never counts against
    /// the deadline of one poll. A connect that fails is left to the first poll, which reports it.
    pub(crate) async fn fetch_slot(&self, deadline: Instant) -> Option<FetchSlot> {
        let slot = self.fetch_pool.take(deadline).await?;
        if let Ok(Err(error)) = timeout_at(deadline, slot.client.connected(&self.config)).await {
            tracing::debug!(%error, "a Fetch read slot could not connect to Iggy");
        }
        Some(slot)
    }

    /// Probes every partition of the topic `kafka_topic` maps to, with one `get_topic` call, or
    /// two when a partition reads as loading.
    ///
    /// Probes have their own client, so they never wait behind a Produce send.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's rules.
    /// [`BridgeError::Iggy`] if the stream or topic is missing, or the call fails.
    /// [`BridgeError::Timeout`] past `REQUEST_TIMEOUT`.
    pub(crate) async fn probe(&self, kafka_topic: &str) -> Result<TopicProbe, BridgeError> {
        let target = self.topic_target(kafka_topic)?;
        let probe = self.probe_target(&target).await?;
        // A load lasts. A commit that raced the read does not.
        if probe
            .partitions
            .iter()
            .any(|(_, partition)| partition.is_loading())
        {
            return self.probe_target(&target).await;
        }
        Ok(probe)
    }

    async fn probe_target(&self, target: &TopicTarget) -> Result<TopicProbe, BridgeError> {
        let get_topic = |client: Arc<IggyClient>| async move {
            client
                .get_topic(&target.stream_id, &target.topic_id)
                .await
                .map_err(BridgeError::Iggy)
        };
        let details = self
            .probe_client
            .until(&self.config, Instant::now() + REQUEST_TIMEOUT, get_topic)
            .await?
            // A missing stream reads as `None` too. Both answer the same Kafka code.
            .ok_or_else(|| {
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    target.topic_id.to_string(),
                    target.stream_id.to_string(),
                ))
            })?;
        Ok(details
            .partitions
            .iter()
            .map(|partition| (partition.id, PartitionProbe::from(partition)))
            .collect())
    }

    /// A place for one re-check on a slot's client, beside the requests. `None` if every slot's
    /// places are taken.
    pub(crate) fn recheck_place(&self) -> Option<RecheckPlace> {
        self.fetch_pool.recheck_place()
    }

    /// Reads up to `options.count` messages and `options.max_bytes` bytes of `partition` by
    /// `strategy`, with `client`.
    ///
    /// A plain consumer without auto commit, so Iggy stores no offset. Never `Next`.
    ///
    /// The SDK gets the deadline too, so a poll that gets no answer ends inside the SDK: it marks
    /// the route of the partition as failed, and the next poll on the client looks the route up
    /// again. Cut off from outside, the SDK would send every later poll to a node that stopped.
    /// The client stays, since polls share its connection and the SDK drops a late reply.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::probe`]. A partition the topic lacks is [`BridgeError::Iggy`] here.
    /// [`BridgeError::Timeout`] if no answer comes by `deadline`.
    pub(crate) async fn poll_in(
        &self,
        client: &FetchClient,
        kafka_topic: &str,
        partition: u32,
        strategy: PollingStrategy,
        options: PollOptions,
        deadline: Instant,
    ) -> Result<PolledMessages, BridgeError> {
        let target = self.topic_target(kafka_topic)?;
        let consumer = Consumer::default();
        let sdk_deadline = deadline
            .checked_sub(SDK_STOP_ROOM)
            .filter(|stop| *stop > Instant::now())
            .unwrap_or(deadline);
        let poll = async {
            let connected = client.0.connected(&self.config).await?;
            connected
                .poll_messages(
                    &target.stream_id,
                    &target.topic_id,
                    partition,
                    &consumer,
                    &strategy,
                    options,
                )
                .await
                .map_err(BridgeError::Iggy)
        };
        match timeout_at(deadline, with_binary_deadline(sdk_deadline, poll)).await {
            // The SDK ends a poll with no answer as a lost link or a refusal, at its deadline.
            Ok(Err(_)) if Instant::now() >= sdk_deadline => Err(BridgeError::Timeout),
            Ok(polled) => polled,
            Err(_elapsed) => Err(BridgeError::Timeout),
        }
    }
}

#[cfg(test)]
impl IggyBridge {
    /// Gives every Fetch read slot `client`, so a test needs no server.
    pub(crate) async fn hold_fetch_clients(&self, client: &Arc<IggyClient>) {
        for slot_client in &self.fetch_pool.all {
            slot_client.hold(Arc::clone(client)).await;
        }
    }

    /// How many Fetch read slots still hold a client.
    pub(crate) async fn held_fetch_clients(&self) -> usize {
        let mut held = 0;
        for slot_client in &self.fetch_pool.all {
            if slot_client.held().await.is_some() {
                held += 1;
            }
        }
        held
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iggy::prelude::{IggyByteSize, IggyClientBuilder, IggyTimestamp};

    use super::*;

    /// A client that never dialed, so a test can hold one without a server.
    fn unconnected() -> Arc<IggyClient> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address("127.0.0.1:1".to_string())
            .build()
            .expect("building a client dials nothing");
        Arc::new(client)
    }

    fn config() -> IggyBridgeConfig {
        IggyBridge::offline().config
    }

    fn stuck(_client: Arc<IggyClient>) -> std::future::Pending<Result<u8, BridgeError>> {
        std::future::pending()
    }

    #[tokio::test]
    async fn given_a_detached_call_whose_caller_gave_up_when_its_deadline_passes_should_drop_the_client()
     {
        let client = LazyClient::default();
        *client.0.lock().await = Some(unconnected());
        let deadline = Instant::now() + Duration::from_millis(50);

        let caller = tokio::time::timeout(
            Duration::from_millis(10),
            client.until_detached(config(), deadline, stuck),
        )
        .await;
        assert!(
            caller.is_err(),
            "the caller stops waiting before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            client.0.lock().await.is_none(),
            "the stuck client is dropped at its own deadline, not left for the next lookup"
        );
    }

    #[tokio::test]
    async fn given_a_call_past_its_deadline_when_given_up_on_should_drop_the_client() {
        let client = LazyClient::default();
        let held = unconnected();
        *client.0.lock().await = Some(Arc::clone(&held));
        let soon = || Instant::now() + Duration::from_millis(20);

        let config = config();
        let ended = client
            .until(&config, soon(), |_| async { Ok::<_, BridgeError>(7) })
            .await;
        assert_eq!(ended.expect("the call ended in time"), 7);
        let kept = client
            .0
            .lock()
            .await
            .clone()
            .expect("a call that ends keeps it");
        assert!(Arc::ptr_eq(&kept, &held));

        let failed = client
            .until(&config, soon(), |_| async {
                Err::<u8, _>(BridgeError::Iggy(IggyError::Disconnected))
            })
            .await;
        assert!(failed.is_err());
        assert!(
            client.0.lock().await.is_some(),
            "an error the SDK returned leaves no call behind"
        );

        let timed_out = client.until(&config, soon(), stuck).await;
        assert!(matches!(timed_out, Err(BridgeError::Timeout)));
        assert!(
            client.0.lock().await.is_none(),
            "the next call connects a new client"
        );
    }

    #[tokio::test]
    async fn given_a_call_past_its_deadline_when_another_call_replaced_its_client_should_keep_the_new_one()
     {
        let client = LazyClient::default();
        let (old, new) = (unconnected(), unconnected());
        client.hold(Arc::clone(&old)).await;
        let config = config();
        let swap = client.clone();
        let replaced = Arc::clone(&new);

        let timed_out = client
            .until(
                &config,
                Instant::now() + Duration::from_millis(20),
                |used| {
                    assert!(Arc::ptr_eq(&used, &old));
                    async move {
                        swap.hold(replaced).await;
                        std::future::pending::<Result<u8, BridgeError>>().await
                    }
                },
            )
            .await;

        assert!(matches!(timed_out, Err(BridgeError::Timeout)));
        let kept = client.held().await.expect("the new client stays");
        assert!(Arc::ptr_eq(&kept, &new));
    }

    const SLOTS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

    #[tokio::test]
    async fn given_every_slot_taken_when_one_is_dropped_should_hand_its_client_to_the_next_read() {
        let pool = Arc::new(FetchPool::new(SLOTS));
        let soon = || Instant::now() + Duration::from_millis(20);
        let mut taken = Vec::new();
        for _ in 0..SLOTS.get() {
            taken.push(pool.take(soon()).await.expect("a free slot"));
        }
        for (index, slot) in taken.iter().enumerate() {
            for other in &taken[index + 1..] {
                assert!(
                    !Arc::ptr_eq(&slot.client.0, &other.client.0),
                    "each slot has its own client"
                );
            }
        }
        assert!(pool.take(soon()).await.is_none(), "every slot is taken");

        let freed = taken.pop().expect("a slot to free");
        let client = freed.client.clone();
        drop(freed);
        let next = pool.take(soon()).await.expect("the freed slot");
        assert!(
            Arc::ptr_eq(&next.client.0, &client.0),
            "with the freed client"
        );
        drop((next, taken));
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_a_slot_is_free_should_take_none_and_keep_its_client() {
        let pool = Arc::new(FetchPool::new(SLOTS));
        let held = unconnected();
        for client in &pool.all {
            client.hold(Arc::clone(&held)).await;
        }
        assert!(pool.take(Instant::now()).await.is_none());
        for client in &pool.all {
            assert!(client.held().await.is_some());
        }
        let soon = Instant::now() + Duration::from_millis(20);
        assert!(pool.take(soon).await.is_some(), "the slot stayed free");
    }

    #[tokio::test]
    async fn given_a_slot_whose_client_never_connected_when_taken_should_dial_before_any_poll() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a local listener");
        let address = listener.local_addr().expect("its address").to_string();
        let bridge = IggyBridge::offline_at(&address);
        let dialed = tokio::spawn(async move { listener.accept().await.is_ok() });

        let taken = bridge
            .fetch_slot(Instant::now() + Duration::from_millis(300))
            .await
            .is_some();

        assert!(taken, "a failed connect still hands the slot over");
        let dialed = tokio::time::timeout(Duration::from_secs(1), dialed).await;
        assert!(
            matches!(dialed, Ok(Ok(true))),
            "the slot dials when taken, so a sign-in never spends a poll's time"
        );
    }

    #[tokio::test]
    async fn given_re_checks_when_placed_should_take_turns_over_the_slots_and_stop_when_all_are_taken()
     {
        let pool = FetchPool::new(SLOTS);
        let slot_of = |place: &RecheckPlace| {
            pool.all
                .iter()
                .position(|client| Arc::ptr_eq(&client.0, &place.client.0.0))
                .expect("a slot's client")
        };
        let first = pool.recheck_place().expect("a free place");
        let second = pool.recheck_place().expect("a free place");
        assert_ne!(
            slot_of(&first),
            slot_of(&second),
            "the next re-check tries the next slot first"
        );

        let mut held = vec![first, second];
        while let Some(place) = pool.recheck_place() {
            held.push(place);
        }
        assert_eq!(held.len(), SLOTS.get() * RECHECKS_PER_SLOT);
        for slot in 0..SLOTS.get() {
            let on_slot = held.iter().filter(|place| slot_of(place) == slot).count();
            assert_eq!(on_slot, RECHECKS_PER_SLOT, "slot {slot}");
        }

        let freed = held.pop().expect("a place to free");
        let slot = slot_of(&freed);
        drop(freed);
        let again = pool.recheck_place().expect("the freed place");
        assert_eq!(slot_of(&again), slot);
        drop((again, held));
    }

    fn partition(current_offset: u64, messages_count: u64, size: u64) -> Partition {
        Partition {
            id: 0,
            created_at: IggyTimestamp::zero(),
            segments_count: 1,
            current_offset,
            size: IggyByteSize::from(size),
            messages_count,
            context: iggy::prelude::PartitionContext::default(),
            members: Vec::new(),
            in_sync_members: Vec::new(),
        }
    }

    #[test]
    fn given_stored_messages_when_probed_should_give_watermark_and_average_size() {
        let probe = PartitionProbe::from(&partition(9, 10, 1000));
        assert_eq!(probe.high_watermark, 10);
        assert_eq!(probe.average_size, 100);
        assert_eq!(probe.messages_count, 10);
    }

    #[test]
    fn given_an_empty_partition_when_probed_should_give_zero_for_both() {
        let probe = PartitionProbe::from(&partition(0, 0, 0));
        assert_eq!(probe.high_watermark, 0);
        assert_eq!(probe.average_size, 0);
    }

    #[test]
    fn given_every_message_expired_when_probed_should_keep_the_watermark() {
        // Retention lowers the count, not the offset.
        let probe = PartitionProbe::from(&partition(41, 0, 0));
        assert_eq!(probe.high_watermark, 42);
        assert_eq!(probe.average_size, 0, "no retained message to measure");
        assert_eq!(probe.messages_count, 0);
    }

    #[test]
    fn given_partitions_in_any_order_when_looked_up_should_find_each_by_id() {
        let probe = |high_watermark| PartitionProbe {
            high_watermark,
            average_size: 1,
            messages_count: high_watermark,
        };
        let topic: TopicProbe = [(2, probe(20)), (0, probe(0)), (5, probe(50))]
            .into_iter()
            .collect();
        assert_eq!(topic.get(0), Some(probe(0)));
        assert_eq!(topic.get(2), Some(probe(20)));
        assert_eq!(topic.get(5), Some(probe(50)));
        assert_eq!(topic.get(3), None);
        assert_eq!(topic.partitions_count(), 3);
    }
}
