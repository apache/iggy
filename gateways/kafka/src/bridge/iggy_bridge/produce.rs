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

//! Produce-side bridge calls.
//!
//! Separate from `fetch.rs` so the two never edit one file.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::join_all;
use iggy::prelude::{IggyClient, IggyError, IggyMessage, MessageClient, Partitioning};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until, timeout, timeout_at};

use super::fetch::LazyClient;
use super::{IggyBridge, SLOT_LIMIT, TopicTarget, in_slot, permit_by, resolve_blocker};
use crate::bridge::config::{IggyBridgeConfig, ProduceSessionLimits};
use crate::bridge::error::BridgeError;

/// Frame bytes besides the messages: request header, batch header and ids.
const SEND_FRAME_ROOM: u64 = 4 * 1024;

/// Sessions that connect and sign in at once. A sign-in costs Iggy a password hash on a shard
/// thread, so a burst of new producers must not start them all together.
const SESSION_OPENS: usize = 4;

/// Each of those slots opens one session in this long, so at most 80 sign in a second. Iggy keeps
/// a session that ended without a sign-out for 30 s, and refuses new ones once it holds 8192.
const OPEN_SPACING: Duration = Duration::from_millis(50);

/// How often the free sessions past their idle limit close.
const SWEEP_EVERY: Duration = Duration::from_secs(30);

/// A full pool closes its least recently used free session to make room, once that session sat
/// unused this long. A producer that sends more often keeps its session, so more producers than
/// sessions do not mean a sign-in for each request.
const MAKE_ROOM_AFTER: Duration = Duration::from_secs(30);

/// Who sends a Produce request. Each producer gets an Iggy session of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Producer {
    /// A producer id this gateway gave out, and the user who sends with it when SASL is on. One
    /// user's id never reaches another user's session.
    Id { id: i64, user: Option<String> },
    /// The Kafka connection, for a producer without such an id.
    Connection(u64),
}

/// One Iggy client. One request holds its turn at a time, so the sends on it reach Iggy in order.
struct ProduceSession {
    client: LazyClient,
    turn: Arc<Semaphore>,
}

impl ProduceSession {
    fn new() -> Self {
        Self {
            client: LazyClient::default(),
            turn: Arc::new(Semaphore::new(1)),
        }
    }
}

/// A producer's place in the pool: a session of its own, or the one that the producers past the
/// limit share.
///
/// Each request of the producer holds a clone, taken under the pool lock, and a send that
/// outlives a request keeps it. So a count of 1 means that nothing of the producer runs.
struct Seat {
    session: Arc<ProduceSession>,
    own: bool,
}

/// A producer's turn on its session. Dropping it frees the turn.
pub struct ProduceTurn {
    seat: Arc<Seat>,
    _permit: OwnedSemaphorePermit,
}

/// The Produce sessions, one per producer, so different producers send at once.
pub(super) struct ProducePool {
    seats: Mutex<Seats>,
    /// The session of every producer that found the pool full.
    overflow: Arc<ProduceSession>,
    limits: ProduceSessionLimits,
    /// [`SESSION_OPENS`] slots. See [`Self::opening`].
    opens: Arc<Semaphore>,
    /// Whether the log already says that the pool filled up.
    full_logged: AtomicBool,
}

/// Every producer's seat, and how many of them have a session of their own.
#[derive(Default)]
struct Seats {
    held: HashMap<Producer, Held>,
    own: usize,
}

/// A seat, and when a request last took it.
struct Held {
    seat: Arc<Seat>,
    used: Instant,
}

impl ProducePool {
    pub(super) fn new(limits: ProduceSessionLimits) -> Self {
        Self {
            seats: Mutex::default(),
            overflow: Arc::new(ProduceSession::new()),
            limits,
            opens: Arc::new(Semaphore::new(SESSION_OPENS)),
            full_logged: AtomicBool::new(false),
        }
    }

    /// Closes idle sessions every [`SWEEP_EVERY`], until the pool is dropped.
    pub(super) fn sweep_in_background(self: &Arc<Self>) {
        let pool = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = interval(SWEEP_EVERY);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(pool) = pool.upgrade() else {
                    return;
                };
                pool.sweep();
            }
        });
    }

    /// The seat of `producer`.
    ///
    /// A producer without a session of its own gets one while the pool has room. A full pool
    /// first closes its least recently used free session, if that one sat unused for
    /// [`MAKE_ROOM_AFTER`]. Failing that, the producer shares the overflow session, and asks again
    /// at each request that finds none of its earlier ones running.
    fn take(&self, producer: Producer) -> Arc<Seat> {
        let now = Instant::now();
        let mut guard = self.seats.lock().unwrap_or_else(PoisonError::into_inner);
        let seats = &mut *guard;
        if let Some(held) = seats.held.get_mut(&producer) {
            held.used = now;
            // A request of the producer still runs on the shared session. A send on another
            // session could land ahead of it.
            if held.seat.own || !held.is_free() {
                return Arc::clone(&held.seat);
            }
        }
        let mut closed = seats.take_idle(now, self.limits.idle);
        if seats.own >= self.limits.max {
            let unused = self.limits.idle.min(MAKE_ROOM_AFTER);
            closed.extend(seats.take_least_used(now, unused));
        }
        let seat = if seats.own < self.limits.max {
            seats.own += 1;
            Seat {
                session: Arc::new(ProduceSession::new()),
                own: true,
            }
        } else {
            // Once, not per request. 0 sessions is the operator's choice, so it says nothing.
            if self.limits.max != 0 && !self.full_logged.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    sessions = self.limits.max,
                    "every Produce session is taken, so more producers share one Iggy client: \
                     raise IGGY_KAFKA_PRODUCE_SESSIONS to give each its own"
                );
            }
            Seat {
                session: Arc::clone(&self.overflow),
                own: false,
            }
        };
        let seat = Arc::new(seat);
        let held = Held {
            seat: Arc::clone(&seat),
            used: now,
        };
        seats.held.insert(producer, held);
        drop(guard);
        closed.into_iter().for_each(retire);
        seat
    }

    /// The client of `session`, connected and signed in at its first send.
    async fn client_of(
        &self,
        session: &Arc<ProduceSession>,
        config: &IggyBridgeConfig,
    ) -> Result<Arc<IggyClient>, BridgeError> {
        if let Some(client) = session.client.held().await {
            return Ok(client);
        }
        let (session, config) = (Arc::clone(session), config.clone());
        let open = async move { session.client.connected(&config).await };
        // `None` if the open's task died. Whether it signed in is then unknown.
        self.opening(open)
            .await
            .unwrap_or(Err(BridgeError::Timeout))
    }

    /// Runs `open` in one of the [`SESSION_OPENS`] slots, in a task of its own, and waits for it.
    ///
    /// A caller that gives up while it waits for a slot started nothing. One that gives up later
    /// leaves the open to end, for the producer's next request: a sign-in cut short leaves Iggy a
    /// session that nobody signs out, at a rate no slot counts.
    ///
    /// The slot stays taken until [`OPEN_SPACING`] after the open started, which caps the
    /// sign-ins a second.
    async fn opening<T: Send + 'static>(
        &self,
        open: impl Future<Output = T> + Send + 'static,
    ) -> Option<T> {
        // Never closed, so this waits and does not fail.
        let slot = Arc::clone(&self.opens).acquire_owned().await;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let free_at = Instant::now() + OPEN_SPACING;
            // The caller may have stopped waiting.
            let _ = sender.send(open.await);
            sleep_until(free_at).await;
            drop(slot);
        });
        receiver.await.ok()
    }

    /// Closes the free sessions unused for the idle limit.
    fn sweep(&self) {
        let closed = self
            .seats
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_idle(Instant::now(), self.limits.idle);
        closed.into_iter().for_each(retire);
    }

    /// Forgets the seat of `producer`, and closes its session once no send runs on it.
    fn forget(&self, producer: &Producer) {
        let mut seats = self.seats.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(held) = seats.held.remove(producer) else {
            return;
        };
        if held.seat.own {
            seats.own -= 1;
            drop(seats);
            retire(Arc::clone(&held.seat.session));
        }
    }

    /// Signs out and shuts down every session's client that connected, all at once. Returns the
    /// first error. A client that fails to shut down stops none of the others.
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        let seats = std::mem::take(&mut *self.seats.lock().unwrap_or_else(PoisonError::into_inner));
        let sessions: Vec<&Arc<ProduceSession>> = seats
            .held
            .values()
            .filter(|held| held.seat.own)
            .map(|held| &held.seat.session)
            .chain([&self.overflow])
            .collect();
        join_all(sessions.iter().map(|session| session.client.close()))
            .await
            .into_iter()
            .fold(Ok(()), Result::and)
    }
}

impl Seats {
    /// Takes out the free seats on the shared session, and the free ones unused for `idle`.
    /// Returns the sessions to close.
    fn take_idle(&mut self, now: Instant, idle: Duration) -> Vec<Arc<ProduceSession>> {
        // A free seat on the shared session holds nothing: its producer gets a new one.
        let closed: Vec<Arc<ProduceSession>> = self
            .held
            .extract_if(|_, held| {
                held.is_free() && (!held.seat.own || now.duration_since(held.used) >= idle)
            })
            .filter(|(_, held)| held.seat.own)
            .map(|(_, held)| Arc::clone(&held.seat.session))
            .collect();
        self.own -= closed.len();
        closed
    }

    /// Takes out the least recently used free session, if it sat unused for `unused`.
    fn take_least_used(&mut self, now: Instant, unused: Duration) -> Option<Arc<ProduceSession>> {
        let producer = self
            .held
            .iter()
            .filter(|(_, held)| held.seat.own && held.is_free())
            .filter(|(_, held)| now.duration_since(held.used) >= unused)
            .min_by_key(|(_, held)| held.used)
            .map(|(producer, _)| producer.clone())?;
        let held = self.held.remove(&producer)?;
        self.own -= 1;
        Some(Arc::clone(&held.seat.session))
    }
}

impl Held {
    /// None of the producer's requests runs, and no send that outlived one.
    fn is_free(&self) -> bool {
        Arc::strong_count(&self.seat) == 1
    }
}

/// Closes a session the pool forgot, in a task of its own, once no send runs on it.
fn retire(session: Arc<ProduceSession>) {
    // No runtime means the process is going down, and its sockets with it.
    let Ok(runtime) = Handle::try_current() else {
        return;
    };
    runtime.spawn(async move {
        // A send that outlived its request still holds the turn. It ends first.
        let _turn = session.turn.acquire().await;
        if let Err(error) = session.client.close().await {
            tracing::debug!(%error, "failed to close a Produce session");
        }
    });
}

impl IggyBridge {
    /// Largest partition, in message bytes, that one Iggy send carries.
    #[must_use]
    pub const fn max_send_bytes(&self) -> u64 {
        self.config.max_message_size.saturating_sub(SEND_FRAME_ROOM)
    }

    /// The turn of `producer` on its session. `None` if it does not free by `deadline`: an
    /// earlier request of the producer, or a send that outlived one, still holds it.
    ///
    /// Take it before anything the request must not hold while it waits here.
    pub async fn produce_turn(&self, producer: Producer, deadline: Instant) -> Option<ProduceTurn> {
        let seat = self.produce_pool.take(producer);
        let permit = permit_by(&seat.session.turn, deadline).await?;
        Some(ProduceTurn {
            seat,
            _permit: permit,
        })
    }

    /// Closes the Produce session that `connection` keyed, once no send runs on it. Call when the
    /// Kafka connection ends.
    pub fn end_connection(&self, connection: u64) {
        self.produce_pool.forget(&Producer::Connection(connection));
    }

    /// Appends `messages` to `partition` of `target` on the session of `turn`, and returns the
    /// base offset. Hands `turn` and `kept` back with it.
    ///
    /// `partition` passes through unchanged: both systems count from 0. The server rejects a
    /// partition the topic lacks, so no round trip checks first.
    ///
    /// `Ok(None)`: committed, but the server named no offset (a duplicate request, or a journal
    /// entry that is gone). Do not retry it.
    ///
    /// A send still running at `deadline` keeps `turn` and `kept`, and hands back `None` for
    /// them. So the producer's later requests answer 7 meanwhile and cannot overtake it, and what
    /// `kept` reserves for `messages` stays reserved. Other producers send on.
    ///
    /// Such a send drops `messages` and `kept` when it ends, at `SLOT_LIMIT` at the latest. Cut
    /// off there, it keeps `turn` until its connection is closed.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Timeout`] if `deadline` passes. The send may have landed.
    /// [`BridgeError::SendLost`] if the connection broke during the send. It may have landed.
    /// [`BridgeError::Iggy`] if the session cannot connect, if the stream, topic or partition is
    /// missing, or if the send is too large.
    pub async fn send_records<K: Send + 'static>(
        &self,
        turn: ProduceTurn,
        kept: K,
        target: &TopicTarget,
        partition: u32,
        mut messages: Vec<IggyMessage>,
        deadline: Instant,
    ) -> (Result<Option<u64>, BridgeError>, Option<(ProduceTurn, K)>) {
        let session = Arc::clone(&turn.seat.session);
        let connect = self.produce_pool.client_of(&session, &self.config);
        let client = match timeout_at(deadline, connect).await {
            Ok(Ok(client)) => client,
            Ok(Err(error)) => return (Err(error), Some((turn, kept))),
            Err(_elapsed) => return (Err(BridgeError::Timeout), Some((turn, kept))),
        };
        // Decoding can run past the deadline. A send must not start late.
        if Instant::now() >= deadline {
            return (Err(BridgeError::Timeout), Some((turn, kept)));
        }
        let (stream_id, topic_id) = (target.stream_id.clone(), target.topic_id.clone());
        let recovery_deadline = (Instant::now() + SLOT_LIMIT).into_std();
        let send = async move {
            // Owns the messages, so a send cut off at the limit drops them.
            let send = {
                let client = Arc::clone(&client);
                async move {
                    let partitioning = Partitioning::partition_id(partition);
                    loop {
                        match client
                            .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                            .await
                        {
                            Err(IggyError::LaneBlocked(operation)) => {
                                resolve_blocker(&client, operation, recovery_deadline).await?;
                            }
                            result => return result,
                        }
                    }
                }
            };
            within_slot_limit(&session, &client, kept, send).await
        };
        // The SDK cannot cancel a send. One that outlives this call keeps the turn in its task,
        // so the next one waits for it here instead of queueing its batch inside the SDK.
        let Some(((response, kept), turn)) = in_slot(turn, deadline, send).await else {
            return (Err(BridgeError::Timeout), None);
        };
        let base_offset = response.map(|response| {
            response
                .confirmations
                .iter()
                .find(|confirmation| confirmation.partition_id == partition)
                .map(|confirmation| confirmation.base_offset)
        });
        (base_offset, kept.map(|kept| (turn, kept)))
    }
}

/// Runs `send` for at most [`SLOT_LIMIT`], and hands `kept` back when it ends by then.
///
/// A send cut off there drops `kept` with the messages it gave up. The SDK goes on writing that
/// send, so its connection closes before this returns, and so before the turn frees: a later
/// send, on a new connection, cannot land ahead of it.
async fn within_slot_limit<T, K>(
    session: &ProduceSession,
    client: &IggyClient,
    kept: K,
    send: impl Future<Output = Result<T, IggyError>>,
) -> (Result<T, BridgeError>, Option<K>) {
    match timeout(SLOT_LIMIT, send).await {
        Ok(result) => (result.map_err(send_error), Some(kept)),
        Err(_elapsed) => {
            // Not across the close: that waits for the SDK's write, which a server that stopped
            // reading can hold up without end.
            drop(kept);
            session.client.retire(client).await;
            (Err(BridgeError::Timeout), None)
        }
    }
}

/// The SDK does not replay a send after these, so it may have landed. `StaleClient` is not one:
/// the SDK returns it only for a send that Iggy rejected, so it answers 6.
const fn send_error(error: IggyError) -> BridgeError {
    match error {
        IggyError::Disconnected | IggyError::EmptyResponse | IggyError::TcpError => {
            BridgeError::SendLost(error)
        }
        error => BridgeError::Iggy(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use iggy::prelude::{IggyClientBuilder, SystemClient};

    use super::*;
    use crate::protocol::api::ERROR_NOT_LEADER_OR_FOLLOWER;

    const WAIT: Duration = Duration::from_millis(20);
    const IDLE: Duration = Duration::from_secs(60);

    fn pool_of(max: usize) -> ProducePool {
        ProducePool::new(ProduceSessionLimits { max, idle: IDLE })
    }

    const fn id(id: i64) -> Producer {
        Producer::Id { id, user: None }
    }

    /// A client that never dialed and holds no credentials, so each call fails at once.
    fn unconnected() -> Arc<IggyClient> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address("127.0.0.1:1".to_string())
            .build()
            .expect("building a client dials nothing");
        Arc::new(client)
    }

    /// Gives `session` a client, and returns it to check on later.
    async fn with_client(session: &ProduceSession) -> Arc<IggyClient> {
        let client = unconnected();
        session.client.hold(Arc::clone(&client)).await;
        client
    }

    /// A call on a client that never dialed fails as not connected, until its shutdown.
    async fn is_shut_down(client: &IggyClient) -> bool {
        matches!(client.ping().await, Err(IggyError::ClientShutdown))
    }

    /// Waits for the task that closes a session the pool forgot. Time is paused in these tests.
    async fn closed(session: &Weak<ProduceSession>) {
        let gone = async {
            while session.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        timeout(Duration::from_secs(1), gone)
            .await
            .expect("a forgotten session closes");
    }

    fn shares(pool: &ProducePool, seat: &Seat) -> bool {
        Arc::ptr_eq(&seat.session, &pool.overflow)
    }

    #[tokio::test]
    async fn given_a_send_past_its_deadline_when_the_next_one_waits_should_not_start_it() {
        let turn = Arc::new(Semaphore::new(1));
        let (release, released) = oneshot::channel::<()>();
        let stuck = async move {
            let _ = released.await;
        };
        let sent = in_slot(
            Arc::clone(&turn).acquire_owned().await.unwrap(),
            Instant::now() + WAIT,
            stuck,
        )
        .await;
        assert!(sent.is_none(), "the send keeps the turn");
        assert!(
            permit_by(&turn, Instant::now() + WAIT).await.is_none(),
            "so the next one never reaches the SDK"
        );

        release.send(()).unwrap();
        assert!(
            permit_by(&turn, Instant::now() + WAIT * 10).await.is_some(),
            "the turn frees when the stuck send ends"
        );
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_sending_should_not_start_the_send() {
        let bridge = IggyBridge::offline();
        let target = bridge.topic_target("orders").expect("a valid topic");
        with_client(&bridge.produce_pool.take(id(1)).session).await;

        let (sent, held) = bridge
            .send_records(
                bridge
                    .produce_turn(id(1), Instant::now() + WAIT)
                    .await
                    .expect("a free turn"),
                7,
                &target,
                0,
                Vec::new(),
                Instant::now(),
            )
            .await;
        assert!(
            matches!(sent, Err(BridgeError::Timeout)),
            "a send that started would fail as not connected"
        );
        assert!(
            matches!(held, Some((_, 7))),
            "nothing runs, so nothing is kept"
        );
    }

    #[tokio::test]
    async fn given_a_lost_connection_when_sending_should_say_the_send_may_have_landed() {
        let session = ProduceSession::new();
        let lost = async { Err::<u64, IggyError>(IggyError::Disconnected) };

        assert!(matches!(
            within_slot_limit(&session, &unconnected(), 7, lost).await,
            (Err(BridgeError::SendLost(IggyError::Disconnected)), Some(7))
        ));
    }

    #[test]
    fn given_a_stale_client_rejection_when_sending_should_answer_not_leader_or_follower() {
        assert_eq!(
            send_error(IggyError::StaleClient).to_kafka_error_code(),
            ERROR_NOT_LEADER_OR_FOLLOWER
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_send_past_the_slot_limit_when_cut_off_should_close_its_connection_first() {
        let session = ProduceSession::new();
        let client = with_client(&session).await;
        let stuck = std::future::pending::<Result<u64, IggyError>>();

        assert!(
            matches!(
                within_slot_limit(&session, &client, 7, stuck).await,
                (Err(BridgeError::Timeout), None)
            ),
            "what it kept for its messages goes with them"
        );
        assert!(
            is_shut_down(&client).await,
            "the SDK's write cannot land after a later send"
        );
        assert!(
            session.client.held().await.is_none(),
            "the next send connects a new client"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_many_new_sessions_when_they_open_should_cap_how_many_sign_in_a_second() {
        let pool = pool_of(4);
        let started = Instant::now();

        for _ in 0..SESSION_OPENS {
            pool.opening(async {}).await;
        }
        assert_eq!(started.elapsed(), Duration::ZERO, "a slot for each");
        pool.opening(async {}).await;
        assert_eq!(
            started.elapsed(),
            OPEN_SPACING,
            "the next one waits for a slot, even after every open ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_open_its_caller_gave_up_on_when_it_can_go_on_should_run_to_its_end() {
        let pool = pool_of(4);
        let (release, released) = oneshot::channel::<()>();
        let ended = Arc::new(AtomicBool::new(false));
        let open = {
            let ended = Arc::clone(&ended);
            async move {
                let _ = released.await;
                ended.store(true, Ordering::SeqCst);
            }
        };

        assert!(timeout(WAIT, pool.opening(open)).await.is_err());
        release.send(()).unwrap();
        tokio::time::sleep(WAIT).await;
        assert!(
            ended.load(Ordering::SeqCst),
            "cut short, a sign-in leaves Iggy a session that nobody signs out"
        );
    }

    #[tokio::test]
    async fn given_one_producer_when_taken_twice_should_get_one_session() {
        let pool = pool_of(4);
        let first = pool.take(id(7));

        assert!(Arc::ptr_eq(&first.session, &pool.take(id(7)).session));
        assert!(
            !Arc::ptr_eq(&first.session, &pool.take(Producer::Connection(7)).session),
            "an id and a connection number never share a session"
        );
        let other_user = Producer::Id {
            id: 7,
            user: Some("eve".to_string()),
        };
        assert!(
            !Arc::ptr_eq(&first.session, &pool.take(other_user).session),
            "one user's id never reaches the session of another"
        );
    }

    #[tokio::test]
    async fn given_a_stuck_send_when_another_producer_sends_should_not_wait_for_it() {
        let pool = pool_of(4);
        let (stuck, other) = (pool.take(id(1)), pool.take(id(2)));
        let _sending = Arc::clone(&stuck.session.turn)
            .acquire_owned()
            .await
            .unwrap();

        assert!(
            permit_by(&stuck.session.turn, Instant::now() + WAIT)
                .await
                .is_none()
        );
        assert!(
            permit_by(&other.session.turn, Instant::now() + WAIT)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn given_a_full_pool_when_more_producers_come_should_share_the_overflow_session() {
        let pool = pool_of(1);
        let own = Arc::downgrade(&pool.take(id(1)).session);

        assert!(shares(&pool, &pool.take(id(2))));
        assert!(shares(&pool, &pool.take(id(3))));
        assert!(
            own.upgrade()
                .is_some_and(|own| Arc::ptr_eq(&own, &pool.take(id(1)).session)),
            "a session in use stays, so no producer signs in again for each request"
        );
    }

    #[tokio::test]
    async fn given_a_limit_of_no_sessions_when_producers_come_should_share_one_client() {
        let pool = pool_of(0);

        assert!(shares(&pool, &pool.take(id(1))));
        assert!(shares(&pool, &pool.take(Producer::Connection(2))));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_full_pool_when_a_new_producer_comes_should_close_only_a_stale_session() {
        let pool = pool_of(2);
        let stale = Arc::downgrade(&pool.take(id(1)).session);
        tokio::time::advance(MAKE_ROOM_AFTER / 2).await;
        let recent = Arc::downgrade(&pool.take(id(2)).session);
        assert!(
            shares(&pool, &pool.take(id(3))),
            "both sessions were used a moment ago"
        );

        tokio::time::advance(MAKE_ROOM_AFTER / 2).await;
        assert!(
            !shares(&pool, &pool.take(id(3))),
            "the least recently used one made room"
        );
        closed(&stale).await;
        assert!(recent.upgrade().is_some());
    }

    #[tokio::test]
    async fn given_a_producer_on_the_overflow_session_when_room_comes_should_move_once_none_of_its_requests_runs()
     {
        let pool = pool_of(1);
        drop(pool.take(Producer::Connection(1)));
        let running = pool.take(id(2));
        assert!(shares(&pool, &running));
        pool.forget(&Producer::Connection(1));

        assert!(
            shares(&pool, &pool.take(id(2))),
            "its earlier request still runs there, and must not be overtaken"
        );
        drop(running);
        assert!(!shares(&pool, &pool.take(id(2))));
    }

    #[tokio::test]
    async fn given_room_in_the_pool_when_a_new_producer_comes_should_not_join_a_busy_overflow_session()
     {
        let pool = pool_of(1);
        drop(pool.take(Producer::Connection(1)));
        let _running = pool.take(id(2));
        pool.forget(&Producer::Connection(1));

        assert!(!shares(&pool, &pool.take(id(3))));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_session_idle_past_the_limit_when_swept_should_close_its_client() {
        let pool = pool_of(4);
        let seat = pool.take(Producer::Connection(1));
        let client = with_client(&seat.session).await;
        let idle = Arc::downgrade(&seat.session);
        drop(seat);

        tokio::time::advance(IDLE / 2).await;
        pool.sweep();
        assert!(idle.upgrade().is_some(), "not idle long enough yet");

        tokio::time::advance(IDLE / 2).await;
        pool.sweep();
        closed(&idle).await;
        assert!(is_shut_down(&client).await);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_session_idle_past_the_limit_when_a_new_producer_comes_should_close_it() {
        let pool = pool_of(4);
        let idle = Arc::downgrade(&pool.take(id(1)).session);
        tokio::time::advance(IDLE).await;

        drop(pool.take(id(2)));
        closed(&idle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_send_in_flight_when_its_session_is_idle_should_keep_it() {
        let pool = pool_of(1);
        let seat = pool.take(id(1));
        tokio::time::advance(IDLE).await;

        pool.sweep();
        assert!(
            shares(&pool, &pool.take(id(2))),
            "a full pool does not close it to make room either"
        );
        assert!(
            Arc::ptr_eq(&seat.session, &pool.take(id(1)).session),
            "the producer's next send queues behind this one, not on a new session"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_connection_that_ends_when_forgotten_should_close_its_session_after_its_send() {
        let pool = pool_of(4);
        let seat = pool.take(Producer::Connection(1));
        let client = with_client(&seat.session).await;
        let sending = Arc::clone(&seat.session.turn)
            .acquire_owned()
            .await
            .unwrap();
        let ended = Arc::downgrade(&seat.session);
        drop(seat);

        pool.forget(&Producer::Connection(1));
        tokio::time::sleep(WAIT).await;
        assert!(ended.upgrade().is_some(), "its send still runs");
        assert!(!is_shut_down(&client).await);

        drop(sending);
        closed(&ended).await;
        assert!(is_shut_down(&client).await);
    }

    #[tokio::test]
    async fn given_open_sessions_when_the_pool_closes_should_shut_every_client_down() {
        let pool = pool_of(1);
        let own = with_client(&pool.take(id(1)).session).await;
        let shared = with_client(&pool.take(id(2)).session).await;

        pool.close()
            .await
            .expect("clients that never dialed close cleanly");
        assert!(is_shut_down(&own).await);
        assert!(is_shut_down(&shared).await, "the overflow session too");
    }
}
