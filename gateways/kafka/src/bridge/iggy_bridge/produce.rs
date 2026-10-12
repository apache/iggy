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
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::{IggyError, IggyMessage, MessageClient, Partitioning};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout, timeout_at};

use super::fetch::LazyClient;
use super::{IggyBridge, SLOT_LIMIT, TopicTarget, in_slot, permit_by};
use crate::bridge::error::BridgeError;

/// Frame bytes besides the messages: request header, batch header and ids.
const SEND_FRAME_ROOM: u64 = 4 * 1024;

/// Produce sessions open at once, each an Iggy connection.
const PRODUCE_SESSIONS: usize = 256;

/// A free session unused this long closes when a new producer opens one.
const SESSION_IDLE: Duration = Duration::from_secs(600);

/// Who sends a Produce request. Each producer gets an Iggy session of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Producer {
    /// The producer id of the request's record batches.
    Id(i64),
    /// The Kafka connection, for a producer without an id.
    Connection(u64),
}

/// One producer's Iggy client, and one send at a time on it, so its sends reach Iggy in order.
pub struct ProduceSession {
    client: LazyClient,
    turn: Arc<Semaphore>,
}

/// The Produce sessions, one per producer, so different producers send at once.
pub(super) struct ProducePool {
    sessions: Mutex<HashMap<Producer, Held>>,
    capacity: usize,
}

/// A session, and when a request last took it.
struct Held {
    session: Arc<ProduceSession>,
    used: Instant,
}

impl ProducePool {
    pub(super) fn new() -> Self {
        Self::with_capacity(PRODUCE_SESSIONS)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    /// The session of `producer`, opened if it has none. `None` when the pool is full and every
    /// session is busy.
    ///
    /// Opening one first drops the free sessions idle past [`SESSION_IDLE`], then the least
    /// recently used free one if the pool is still full.
    fn take(&self, producer: Producer) -> Option<Arc<ProduceSession>> {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(held) = sessions.get_mut(&producer) {
            held.used = now;
            return Some(Arc::clone(&held.session));
        }
        sessions
            .retain(|_, held| !(held.is_free() && now.duration_since(held.used) >= SESSION_IDLE));
        if sessions.len() >= self.capacity {
            let oldest = sessions
                .iter()
                .filter(|(_, held)| held.is_free())
                .min_by_key(|(_, held)| held.used)
                .map(|(producer, _)| *producer)?;
            sessions.remove(&oldest);
        }
        let session = Arc::new(ProduceSession {
            client: LazyClient::default(),
            turn: Arc::new(Semaphore::new(1)),
        });
        let held = Held {
            session: Arc::clone(&session),
            used: now,
        };
        sessions.insert(producer, held);
        drop(sessions);
        Some(session)
    }

    /// Shuts down each session's client that connected. See [`LazyClient::close_all`].
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        let clients: Vec<LazyClient> = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|held| held.session.client.clone())
            .collect();
        LazyClient::close_all(&clients).await
    }
}

impl Held {
    /// No request holds it and no send runs on it, so dropping it loses nothing.
    fn is_free(&self) -> bool {
        Arc::strong_count(&self.session) == 1 && self.session.turn.available_permits() == 1
    }
}

impl IggyBridge {
    /// Largest partition, in message bytes, that one Iggy send carries.
    #[must_use]
    pub const fn max_send_bytes(&self) -> u64 {
        self.config.max_message_size.saturating_sub(SEND_FRAME_ROOM)
    }

    /// The Produce session of `producer`. `None` when every session is busy.
    #[must_use]
    pub fn produce_session(&self, producer: Producer) -> Option<Arc<ProduceSession>> {
        self.produce_pool.take(producer)
    }

    /// Appends `messages` to `partition` of `target` on `session`, and returns the base offset.
    ///
    /// `partition` passes through unchanged: both systems count from 0. The server rejects a
    /// partition the topic lacks, so no round trip checks first.
    ///
    /// `Ok(None)`: committed, but the server named no offset (a duplicate request, or a journal
    /// entry that is gone). Do not retry it.
    ///
    /// A stuck send holds its session up to `SLOT_LIMIT`, so the producer's later sends answer 7
    /// meanwhile and cannot overtake it. Other producers send on.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Timeout`] if `deadline` passes. The send may have landed.
    /// [`BridgeError::SendLost`] if the connection broke during the send. It may have landed.
    /// [`BridgeError::Iggy`] if the session cannot connect, if the stream, topic or partition is
    /// missing, or if the send is too large.
    pub async fn send_records(
        &self,
        session: &ProduceSession,
        target: &TopicTarget,
        partition: u32,
        mut messages: Vec<IggyMessage>,
        deadline: Instant,
    ) -> Result<Option<u64>, BridgeError> {
        let client = timeout_at(deadline, session.client.connected(&self.config))
            .await
            .map_err(|_elapsed| BridgeError::Timeout)??;
        let (stream_id, topic_id) = (target.stream_id.clone(), target.topic_id.clone());
        let send = async move {
            let partitioning = Partitioning::partition_id(partition);
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await
        };
        let response = send_in_slot(&session.turn, deadline, send).await?;
        Ok(response
            .confirmations
            .iter()
            .find(|confirmation| confirmation.partition_id == partition)
            .map(|confirmation| confirmation.base_offset))
    }
}

/// Runs `send` in a task that holds `slot` until the send ends, and waits for it until `deadline`.
///
/// The SDK cannot cancel a send. A send that outlives its caller keeps the slot, so the next one
/// waits here instead of queueing its batch inside the SDK.
async fn send_in_slot<T: Send + 'static>(
    slot: &Arc<Semaphore>,
    deadline: Instant,
    send: impl Future<Output = Result<T, IggyError>> + Send + 'static,
) -> Result<T, BridgeError> {
    let Some(permit) = permit_by(slot, deadline).await else {
        return Err(BridgeError::Timeout);
    };
    let send = async move {
        match timeout(SLOT_LIMIT, send).await {
            Ok(result) => result.map_err(send_error),
            Err(_elapsed) => Err(BridgeError::Timeout),
        }
    };
    // The permit comes back only to be dropped. A send that outlives the caller frees it in its task.
    in_slot(permit, deadline, send)
        .await
        .map_or(Err(BridgeError::Timeout), |(result, _permit)| result)
}

/// The SDK does not replay a send after these, so it may have landed.
const fn send_error(error: IggyError) -> BridgeError {
    match error {
        IggyError::Disconnected
        | IggyError::EmptyResponse
        | IggyError::TcpError
        | IggyError::StaleClient => BridgeError::SendLost(error),
        error => BridgeError::Iggy(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    const WAIT: Duration = Duration::from_millis(20);

    /// A send that records that it ran.
    async fn flagged(started: Arc<AtomicBool>) -> Result<u64, IggyError> {
        started.store(true, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok(2)
    }

    #[tokio::test]
    async fn given_a_send_past_its_deadline_when_the_next_one_waits_should_not_start_it() {
        let slot = Arc::new(Semaphore::new(1));
        let (release, released) = oneshot::channel::<()>();
        let stuck = async move {
            let _ = released.await;
            Ok::<u64, IggyError>(1)
        };
        assert!(matches!(
            send_in_slot(&slot, Instant::now() + WAIT, stuck).await,
            Err(BridgeError::Timeout)
        ));

        let started = Arc::new(AtomicBool::new(false));
        assert!(
            matches!(
                send_in_slot(&slot, Instant::now() + WAIT, flagged(Arc::clone(&started))).await,
                Err(BridgeError::Timeout)
            ),
            "the stuck send still holds the slot"
        );
        assert!(
            !started.load(Ordering::SeqCst),
            "a send that never got the slot never reaches the SDK"
        );

        release.send(()).unwrap();
        let after = send_in_slot(
            &slot,
            Instant::now() + WAIT * 10,
            flagged(Arc::clone(&started)),
        )
        .await;
        assert_eq!(after.unwrap(), 2, "the slot frees when the stuck send ends");
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_sending_should_not_start_the_send() {
        let slot = Arc::new(Semaphore::new(1));
        let started = Arc::new(AtomicBool::new(false));

        assert!(matches!(
            send_in_slot(&slot, Instant::now(), flagged(Arc::clone(&started))).await,
            Err(BridgeError::Timeout)
        ));
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn given_a_lost_connection_when_sending_should_say_the_send_may_have_landed() {
        let slot = Arc::new(Semaphore::new(1));
        let lost = async { Err::<u64, IggyError>(IggyError::Disconnected) };

        assert!(matches!(
            send_in_slot(&slot, Instant::now() + WAIT * 10, lost).await,
            Err(BridgeError::SendLost(IggyError::Disconnected))
        ));
    }

    #[tokio::test]
    async fn given_one_producer_when_taken_twice_should_get_one_session() {
        let pool = ProducePool::with_capacity(4);
        let first = pool.take(Producer::Id(7)).unwrap();

        assert!(Arc::ptr_eq(&first, &pool.take(Producer::Id(7)).unwrap()));
        assert!(
            !Arc::ptr_eq(&first, &pool.take(Producer::Connection(7)).unwrap()),
            "an id and a connection number never share a session"
        );
    }

    #[tokio::test]
    async fn given_a_stuck_send_when_another_producer_sends_should_not_wait_for_it() {
        let pool = ProducePool::with_capacity(4);
        let (stuck, other) = (
            pool.take(Producer::Id(1)).unwrap(),
            pool.take(Producer::Id(2)).unwrap(),
        );
        let (_release, released) = oneshot::channel::<()>();
        let stuck_send = async move {
            let _ = released.await;
            Ok::<u64, IggyError>(1)
        };
        assert!(matches!(
            send_in_slot(&stuck.turn, Instant::now() + WAIT, stuck_send).await,
            Err(BridgeError::Timeout)
        ));

        let started = Arc::new(AtomicBool::new(false));
        let sent = send_in_slot(&other.turn, Instant::now() + WAIT * 10, flagged(started)).await;
        assert_eq!(sent.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_full_pool_when_a_new_producer_comes_should_drop_the_least_used_free_one() {
        let pool = ProducePool::with_capacity(2);
        let first = Arc::downgrade(&pool.take(Producer::Id(1)).unwrap());
        tokio::time::advance(WAIT).await;
        let second = Arc::downgrade(&pool.take(Producer::Id(2)).unwrap());
        tokio::time::advance(WAIT).await;
        pool.take(Producer::Id(1)).unwrap();
        tokio::time::advance(WAIT).await;

        pool.take(Producer::Id(3)).unwrap();
        assert!(first.upgrade().is_some(), "used most recently, so it stays");
        assert!(second.upgrade().is_none());
    }

    #[tokio::test]
    async fn given_a_full_pool_of_held_sessions_when_a_new_producer_comes_should_refuse_it() {
        let pool = ProducePool::with_capacity(1);
        let held = pool.take(Producer::Id(1)).unwrap();

        assert!(pool.take(Producer::Id(2)).is_none());
        drop(held);
        assert!(
            pool.take(Producer::Id(2)).is_some(),
            "a free session makes room"
        );
    }

    #[tokio::test]
    async fn given_a_send_in_flight_when_the_pool_is_full_should_keep_its_session() {
        let pool = ProducePool::with_capacity(1);
        let session = pool.take(Producer::Id(1)).unwrap();
        let _sending = Arc::clone(&session.turn).acquire_owned().await.unwrap();
        let sending = Arc::downgrade(&session);
        drop(session);

        assert!(
            pool.take(Producer::Id(2)).is_none(),
            "the producer's next send queues behind this one, not on a new session"
        );
        assert!(sending.upgrade().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_session_idle_past_the_limit_when_a_new_producer_comes_should_drop_it() {
        let pool = ProducePool::with_capacity(4);
        let idle = Arc::downgrade(&pool.take(Producer::Connection(1)).unwrap());
        tokio::time::advance(SESSION_IDLE).await;

        pool.take(Producer::Id(2)).unwrap();
        assert!(idle.upgrade().is_none());
    }
}
