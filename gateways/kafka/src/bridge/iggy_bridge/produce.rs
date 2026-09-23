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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{IggyError, IggyMessage, MessageClient, Partitioning};
use tokio::sync::{Semaphore, oneshot};
use tokio::time::{Instant, timeout, timeout_at};

use super::{IggyBridge, TopicTarget};
use crate::bridge::error::BridgeError;

/// How long a send may hold the slot: the SDK read deadline (30 s) plus a reconnect (15 s).
const SEND_LIMIT: Duration = Duration::from_secs(45);

/// Frame bytes besides the messages: request header, batch header and ids.
const SEND_FRAME_ROOM: u64 = 4 * 1024;

impl IggyBridge {
    /// Largest partition, in message bytes, that one Iggy send carries.
    #[must_use]
    pub const fn max_send_bytes(&self) -> u64 {
        self.config.max_message_size.saturating_sub(SEND_FRAME_ROOM)
    }

    /// Appends `messages` to `partition` of `target` and returns the base offset.
    ///
    /// `partition` passes through unchanged: both systems count from 0. The server rejects a
    /// partition the topic lacks, so no round trip checks first.
    ///
    /// `Ok(None)`: committed, but the server named no offset (a duplicate request, or a journal
    /// entry that is gone). Do not retry it.
    ///
    /// A stuck send holds the slot up to `SEND_LIMIT`. Other sends answer 7 meanwhile.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Timeout`] if `deadline` passes. The send may have landed.
    /// [`BridgeError::SendLost`] if the connection broke during the send. It may have landed.
    /// [`BridgeError::Iggy`] if the stream, topic or partition is missing, or the send is too
    /// large.
    pub async fn send_records(
        &self,
        target: &TopicTarget,
        partition: u32,
        mut messages: Vec<IggyMessage>,
        deadline: Instant,
    ) -> Result<Option<u64>, BridgeError> {
        let client = Arc::clone(&self.client);
        let (stream_id, topic_id) = (target.stream_id.clone(), target.topic_id.clone());
        let send = async move {
            let partitioning = Partitioning::partition_id(partition);
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await
        };
        let response = send_in_slot(&self.send_slot, deadline, send).await?;
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
    let permit = match timeout_at(deadline, Arc::clone(slot).acquire_owned()).await {
        Ok(Ok(permit)) if Instant::now() < deadline => permit,
        _ => return Err(BridgeError::Timeout),
    };
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let result = match timeout(SEND_LIMIT, send).await {
            Ok(result) => result.map_err(send_error),
            Err(_elapsed) => Err(BridgeError::Timeout),
        };
        drop(permit);
        // The caller may have stopped waiting.
        let _ = sender.send(result);
    });
    match timeout_at(deadline, receiver).await {
        Ok(Ok(result)) => result,
        _ => Err(BridgeError::Timeout),
    }
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
}
