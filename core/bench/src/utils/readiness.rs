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

//! The wait, after the bench creates a geo or tiered topic, until each
//! partition takes a message. A geo topic refuses sends until the object
//! store holds the topic's object, and the bench must not measure that.
//!
//! The wait leaves one probe message, marked with [`PROBE_HEADER`], at the
//! start of each partition. Consumers skip it, see [`is_probe`].

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::*;
use tokio::time::{Instant, sleep};
use tracing::{error, info};

/// The user header that marks a probe message.
pub const PROBE_HEADER: &str = "iggy-bench-probe";

/// The longest wait for every partition of one topic to take a probe.
pub const READINESS_TIMEOUT: Duration = Duration::from_mins(1);

/// The pause before a partition that refused a probe gets the next one.
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// The message that the wait sends to each partition.
///
/// # Errors
/// None in practice: the payload and the header are fixed and valid.
pub fn probe() -> Result<IggyMessage, IggyError> {
    let user_headers = BTreeMap::from([(probe_key()?, HeaderValue::from_str("1")?)]);
    IggyMessage::builder()
        .payload(Bytes::from_static(b"probe"))
        .user_headers(user_headers)
        .build()
}

/// Whether `message` is a probe. Benchmark messages carry no user headers,
/// so only a message with headers is parsed.
#[must_use]
pub fn is_probe(message: &IggyMessage) -> bool {
    message.header.user_headers_length != 0
        && probe_key().is_ok_and(|key| {
            message
                .user_headers_map()
                .ok()
                .flatten()
                .is_some_and(|headers| headers.contains_key(&key))
        })
}

/// Sends a probe to `stream_id`/`topic-1` partition by partition, until each
/// of its `partitions` takes one, within [`READINESS_TIMEOUT`].
///
/// # Errors
/// The error of a send that a retry cannot fix, or the last refusal when a
/// partition took no probe within the timeout.
pub async fn wait_until_partitions_take_messages(
    client: &IggyClient,
    stream_id: &Identifier,
    partitions: u32,
) -> Result<(), IggyError> {
    let topic_id = Identifier::named("topic-1")?;
    let started = Instant::now();
    let topic_id = &topic_id;
    let send = |partition: u32| async move {
        let mut messages = [probe()?];
        client
            .send_messages(
                stream_id,
                topic_id,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .map(drop)
    };
    let outcome = retry_each_partition(partitions, READINESS_TIMEOUT, send).await;
    match &outcome {
        Ok(()) => info!(
            "Every partition of stream {stream_id} takes messages after {:.2} s",
            started.elapsed().as_secs_f64()
        ),
        Err((partition, error)) => error!(
            "Partition {partition} of stream {stream_id} took no message within {} s: {error}",
            READINESS_TIMEOUT.as_secs()
        ),
    }
    outcome.map_err(|(_, error)| error)
}

/// Calls `send` for each partition in turn, again after [`RETRY_INTERVAL`]
/// while it fails with a transient error and `timeout` has not passed.
async fn retry_each_partition<F, Fut>(
    partitions: u32,
    timeout: Duration,
    mut send: F,
) -> Result<(), (u32, IggyError)>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<(), IggyError>>,
{
    let deadline = Instant::now() + timeout;
    for partition in 0..partitions {
        loop {
            match send(partition).await {
                Ok(()) => break,
                Err(error) if is_transient(&error) && Instant::now() < deadline => {
                    sleep(RETRY_INTERVAL).await;
                }
                Err(error) => return Err((partition, error)),
            }
        }
    }
    Ok(())
}

const fn is_transient(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::TransientNotAccepted | IggyError::TransientNotCommitted | IggyError::NotPrimary
    )
}

fn probe_key() -> Result<HeaderKey, IggyError> {
    HeaderKey::from_str(PROBE_HEADER)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::ready;

    use super::*;

    #[test]
    fn given_a_probe_when_checking_should_tell_it_from_a_benchmark_message() {
        let plain = IggyMessage::builder()
            .payload(Bytes::from_static(b"probe"))
            .build()
            .unwrap();
        let other_header = IggyMessage::builder()
            .payload(Bytes::from_static(b"probe"))
            .user_headers(BTreeMap::from([(
                HeaderKey::from_str("other").unwrap(),
                HeaderValue::from_str("1").unwrap(),
            )]))
            .build()
            .unwrap();

        assert!(is_probe(&probe().unwrap()));
        assert!(!is_probe(&plain));
        assert!(!is_probe(&other_header));
    }

    #[tokio::test(start_paused = true)]
    async fn given_partitions_that_refuse_at_first_when_waiting_should_send_until_each_takes_one() {
        let refusals = RefCell::new(vec![3, 0, 1]);
        let sent = RefCell::new(Vec::new());
        let started = Instant::now();

        let outcome = retry_each_partition(3, READINESS_TIMEOUT, |partition: u32| {
            sent.borrow_mut().push(partition);
            let mut refusals = refusals.borrow_mut();
            let left = &mut refusals[partition as usize];
            if *left == 0 {
                return ready(Ok(()));
            }
            *left -= 1;
            ready(Err(IggyError::TransientNotAccepted))
        })
        .await;

        assert!(outcome.is_ok(), "{outcome:?}");
        assert_eq!(*sent.borrow(), [0, 0, 0, 0, 1, 2, 2]);
        assert_eq!(started.elapsed(), RETRY_INTERVAL * 4);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_partition_that_never_takes_a_probe_when_waiting_should_fail_at_the_timeout() {
        let timeout = Duration::from_secs(2);
        let started = Instant::now();

        let outcome = retry_each_partition(2, timeout, |partition: u32| {
            ready(if partition == 0 {
                Ok(())
            } else {
                Err(IggyError::TransientNotAccepted)
            })
        })
        .await;

        assert!(matches!(outcome, Err((1, IggyError::TransientNotAccepted))));
        assert!(started.elapsed() >= timeout);
        assert!(started.elapsed() < timeout + RETRY_INTERVAL * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_error_a_retry_cannot_fix_when_waiting_should_fail_at_once() {
        let sent = RefCell::new(0);

        let outcome = retry_each_partition(2, READINESS_TIMEOUT, |_| {
            *sent.borrow_mut() += 1;
            ready(Err(IggyError::Unauthorized))
        })
        .await;

        assert!(matches!(outcome, Err((0, IggyError::Unauthorized))));
        assert_eq!(*sent.borrow(), 1);
    }
}
