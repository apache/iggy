/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::error::LabError;
use iggy::prelude::*;
use iggy_common::IggyError;
use std::time::Duration;
use tokio::time::{Instant, interval};

const DEFAULT_POLL_TICK: Duration = Duration::from_millis(100);

/// Poll `probe` until it returns `Some(T)`, or fail after `timeout`.
///
/// On each tick, calls `probe()`:
/// - `Ok(Some(v))` → returns `v`
/// - `Ok(None)` → retries
/// - `Err(e)` where `e` is retryable → retries
/// - `Err(e)` otherwise → propagates immediately
#[allow(dead_code)]
pub async fn eventually<F, Fut, T>(
    timeout: Duration,
    tick: Duration,
    mut probe: F,
) -> Result<T, LabError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, LabError>>,
{
    let deadline = Instant::now() + timeout;
    let mut ticker = interval(tick);
    loop {
        ticker.tick().await;
        if Instant::now() >= deadline {
            return Err(LabError::Timeout {
                context: "eventually: condition not met within timeout".into(),
            });
        }
        match probe().await {
            Ok(Some(v)) => return Ok(v),
            Ok(None) => continue,
            Err(e) if is_retryable(&e) => continue,
            Err(e) => return Err(e),
        }
    }
}

fn is_retryable(err: &LabError) -> bool {
    match err {
        LabError::Iggy(e) => matches!(
            e,
            IggyError::ResourceNotFound(_)
                | IggyError::ConsumerGroupIdNotFound(_, _)
                | IggyError::ConsumerGroupNameNotFound(_, _)
                | IggyError::ConsumerGroupMemberNotFound(_, _, _)
                | IggyError::Disconnected
                | IggyError::NotConnected
                | IggyError::CannotEstablishConnection
                | IggyError::Unauthenticated
        ),
        _ => false,
    }
}

/// Wait until `get_consumer_group()` reports exactly `expected` members.
#[allow(dead_code)]
pub async fn await_members_count(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
    expected: u32,
    timeout: Duration,
) -> Result<ConsumerGroupDetails, LabError> {
    eventually(timeout, DEFAULT_POLL_TICK, || async {
        let details = client
            .get_consumer_group(stream_id, topic_id, group_id)
            .await?;
        match details {
            Some(d) if d.members_count == expected => Ok(Some(d)),
            _ => Ok(None),
        }
    })
    .await
}

/// Wait until rebalance is complete: every member has partitions assigned and
/// the total assigned partitions equals the topic's partition count.
#[allow(dead_code)]
pub async fn await_rebalance_complete(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
    timeout: Duration,
) -> Result<ConsumerGroupDetails, LabError> {
    eventually(timeout, DEFAULT_POLL_TICK, || async {
        let details = client
            .get_consumer_group(stream_id, topic_id, group_id)
            .await?;
        let Some(d) = details else {
            return Ok(None);
        };
        if d.members_count == 0 {
            return Ok(None);
        }
        let all_have_partitions = d.members.iter().all(|m| m.partitions_count > 0);
        let total_assigned: u32 = d.members.iter().map(|m| m.partitions_count).sum();
        if all_have_partitions && total_assigned == d.partitions_count {
            Ok(Some(d))
        } else {
            Ok(None)
        }
    })
    .await
}
