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

use crate::harness::{TestBinary, TestHarness};
use iggy::prelude::UserCreateOptions;
use iggy::prelude::{
    ClientWrapper, Consumer, ConsumerGroupClient, ConsumerGroupDetails, DEFAULT_ROOT_PASSWORD,
    DEFAULT_ROOT_USERNAME, GlobalPermissions, Identifier, IdentityInfo, IggyClient, IggyError,
    MessageClient, Permissions, PollOptions, PolledMessages, PollingStrategy, ServerCommitPolicy,
    StreamClient, TopicClient, TopicDetails, UserClient, UserStatus,
};
use iggy_binary_protocol::codes::{GET_PARTITION_ROUTE_CODE, SYNC_CONSUMER_GROUP_CODE};
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{FetchedSection, fetch_binary_messages};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{Instant, sleep};

pub const USER_PASSWORD: &str = "secret";
/// Each attempt of [`GroupPoller::poll`] syncs the assignment again, so the
/// second one covers an assignment that moved between the sync and the poll.
const GROUP_POLL_ATTEMPTS: usize = 2;
const ASSIGNMENT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TOPIC_STATS_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// On an idle cluster a backup learns a commit only from the commit broadcast.
/// Several broadcasts cover a late one.
const BACKUP_COMMIT_BROADCASTS: u32 = 6;

/// Wait for the expected assignment distribution, checking uniqueness during convergence.
pub async fn wait_for_consumer_group_assignment(
    client: &impl ConsumerGroupClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
    members_count: u32,
    budget: Duration,
) -> ConsumerGroupDetails {
    let deadline = Instant::now() + budget;
    loop {
        let group = client
            .get_consumer_group(stream_id, topic_id, group_id)
            .await
            .expect("read consumer group")
            .expect("consumer group exists");
        let mut assigned = HashSet::new();
        for member in &group.members {
            for partition in &member.partitions {
                assert!(
                    assigned.insert(*partition),
                    "duplicate partition owner: {group:?}"
                );
            }
        }
        let fewest = group
            .members
            .iter()
            .map(|member| member.partitions_count)
            .min()
            .unwrap_or(0);
        let most = group
            .members
            .iter()
            .map(|member| member.partitions_count)
            .max()
            .unwrap_or(0);
        if group.members_count == members_count
            && (members_count == 0 || assigned.len() == group.partitions_count as usize)
            && most - fewest <= 1
        {
            return group;
        }
        assert!(
            Instant::now() < deadline,
            "ownership did not converge to {members_count} members within {budget:?}: {group:?}"
        );
        sleep(ASSIGNMENT_POLL_INTERVAL).await;
    }
}

/// The group poll that the SDK removed, as test code: a round robin over the
/// partitions that the member may poll.
///
/// Each attempt syncs the assignment first, so the poller follows a rebalance
/// at once and never polls a partition that waits for revocation.
#[derive(Debug)]
pub struct GroupPoller {
    consumer: Consumer,
    cursor: usize,
}

impl GroupPoller {
    pub fn new(group: Identifier) -> Self {
        Self {
            consumer: Consumer::group(group),
            cursor: 0,
        }
    }

    /// Poll the next partition of the member's assignment. A
    /// `ConsumerGroupPartitionNotOwned` reply starts a new attempt, at most
    /// `GROUP_POLL_ATTEMPTS`.
    ///
    /// A member without partitions, or one whose every attempt is refused,
    /// gets [`PolledMessages::empty`]. Its `partition_id` is 0, so do not read
    /// it.
    ///
    /// # Errors
    /// `ConsumerGroupMemberNotFound` if the client is not a member, and the
    /// other errors of [`group_partitions`] and of the poll.
    pub async fn poll(
        &mut self,
        client: &IggyClient,
        stream_id: &Identifier,
        topic_id: &Identifier,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, IggyError> {
        for _ in 0..GROUP_POLL_ATTEMPTS {
            let partitions = group_partitions(client, stream_id, topic_id, &self.consumer.id)
                .await?
                .ok_or_else(|| {
                    IggyError::ConsumerGroupMemberNotFound(
                        0,
                        self.consumer.id.clone(),
                        topic_id.clone(),
                    )
                })?;
            if partitions.is_empty() {
                return Ok(PolledMessages::empty());
            }
            let partition_id = partitions[self.cursor % partitions.len()];
            self.cursor = self.cursor.wrapping_add(1);
            match client
                .poll_messages(
                    stream_id,
                    topic_id,
                    partition_id,
                    &self.consumer,
                    strategy,
                    options,
                )
                .await
            {
                Err(IggyError::ConsumerGroupPartitionNotOwned(..)) => {}
                outcome => return outcome,
            }
        }
        Ok(PolledMessages::empty())
    }
}

/// The caller's assignment from a raw `SyncConsumerGroup` frame: the
/// partitions it may poll, without those that wait for revocation. `None` when
/// the reply is empty: the client is not a member, or the stream, topic or
/// group does not resolve.
///
/// # Errors
/// The errors of the send, and `InvalidFormat` for a reply that does not
/// decode.
pub async fn group_partitions(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    group_id: &Identifier,
) -> Result<Option<Vec<u32>>, IggyError> {
    let request = SyncConsumerGroupRequest {
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        group_id: identifier_to_wire(group_id)?,
    };
    let reply = client
        .send_binary_request(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
        .await?;
    if reply.is_empty() {
        return Ok(None);
    }
    SyncConsumerGroupResponse::decode_from(&reply)
        .map(|assignment| Some(assignment.partitions))
        .map_err(|_| IggyError::InvalidFormat)
}

/// One `FetchMessages` frame, routed by its first target, so all targets need
/// one primary. The sections are not decrypted.
///
/// # Errors
/// `FeatureUnavailable` on HTTP and on a nested client, and the errors of the
/// fetch.
pub async fn fetch_sections(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    targets: &[(u32, PollingStrategy)],
    consumer: &Consumer,
    options: PollOptions,
) -> Result<Vec<FetchedSection>, IggyError> {
    let auto_commit = options.server_commit == ServerCommitPolicy::OnRead;
    let transport = client.client();
    let transport = transport.read().await;
    match &*transport {
        ClientWrapper::Tcp(inner) => {
            fetch_binary_messages(
                inner,
                stream_id,
                topic_id,
                targets,
                consumer,
                options.count,
                auto_commit,
                options.into(),
            )
            .await
        }
        ClientWrapper::Quic(inner) => {
            fetch_binary_messages(
                inner,
                stream_id,
                topic_id,
                targets,
                consumer,
                options.count,
                auto_commit,
                options.into(),
            )
            .await
        }
        ClientWrapper::WebSocket(inner) => {
            fetch_binary_messages(
                inner,
                stream_id,
                topic_id,
                targets,
                consumer,
                options.count,
                auto_commit,
                options.into(),
            )
            .await
        }
        ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => Err(IggyError::FeatureUnavailable),
    }
}

/// The replica id of the partition's primary, from a raw `GetPartitionRoute`
/// frame.
///
/// # Errors
/// The errors of the send, and `InvalidFormat` for a reply that does not
/// decode.
pub async fn partition_primary(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: u32,
) -> Result<u8, IggyError> {
    let request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id,
    };
    let reply = client
        .send_binary_request(GET_PARTITION_ROUTE_CODE, request.to_bytes())
        .await?;
    PartitionRouteResponse::decode_from(&reply)
        .map(|route| route.primary_replica)
        .map_err(|_| IggyError::InvalidFormat)
}

/// Wait until `get_topic` counts `expected` messages in the topic.
pub async fn wait_for_topic_messages_count(
    client: &(impl TopicClient + ?Sized),
    stream_id: &Identifier,
    topic_id: &Identifier,
    expected: u64,
    budget: Duration,
) {
    let messages_count = wait_for_topic(client, stream_id, topic_id, budget, |details| {
        details.messages_count == expected
    })
    .await
    .messages_count;
    assert_eq!(
        messages_count, expected,
        "topic messages_count is {messages_count} after {budget:?}, expected {expected}"
    );
}

/// Wait until `get_topic` returns details that satisfy `ready` or `budget`
/// runs out, and return the last details read.
///
/// Topic stats are per node. A node that is not the partition primary counts a
/// message only after it learns the commit, so its stats can trail the send by
/// up to [`backup_commit_budget`].
pub async fn wait_for_topic(
    client: &(impl TopicClient + ?Sized),
    stream_id: &Identifier,
    topic_id: &Identifier,
    budget: Duration,
    ready: impl Fn(&TopicDetails) -> bool,
) -> TopicDetails {
    let deadline = Instant::now() + budget;
    loop {
        let details = client
            .get_topic(stream_id, topic_id)
            .await
            .expect("read topic")
            .expect("topic exists");
        if ready(&details) || Instant::now() >= deadline {
            return details;
        }
        sleep(TOPIC_STATS_POLL_INTERVAL).await;
    }
}

/// How long a backup can take to learn a commit at the longest
/// `cluster.commit_broadcast_interval` the running servers of `harness` use.
///
/// # Panics
///
/// If no server runs, or a running server's config dump holds no valid interval.
pub fn backup_commit_budget(harness: &TestHarness) -> Duration {
    harness
        .all_servers()
        .iter()
        .filter(|server| server.is_running())
        .map(|server| {
            server
                .commit_broadcast_interval()
                .expect("the commit broadcast interval of a running server")
        })
        .max()
        .expect("a running server")
        .saturating_mul(BACKUP_COMMIT_BROADCASTS)
}

/// Login as root user.
pub async fn login_root(client: &impl UserClient) -> Result<IdentityInfo, iggy_common::IggyError> {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
}

/// Login as a specific user with the default test password.
pub async fn login_user(client: &impl UserClient, username: &str) -> IdentityInfo {
    client
        .login_user(username, USER_PASSWORD)
        .await
        .expect("Failed to login user")
}

/// Create a user with full permissions.
pub async fn create_user(client: &impl UserClient, username: &str) {
    client
        .create_user(
            username,
            USER_PASSWORD,
            UserStatus::Active,
            Some(full_permissions()),
            &UserCreateOptions::default(),
        )
        .await
        .expect("Failed to create user");
}

/// Delete a user by username.
pub async fn delete_user(client: &impl UserClient, username: &str) {
    client
        .delete_user(&Identifier::named(username).expect("Invalid username"))
        .await
        .expect("Failed to delete user");
}

/// Assert the system is in a clean state (no streams, only root user).
pub async fn assert_clean_system<C: StreamClient + UserClient>(client: &C) {
    let streams = client.get_streams().await.expect("Failed to get streams");
    assert!(
        streams.is_empty(),
        "Expected no streams, found {}",
        streams.len()
    );

    let users = client.get_users().await.expect("Failed to get users");
    assert_eq!(
        users.len(),
        1,
        "Expected only root user, found {} users",
        users.len()
    );
}

fn full_permissions() -> Permissions {
    Permissions {
        global: GlobalPermissions {
            manage_servers: true,
            read_servers: true,
            manage_users: true,
            read_users: true,
            manage_streams: true,
            read_streams: true,
            manage_topics: true,
            read_topics: true,
            poll_messages: true,
            send_messages: true,
        },
        streams: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_permissions() {
        let perms = full_permissions();
        assert!(perms.global.manage_servers);
        assert!(perms.global.poll_messages);
        assert!(perms.global.send_messages);
    }
}
