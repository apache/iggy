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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{Client, ConsumerGroupClient, Identifier, IggyError};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

use crate::client_wrappers::client_wrapper::ClientWrapper;

const ORDERING: Ordering = Ordering::SeqCst;

/// The membership of a group reader. The reader joins in `build()`, joins again after the
/// client loses its session, and leaves in `close()`.
pub(crate) struct Membership {
    client: IggyRwLock<ClientWrapper>,
    stream: Arc<Identifier>,
    topic: Arc<Identifier>,
    group: Identifier,
    /// Only `IggyConsumer` creates a missing group.
    create: bool,
    joined: AtomicBool,
    /// Rises at each `forget`, so a join that a forget overtook does not count as joined.
    forgets: AtomicU64,
    /// Serializes the joins and the leave, so a leave never overtakes a join in flight.
    turn: Mutex<()>,
}

impl Membership {
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        group: Identifier,
        create: bool,
    ) -> Self {
        Self {
            client,
            stream,
            topic,
            group,
            create,
            joined: AtomicBool::new(false),
            forgets: AtomicU64::new(0),
            turn: Mutex::new(()),
        }
    }

    /// Joins the group unless the member already joined in this session. A server treats a
    /// second join from the same session as a no-op. So a create or a join with an unknown
    /// outcome is abandoned at once: the next join looks the group up and joins again, which
    /// changes nothing when the lost one landed.
    pub(crate) async fn join(&self) -> Result<(), IggyError> {
        if self.joined.load(ORDERING) {
            return Ok(());
        }
        let forgets = self.forgets.load(ORDERING);
        let (stream_id, topic_id, group_id) = (&self.stream, &self.topic, &self.group);
        let _turn = self.turn.lock().await;
        let client = self.client.read().await;
        trace!("Validating consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}");
        if client
            .get_consumer_group(stream_id, topic_id, group_id)
            .await?
            .is_none()
        {
            if !self.create {
                error!("Consumer group does not exist and auto-creation is disabled.");
                return Err(IggyError::ConsumerGroupNameNotFound(
                    group_id.to_string(),
                    Identifier::from_identifier(topic_id),
                ));
            }
            info!("Creating consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}");
            match client
                .create_consumer_group(stream_id, topic_id, &group_id.get_string_value()?)
                .await
            {
                Ok(_) | Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
                Err(error) => {
                    abandon_uncertain(&client, &error).await;
                    error!(
                        "Failed to create consumer group {group_id} for topic: {topic_id}, stream: {stream_id}: {error}"
                    );
                    return Err(error);
                }
            }
        }
        info!("Joining consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}");
        if let Err(error) = client
            .join_consumer_group(stream_id, topic_id, group_id)
            .await
        {
            self.joined.store(false, ORDERING);
            abandon_uncertain(&client, &error).await;
            error!(
                "Failed to join consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}: {error}"
            );
            return Err(error);
        }
        // The session that joined can end while the join is in flight.
        if self.forgets.load(ORDERING) == forgets {
            self.joined.store(true, ORDERING);
        }
        info!("Joined consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}");
        Ok(())
    }

    /// The session that joined is gone, so the next refresh joins again.
    pub(crate) fn forget(&self) {
        self.forgets.fetch_add(1, ORDERING);
        self.joined.store(false, ORDERING);
    }

    /// Lets the server give the partitions to the other members at once, instead of after the
    /// connection times out. It leaves also when a reconnect forgot the join, because the
    /// session that joined can still hold the member. It abandons a leave with an unknown
    /// outcome, as a join does.
    pub(crate) async fn leave(&self) {
        self.joined.store(false, ORDERING);
        let (stream_id, topic_id, group_id) = (&self.stream, &self.topic, &self.group);
        let _turn = self.turn.lock().await;
        let client = self.client.read().await;
        trace!("Leaving consumer group: {group_id} for stream: {stream_id}, topic: {topic_id}");
        if let Err(error) = client
            .leave_consumer_group(stream_id, topic_id, group_id)
            .await
        {
            abandon_uncertain(&client, &error).await;
            // Expected for a member that the server already dropped, and for a deleted group.
            debug!(
                "Failed to leave consumer group: {group_id} for stream: {stream_id}, topic: {topic_id}. {error}"
            );
        }
    }
}

/// A group write with an unknown outcome blocks every later metadata write of the client,
/// also those of the application, until someone resolves or abandons it.
async fn abandon_uncertain(client: &ClientWrapper, error: &IggyError) {
    if let IggyError::OperationUncertain(op) = error {
        // An operation that the client no longer retains is already free.
        let _ = client.abandon_operation(*op).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::codes::LEAVE_CONSUMER_GROUP_CODE;

    use crate::clients::reader::fixtures::{
        GROUP_ID, PRIMARY, STREAM_ID, TOPIC_ID, connect_to_one_replica, identifier,
    };
    use crate::links::fake_cluster::{Answer, endpoint};

    /// A reconnect clears the joined flag, while the session that joined can still hold the
    /// member on the server.
    #[tokio::test]
    async fn given_a_reconnect_after_the_join_when_leaving_should_send_the_leave_and_ignore_a_missing_member()
     {
        let (client, cluster) = connect_to_one_replica().await;
        cluster.script(
            &endpoint(PRIMARY),
            LEAVE_CONSUMER_GROUP_CODE,
            [Answer::Refuse(IggyError::ConsumerGroupMemberNotFound(
                1,
                identifier(TOPIC_ID),
                identifier(STREAM_ID),
            ))],
        );
        let membership = Membership::new(
            IggyRwLock::new(ClientWrapper::Tcp(client)),
            Arc::new(identifier(STREAM_ID)),
            Arc::new(identifier(TOPIC_ID)),
            identifier(GROUP_ID),
            false,
        );
        membership.joined.store(true, ORDERING);
        membership.forget();

        membership.leave().await;

        assert_eq!(
            cluster.count(LEAVE_CONSUMER_GROUP_CODE),
            1,
            "the close leaves whatever the client knows of its session"
        );
    }
}
