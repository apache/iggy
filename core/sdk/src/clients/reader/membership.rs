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
use std::sync::atomic::{AtomicBool, Ordering};

use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{Client, ConsumerGroupClient, Identifier, IggyError};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::driver::operation_deadline;

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
    /// A create or a join whose outcome is unknown, resolved before the next join.
    uncertain: Mutex<Option<u64>>,
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
            uncertain: Mutex::new(None),
        }
    }

    /// Joins the group unless the member already joined in this session. A server treats a
    /// second join from the same session as a no-op.
    pub(crate) async fn join(&self) -> Result<(), IggyError> {
        if self.joined.load(ORDERING) {
            return Ok(());
        }
        let (stream_id, topic_id, group_id) = (&self.stream, &self.topic, &self.group);
        let mut uncertain_operation = self.uncertain.lock().await;
        let client = self.client.read().await;
        if let Some(op) = *uncertain_operation {
            let result = client
                .resolve_operation(op, operation_deadline().into_std())
                .await;
            if matches!(result, Err(IggyError::OperationUncertain(_))) {
                return result;
            }
            *uncertain_operation = None;
            match result {
                Ok(())
                | Err(IggyError::OperationNotRetained(_))
                | Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
                Err(error) => return Err(error),
            }
        }
        // A proved join can belong to an earlier session, so establish membership again.
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
                    if let IggyError::OperationUncertain(op) = &error {
                        *uncertain_operation = Some(*op);
                    }
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
            if let IggyError::OperationUncertain(op) = &error {
                *uncertain_operation = Some(*op);
            }
            error!(
                "Failed to join consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}: {error}"
            );
            return Err(error);
        }
        self.joined.store(true, ORDERING);
        info!("Joined consumer group: {group_id} for topic: {topic_id}, stream: {stream_id}");
        Ok(())
    }

    /// The session that joined is gone, so the next refresh joins again.
    pub(crate) fn forget(&self) {
        self.joined.store(false, ORDERING);
    }

    /// Lets the server give the partitions to the other members at once, instead of after the
    /// connection times out.
    pub(crate) async fn leave(&self) {
        // Cleared either way: the reader is torn down whether or not the server confirms.
        if !self.joined.swap(false, ORDERING) {
            return;
        }
        let (stream_id, topic_id, group_id) = (&self.stream, &self.topic, &self.group);
        trace!("Leaving consumer group: {group_id} for stream: {stream_id}, topic: {topic_id}");
        if let Err(error) = self
            .client
            .read()
            .await
            .leave_consumer_group(stream_id, topic_id, group_id)
            .await
        {
            // Expected after an explicit leave or a deleted group.
            debug!(
                "Failed to leave consumer group: {group_id} for stream: {stream_id}, topic: {topic_id}. {error}"
            );
        }
    }
}
