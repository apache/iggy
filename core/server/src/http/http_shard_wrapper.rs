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

use std::rc::Rc;

use iggy_common::{
    Consumer, ConsumerOffsetInfo, Identifier, IggyError, Partitioning, PartitioningKind,
};
use send_wrapper::SendWrapper;

use crate::shard::system::messages::PollingArgs;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::ShardRequest;
use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::topics;
use crate::streaming::users::user::User;
use crate::{shard::IggyShard, streaming::session::Session};
use iggy_common::IggyPollMetadata;

/// Wrapper around IggyShard for HTTP handlers.
///
/// Provides three categories of access:
/// 1. Control-plane mutations via `send_to_control_plane()` (routed through message pump)
/// 2. Read-only metadata access via `shard()` (direct, same-thread safe)
/// 3. Data-plane operations (poll/append messages, consumer offsets)
///
/// # Safety
/// This wrapper is safe because:
/// 1. HTTP server runs on shard 0's single thread (compio model)
/// 2. All operations are confined to that thread
/// 3. The underlying IggyShard is never accessed from multiple threads
pub struct HttpSafeShard {
    inner: Rc<IggyShard>,
}

// Safety: HttpSafeShard is only used in HTTP handlers on shard 0's thread.
// All operations are confined to the thread that created the IggyShard instance.
// The underlying IggyShard contains RefCell and Rc types that are not thread-safe,
// but they are never accessed across threads in the HTTP server context with
// compio's single-threaded model.
unsafe impl Send for HttpSafeShard {}
unsafe impl Sync for HttpSafeShard {}

impl HttpSafeShard {
    pub fn new(shard: Rc<IggyShard>) -> Self {
        Self { inner: shard }
    }

    /// Direct access to shard for read-only operations and auth.
    pub fn shard(&self) -> &IggyShard {
        &self.inner
    }

    /// Route control-plane mutations through the message pump.
    pub async fn send_to_control_plane(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse, IggyError> {
        let future = SendWrapper::new(self.inner.send_to_control_plane(request));
        future.await
    }

    // === Data-plane operations (message polling/appending) ===

    pub async fn get_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let topic = self.shard().resolve_topic(stream_id, topic_id)?;
        let future = SendWrapper::new(self.shard().get_consumer_offset(
            client_id,
            consumer,
            topic,
            partition_id,
        ));
        future.await
    }

    pub async fn store_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        let topic = self.shard().resolve_topic(stream_id, topic_id)?;
        let future = SendWrapper::new(self.shard().store_consumer_offset(
            client_id,
            consumer,
            topic,
            partition_id,
            offset,
        ));
        let _result = future.await?;
        Ok(())
    }

    pub async fn delete_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let topic = self.shard().resolve_topic(stream_id, topic_id)?;
        let future = SendWrapper::new(self.shard().delete_consumer_offset(
            client_id,
            consumer,
            topic,
            partition_id,
        ));
        let _result = future.await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn poll_messages(
        &self,
        client_id: u32,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        consumer: Consumer,
        maybe_partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        let topic = self
            .shard()
            .resolve_topic_for_poll(user_id, &stream_id, &topic_id)?;
        let future = SendWrapper::new(self.shard().poll_messages(
            client_id,
            topic,
            consumer.clone(),
            maybe_partition_id,
            args,
        ));

        future.await
    }

    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        partitioning: &Partitioning,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        use crate::shard::transmission::message::ResolvedPartition;

        let topic = self
            .shard()
            .resolve_topic_for_append(user_id, &stream_id, &topic_id)?;
        let partition_id = match partitioning.kind {
            PartitioningKind::Balanced => self
                .shard()
                .metadata
                .get_next_partition_id(topic.stream_id, topic.topic_id)
                .ok_or(IggyError::TopicIdNotFound(stream_id, topic_id))?,
            PartitioningKind::PartitionId => u32::from_le_bytes(
                partitioning
                    .value
                    .get(..4)
                    .ok_or(IggyError::InvalidCommand)?
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize,
            PartitioningKind::MessagesKey => {
                let partitions_count = self
                    .shard()
                    .metadata
                    .partitions_count(topic.stream_id, topic.topic_id);
                topics::helpers::calculate_partition_id_by_messages_key_hash(
                    partitions_count,
                    &partitioning.value,
                )
            }
        };

        let partition = ResolvedPartition {
            stream_id: topic.stream_id,
            topic_id: topic.topic_id,
            partition_id,
        };

        let future = SendWrapper::new(self.shard().append_messages(partition, batch));
        future.await
    }

    pub fn login_user(
        &self,
        username: &str,
        password: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        self.shard().login_user(username, password, session)
    }

    pub fn logout_user(&self, session: &Session) -> Result<(), IggyError> {
        self.shard().logout_user(session)
    }

    pub fn login_with_personal_access_token(
        &self,
        token: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        self.shard()
            .login_with_personal_access_token(token, session)
    }
}
