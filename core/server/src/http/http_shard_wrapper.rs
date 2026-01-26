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
    Consumer, ConsumerOffsetInfo, Identifier, IggyError, IggyExpiry, Partitioning,
    PartitioningKind, Permissions, Stats, UserId, UserStatus,
};
use send_wrapper::SendWrapper;

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::system::messages::PollingArgs;
use crate::state::command::EntryCommand;
use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::topics;
use crate::streaming::users::user::User;
use crate::{shard::IggyShard, streaming::session::Session};
use iggy_common::PersonalAccessToken;

/// A wrapper around IggyShard that is safe to use in HTTP handlers.
///
/// # Safety
/// This wrapper is only safe to use when:
/// 1. The HTTP server runs on a single thread (compio's thread-per-core model)
/// 2. All operations are confined to shard 0's thread
/// 3. The underlying IggyShard is never accessed from multiple threads
///
/// The safety guarantee is provided by the HTTP server architecture where
/// all HTTP requests are handled on the same thread that owns the IggyShard.
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

    pub fn shard(&self) -> &IggyShard {
        &self.inner
    }

    pub async fn get_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let future = SendWrapper::new(self.shard().get_consumer_offset(
            client_id,
            consumer,
            stream_id,
            topic_id,
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
        let future = SendWrapper::new(self.shard().store_consumer_offset(
            client_id,
            consumer,
            stream_id,
            topic_id,
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
        let future = SendWrapper::new(self.shard().delete_consumer_offset(
            client_id,
            consumer,
            stream_id,
            topic_id,
            partition_id,
        ));
        let _result = future.await?;
        Ok(())
    }

    pub async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().delete_stream(stream_id));
        future.await?;
        Ok(())
    }

    pub fn update_stream(&self, stream_id: &Identifier, name: String) -> Result<(), IggyError> {
        self.shard().update_stream(stream_id, name)
    }

    pub async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().purge_stream(stream_id));
        future.await
    }

    pub async fn create_stream(
        &self,
        name: String,
    ) -> Result<crate::streaming::streams::stream::Stream, IggyError> {
        let future = SendWrapper::new(self.shard().create_stream(name));
        future.await
    }

    pub async fn apply_state(
        &self,
        user_id: UserId,
        command: &EntryCommand,
    ) -> Result<(), IggyError> {
        self.shard().state.apply(user_id, command).await
    }

    pub fn get_users(&self) -> Vec<User> {
        self.shard().get_users()
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<User, IggyError> {
        self.shard()
            .create_user(username, password, status, permissions)
    }

    pub fn delete_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        self.shard().delete_user(user_id)
    }

    pub fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        self.shard().update_user(user_id, username, status)
    }

    pub fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.shard().update_permissions(user_id, permissions)
    }

    pub fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.shard()
            .change_password(user_id, current_password, new_password)
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

    pub fn get_personal_access_tokens(
        &self,
        user_id: u32,
    ) -> Result<Vec<PersonalAccessToken>, IggyError> {
        self.shard().get_personal_access_tokens(user_id)
    }

    pub fn create_personal_access_token(
        &self,
        user_id: u32,
        name: &str,
        expiry: IggyExpiry,
    ) -> Result<(PersonalAccessToken, String), IggyError> {
        self.shard()
            .create_personal_access_token(user_id, name, expiry)
    }

    pub fn delete_personal_access_token(&self, user_id: u32, name: &str) -> Result<(), IggyError> {
        self.shard().delete_personal_access_token(user_id, name)
    }

    pub fn login_with_personal_access_token(
        &self,
        token: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        self.shard()
            .login_with_personal_access_token(token, session)
    }

    pub async fn get_stats(&self) -> Result<Stats, IggyError> {
        self.shard().get_stats().await
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
        let future = SendWrapper::new(self.shard().poll_messages(
            client_id,
            user_id,
            stream_id,
            topic_id,
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
        self.shard().ensure_topic_exists(&stream_id, &topic_id)?;

        let partition_id = self.shard().streams.with_topic_by_id(
            &stream_id,
            &topic_id,
            |(root, auxilary, ..)| match partitioning.kind {
                PartitioningKind::Balanced => {
                    let upperbound = root.partitions().len();
                    let pid = auxilary.get_next_partition_id(upperbound);
                    Ok(pid)
                }
                PartitioningKind::PartitionId => Ok(u32::from_le_bytes(
                    partitioning.value[..partitioning.length as usize]
                        .try_into()
                        .map_err(|_| IggyError::InvalidNumberEncoding)?,
                ) as usize),
                PartitioningKind::MessagesKey => {
                    let upperbound = root.partitions().len();
                    Ok(
                        topics::helpers::calculate_partition_id_by_messages_key_hash(
                            upperbound,
                            &partitioning.value,
                        ),
                    )
                }
            },
        )?;

        let future = SendWrapper::new(self.shard().append_messages(
            user_id,
            stream_id,
            topic_id,
            partition_id,
            batch,
        ));
        future.await
    }
}
