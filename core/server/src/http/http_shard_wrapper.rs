use std::cell::Ref;
use std::rc::Rc;

use iggy_common::{
    CompressionAlgorithm, Consumer, ConsumerOffsetInfo, Identifier, IggyError, IggyExpiry,
    MaxTopicSize, Partitioning, Permissions, Stats, UserId, UserStatus,
};
use send_wrapper::SendWrapper;

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::system::messages::PollingArgs;
use crate::state::command::EntryCommand;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::user::User;
use crate::{shard::IggyShard, streaming::session::Session};

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
        session: &SendWrapper<Session>,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let future = SendWrapper::new(self.shard().get_consumer_offset(
            session,
            consumer,
            stream_id,
            topic_id,
            partition_id,
        ));
        future.await
    }

    pub async fn store_consumer_offset(
        &self,
        session: &SendWrapper<Session>,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().store_consumer_offset(
            session,
            consumer,
            stream_id,
            topic_id,
            partition_id,
            offset,
        ));
        future.await
    }

    pub async fn delete_consumer_offset(
        &self,
        session: &SendWrapper<Session>,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().delete_consumer_offset(
            session,
            consumer,
            stream_id,
            topic_id,
            partition_id,
        ));
        future.await
    }

    pub fn get_streams(&self) -> Vec<Ref<'_, Stream>> {
        self.shard().get_streams()
    }

    pub fn get_stream(&self, stream_id: &Identifier) -> Result<Ref<'_, Stream>, IggyError> {
        self.shard().get_stream(stream_id)
    }

    pub async fn delete_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<Stream, IggyError> {
        self.shard().delete_stream(session, stream_id)
    }

    pub async fn update_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: &str,
    ) -> Result<(), IggyError> {
        self.shard().update_stream(session, stream_id, name).await
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.shard().purge_stream(session, stream_id).await
    }

    // pub fn get_topics(
    //     &self,
    //     session: &Session,
    //     stream_id: &Identifier,
    // ) -> Result<Vec<&Topic>, IggyError> {
    //     self.shard().get_topics(session, stream_id)
    // }

    pub fn find_topic<'topic, 'stream>(
        &self,
        session: &Session,
        stream: &'stream Stream,
        topic_id: &Identifier,
    ) -> Result<&'topic Topic, IggyError>
    where
        'stream: 'topic,
    {
        self.shard().find_topic(session, stream, topic_id)
    }

    pub async fn create_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(Identifier, Vec<u32>), IggyError> {
        self.shard()
            .create_topic(
                session,
                stream_id,
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            )
            .await
    }

    pub async fn delete_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Topic, IggyError> {
        self.shard()
            .delete_topic(session, stream_id, topic_id)
            .await
    }

    pub async fn update_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.shard()
            .update_topic(
                session,
                stream_id,
                topic_id,
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            )
            .await
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.shard().purge_topic(session, stream_id, topic_id).await
    }

    pub async fn get_users(&self, session: &Session) -> Result<Vec<User>, IggyError> {
        self.shard().get_users(session).await
    }

    pub fn create_user(
        &self,
        session: &Session,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<User, IggyError> {
        self.shard()
            .create_user(session, username, password, status, permissions)
    }

    pub fn delete_user(&self, session: &Session, user_id: &Identifier) -> Result<User, IggyError> {
        self.shard().delete_user(session, user_id)
    }

    pub fn update_user(
        &self,
        session: &Session,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        self.shard().update_user(session, user_id, username, status)
    }

    pub fn update_permissions(
        &self,
        session: &Session,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.shard()
            .update_permissions(session, user_id, permissions)
    }

    pub async fn change_password(
        &self,
        session: &Session,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.shard()
            .change_password(session, user_id, current_password, new_password)
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
        session: &Session,
    ) -> Result<Vec<PersonalAccessToken>, IggyError> {
        self.shard().get_personal_access_tokens(session)
    }

    pub fn create_personal_access_token(
        &self,
        session: &Session,
        name: &str,
        expiry: IggyExpiry,
    ) -> Result<(PersonalAccessToken, String), IggyError> {
        self.shard()
            .create_personal_access_token(session, name, expiry)
    }

    pub fn delete_personal_access_token(
        &self,
        session: &Session,
        name: &str,
    ) -> Result<(), IggyError> {
        self.shard().delete_personal_access_token(session, name)
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

    pub async fn poll_messages(
        &self,
        client_id: u32,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
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

    pub fn get_consumer_group<'cg, 'stream>(
        &self,
        session: &Session,
        stream: &'stream Stream,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<Ref<'cg, ConsumerGroup>>, IggyError>
    where
        'stream: 'cg,
    {
        self.shard()
            .get_consumer_group(session, stream, topic_id, group_id)
    }

    pub fn get_consumer_groups(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.shard()
            .get_consumer_groups(session, stream_id, topic_id)
    }

    pub fn create_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<Identifier, IggyError> {
        self.shard()
            .create_consumer_group(session, stream_id, topic_id, group_id, name)
    }

    pub async fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        // Wrap the entire operation in SendWrapper since it's async
        let future = SendWrapper::new(self.shard().delete_consumer_group(
            session,
            stream_id,
            topic_id,
            consumer_group_id,
        ));
        future.await
    }

    pub async fn append_messages(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().append_messages(
            user_id,
            stream_id,
            topic_id,
            partitioning,
            batch,
        ));
        future.await
    }

    pub async fn create_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.shard()
            .create_partitions(session, stream_id, topic_id, partitions_count)
            .await
    }

    pub async fn delete_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        let future = SendWrapper::new(self.shard().delete_partitions(
            session,
            stream_id,
            topic_id,
            partitions_count,
        ));
        future.await
    }

    pub fn try_find_stream(
        &self,
        session: &Session,
        identifier: &Identifier,
    ) -> Result<Option<Ref<'_, Stream>>, IggyError> {
        self.shard().try_find_stream(session, identifier)
    }
}
