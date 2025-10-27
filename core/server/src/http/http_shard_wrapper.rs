use std::rc::Rc;

use iggy_common::{
    Consumer, ConsumerOffsetInfo, Identifier, IggyError, IggyExpiry, Partitioning, Permissions,
    Stats, UserId, UserStatus,
};
use send_wrapper::SendWrapper;

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::shard::system::messages::PollingArgs;
use crate::state::command::EntryCommand;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
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
        consumer: Consumer,
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
        let _result = future.await?;
        Ok(())
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
        let _result = future.await?;
        Ok(())
    }

    pub async fn delete_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().delete_stream(session, stream_id));
        future.await?;
        Ok(())
    }

    pub fn update_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        self.shard().update_stream(session, stream_id, name)
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let future = SendWrapper::new(self.shard().purge_stream(session, stream_id));
        future.await
    }

    pub async fn create_stream(
        &self,
        session: &Session,
        name: String,
    ) -> Result<crate::streaming::streams::stream::Stream, IggyError> {
        let future = SendWrapper::new(self.shard().create_stream(session, name));
        future.await
    }

    pub async fn apply_state(
        &self,
        user_id: UserId,
        command: &EntryCommand,
    ) -> Result<(), IggyError> {
        self.shard().state.apply(user_id, command).await
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
        let future = SendWrapper::new(self.shard().append_messages(
            user_id,
            stream_id,
            topic_id,
            partitioning,
            batch,
        ));
        future.await
    }
}
