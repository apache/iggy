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

use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use error_set::ErrContext;
use futures::future::try_join_all;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::fs;
use tracing::{error, info, warn};


impl IggyShard {
    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect()
    }

    pub fn find_streams(&self, session: &Session) -> Result<Vec<&Stream>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .get_streams(session.get_user_id())
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get streams for user {}",
                    session.get_user_id(),
                )
            })?;
        Ok(self.get_streams())
    }

    pub fn find_stream(
        &self,
        session: &Session,
        identifier: &Identifier,
    ) -> Result<&Stream, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(identifier);
        if let Ok(stream) = stream {
            self.permissioner
                .get_stream(session.get_user_id(), stream.stream_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to get stream for user {}",
                        session.get_user_id(),
                    )
                })?;
            return Ok(stream);
        }

        stream
    }

    pub fn try_find_stream(
        &self,
        session: &Session,
        identifier: &Identifier,
    ) -> Result<Option<&Stream>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(stream) = self.try_get_stream(identifier)? else {
            return Ok(None);
        };

        self.permissioner
            .get_stream(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get stream with ID: {identifier} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(Some(stream))
    }

    pub fn try_get_stream(&self, identifier: &Identifier) -> Result<Option<&Stream>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => Ok(self.streams.get(&identifier.get_u32_value()?)),
            IdKind::String => Ok(self.try_get_stream_by_name(&identifier.get_cow_str_value()?)),
        }
    }

    fn try_get_stream_by_name(&self, name: &str) -> Option<&Stream> {
        self.streams_ids
            .get(name)
            .and_then(|id| self.streams.get(id))
    }

    pub fn get_stream(&self, identifier: &Identifier) -> Result<&Stream, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_stream_mut(&mut self, identifier: &Identifier) -> Result<&mut Stream, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_stream_by_name(&self, name: &str) -> Result<&Stream, IggyError> {
        let stream_id = self.streams_ids.get(name);
        if stream_id.is_none() {
            return Err(IggyError::StreamNameNotFound(name.to_string()));
        }

        self.get_stream_by_id(*stream_id.unwrap())
    }

    fn get_stream_by_id(&self, stream_id: u32) -> Result<&Stream, IggyError> {
        let stream = self.streams.get(&stream_id);
        if stream.is_none() {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    fn get_stream_by_name_mut(&mut self, name: &str) -> Result<&mut Stream, IggyError> {
        let stream_id;
        {
            let id = self.streams_ids.get_mut(name);
            if id.is_none() {
                return Err(IggyError::StreamNameNotFound(name.to_string()));
            }

            stream_id = *id.unwrap();
        }

        self.get_stream_by_id_mut(stream_id)
    }

    fn get_stream_by_id_mut(&mut self, stream_id: u32) -> Result<&mut Stream, IggyError> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    pub async fn create_stream(
        &mut self,
        session: &Session,
        stream_id: Option<u32>,
        name: &str,
    ) -> Result<&Stream, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.create_stream(session.get_user_id())?;
        if self.streams_ids.contains_key(name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
        }

        let mut id;
        if stream_id.is_none() {
            id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.streams.contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::StreamIdAlreadyExists(id));
                    }
                    id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = stream_id.unwrap();
        }

        if self.streams.contains_key(&id) {
            return Err(IggyError::StreamIdAlreadyExists(id));
        }

        let stream = Stream::create(id, name, self.config.clone(), self.storage.clone());
        stream.persist().await?;
        info!("Created stream with ID: {id}, name: '{name}'.");
        self.streams_ids.insert(name.to_owned(), stream.stream_id);
        self.streams.insert(stream.stream_id, stream);
        self.metrics.increment_streams(1);
        self.get_stream_by_id(id)
    }

    pub async fn update_stream(
        &mut self,
        session: &Session,
        id: &Identifier,
        name: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id;
        {
            let stream = self.get_stream(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {id}")
            })?;
            stream_id = stream.stream_id;
        }

        self.permissioner
            .update_stream(session.get_user_id(), stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;

        {
            if let Some(stream_id_by_name) = self.streams_ids.get(name) {
                if *stream_id_by_name != stream_id {
                    return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
                }
            }
        }

        let old_name;
        {
            let stream = self.get_stream_mut(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {id}")
            })?;
            old_name = stream.name.clone();
            stream.name = name.to_owned();
            stream.persist().await?;
        }

        {
            self.streams_ids.remove(&old_name);
            self.streams_ids.insert(name.to_owned(), stream_id);
        }

        info!("Stream with ID '{id}' updated. Old name: '{old_name}' changed to: '{name}'.");
        Ok(())
    }

    pub async fn delete_stream(
        &mut self,
        session: &Session,
        id: &Identifier,
    ) -> Result<u32, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {id}")
        })?;
        let stream_id = stream.stream_id;
        self.permissioner
            .delete_stream(session.get_user_id(), stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream.stream_id,
                )
            })?;
        let stream_name = stream.name.clone();
        if stream.delete().await.is_err() {
            return Err(IggyError::CannotDeleteStream(stream_id));
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(stream.get_topics_count());
        self.metrics
            .decrement_partitions(stream.get_partitions_count());
        self.metrics.decrement_messages(stream.get_messages_count());
        self.metrics.decrement_segments(stream.get_segments_count());
        self.streams.remove(&stream_id);
        self.streams_ids.remove(&stream_name);
        let current_stream_id = CURRENT_STREAM_ID.load(Ordering::SeqCst);
        if current_stream_id > stream_id {
            CURRENT_STREAM_ID.store(stream_id, Ordering::SeqCst);
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_stream(stream_id)
            .await;
        Ok(stream_id)
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        self.permissioner
            .purge_stream(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream.stream_id,
                )
            })?;
        stream.purge().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
    use crate::configs::system::SystemConfig;
    use crate::state::{MockState, StateKind};
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use crate::streaming::users::user::User;
    use iggy_common::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
    use std::{
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    #[tokio::test]
    async fn should_get_stream_by_id_and_name() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        );

        let stream_id = 1;
        let stream_name = "test";
        let mut system = System::create(
            config,
            storage,
            Arc::new(StateKind::Mock(MockState::new())),
            None,
            DataMaintenanceConfig::default(),
            PersonalAccessTokenConfig::default(),
        );
        let root = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        let permissions = root.permissions.clone();
        let session = Session::new(
            1,
            root.id,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234),
        );
        system
            .permissioner
            .init_permissions_for_user(root.id, permissions);
        system
            .create_stream(&session, Some(stream_id), stream_name)
            .await
            .unwrap();

        let stream = system.get_stream(&Identifier::numeric(stream_id).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.stream_id, stream_id);
        assert_eq!(stream.name, stream_name);

        let stream = system.get_stream(&Identifier::named(stream_name).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.stream_id, stream_id);
        assert_eq!(stream.name, stream_name);
    }
}
