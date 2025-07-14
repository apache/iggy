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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::streaming::partitions::partition;
use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use error_set::ErrContext;
use futures::future::try_join_all;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{IdKind, Identifier, IggyError};
use std::cell::{Ref, RefCell, RefMut};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::fs;
use tracing::{error, info, warn};

static CURRENT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

impl IggyShard {
    pub fn get_streams(&self) -> Vec<Ref<'_, Stream>> {
        let len = self.streams.borrow().len();
        let result = (0..len)
            .map(|i| {
                Ref::map(self.streams.borrow(), |streams| {
                    streams.values().nth(i).unwrap()
                })
            })
            .collect();
        result
    }

    pub fn find_streams(&self, session: &Session) -> Result<Vec<Ref<'_, Stream>>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
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
    ) -> Result<Ref<'_, Stream>, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(identifier);
        if let Ok(stream) = stream {
            self.permissioner
            .borrow()
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
    ) -> Result<Option<Ref<'_, Stream>>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(stream) = self.try_get_stream(identifier)? else {
            return Ok(None);
        };

        self.permissioner
        .borrow()
            .get_stream(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get stream with ID: {identifier} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(Some(stream))
    }

    pub fn try_get_stream(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<Ref<'_, Stream>>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value()?).map(Some),
            IdKind::String => Ok(self.try_get_stream_by_name(&identifier.get_cow_str_value()?)),
        }
    }

    pub fn try_get_topic_id(&self, stream_id: &Identifier, name: &str) -> Option<u32> {
        let stream = self.get_stream(stream_id).ok()?;
        stream.topics_ids.get(name).and_then(|id| Some(*id))
    }

    pub fn try_get_stream_id(&self, name: &str) -> Option<u32> {
        self.streams_ids.borrow().get(name).and_then(|id| Some(*id))
    }

    fn try_get_stream_by_name(&self, name: &str) -> Option<Ref<'_, Stream>> {
        self.streams_ids
            .borrow()
            .get(name)
            .and_then(|id| Some(self.get_stream_ref(*id)))
    }

    pub fn get_stream(&self, identifier: &Identifier) -> Result<Ref<'_, Stream>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_stream_mut(&self, identifier: &Identifier) -> Result<RefMut<'_, Stream>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_stream_by_name(&self, name: &str) -> Result<Ref<'_, Stream>, IggyError> {
        let exists = self.streams_ids.borrow().iter().any(|s| s.0 == name);
        if !exists {
            return Err(IggyError::StreamNameNotFound(name.to_string()));
        }
        let stream_id = self.streams_ids.borrow().get(name).cloned().unwrap();
        self.get_stream_by_id(stream_id)
    }

    fn get_stream_by_id(&self, stream_id: u32) -> Result<Ref<'_, Stream>, IggyError> {
        let exists = self.streams.borrow().iter().any(|s| s.0 == &stream_id);
        if !exists {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }
        Ok(self.get_stream_ref(stream_id))
    }

    fn get_stream_ref(&self, stream_id: u32) -> Ref<'_, Stream> {
        Ref::map(self.streams.borrow(), |streams| {
            streams.get(&stream_id).expect("Stream ID not found")
        })
    }

    fn get_stream_by_name_mut(&self, name: &str) -> Result<RefMut<'_, Stream>, IggyError> {
        let exists = self.streams_ids.borrow().iter().any(|s| s.0 == name);
        if !exists {
            return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
        }
        let streams_ids = self.streams_ids.borrow();
        let id = streams_ids.get(name).cloned();
        drop(streams_ids);
        self.get_stream_by_id_mut(id.unwrap())
    }

    fn get_stream_by_id_mut(&self, stream_id: u32) -> Result<RefMut<'_, Stream>, IggyError> {
        let exists = self.streams.borrow().iter().any(|s| s.0 == &stream_id);
        if !exists {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }
        Ok(RefMut::map(self.streams.borrow_mut(), |s| {
            s.get_mut(&stream_id).unwrap()
        }))
    }

    pub fn create_stream_bypass_auth(
        &self,
        stream_id: Option<u32>,
        name: &str,
    ) -> Result<(), IggyError> {
        let stream = self.create_stream_base(stream_id, name)?;
        let id = stream.stream_id;
        self.streams_ids.borrow_mut().insert(name.to_owned(), id);
        self.streams.borrow_mut().insert(id, stream);
        self.metrics.increment_streams(1);
        Ok(())
    }

    pub async fn create_stream(
        &self,
        session: &Session,
        stream_id: Option<u32>,
        name: &str,
    ) -> Result<Identifier, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .create_stream(session.get_user_id())?;
        if self.streams_ids.borrow().contains_key(name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
        }
        let stream = self
            .create_stream_base(stream_id, name)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to create stream with name: {name}")
            })?;
        let id = stream.stream_id;

        stream.persist().await?;
        info!("Created stream with ID: {id}, name: '{name}'.");
        self.streams_ids.borrow_mut().insert(name.to_owned(), id);
        self.streams.borrow_mut().insert(id, stream);
        self.metrics.increment_streams(1);
        Ok(Identifier::numeric(id)?)
    }

    fn create_stream_base(&self, stream_id: Option<u32>, name: &str) -> Result<Stream, IggyError> {
        if self.streams_ids.borrow().contains_key(name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
        }

        let mut id;
        if stream_id.is_none() {
            id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.streams.borrow().contains_key(&id) {
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

        if self.streams.borrow().contains_key(&id) {
            return Err(IggyError::StreamIdAlreadyExists(id));
        }

        let stream = Stream::create(id, name, self.config.system.clone(), self.storage.clone());
        Ok(stream)
    }

    pub fn update_stream_bypass_auth(&self, id: &Identifier, name: &str) -> Result<(), IggyError> {
        let stream_id;
        {
            let stream = self.get_stream(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {id}")
            })?;
            stream_id = stream.stream_id;
        }

        let old_name = self.update_stream_base(stream_id, id, name)?;
        info!("Stream with ID '{id}' updated. Old name: '{old_name}' changed to: '{name}'.");
        Ok(())
    }

    pub async fn update_stream(
        &self,
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
        .borrow()
            .update_stream(session.get_user_id(), stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;

        let old_name = self.update_stream_base(stream_id, id, name)?;
        let stream = self.get_stream(id)?;
        stream.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist stream with ID: {stream_id}, name: {name}")
        })?;

        info!("Stream with ID '{id}' updated. Old name: '{old_name}' changed to: '{name}'.");
        Ok(())
    }

    fn update_stream_base(
        &self,
        stream_id: u32,
        id: &Identifier,
        name: &str,
    ) -> Result<String, IggyError> {
        if let Some(stream_id_by_name) = self.streams_ids.borrow().get(name) {
            if *stream_id_by_name != stream_id {
                return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
            }
        }
        let old_name;
        {
            let mut stream = self.get_stream_mut(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {id}")
            })?;
            old_name = stream.name.clone();
            stream.name = name.to_owned();
            // Drop the exclusive borrow.
        }

        {
            self.streams_ids.borrow_mut().remove(&old_name);
            self.streams_ids
                .borrow_mut()
                .insert(name.to_owned(), stream_id);
        }
        Ok(old_name)
    }

    pub fn delete_stream_bypass_auth(&self, id: &Identifier) -> Result<Stream, IggyError> {
        let stream = self.get_stream(id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {id}")
        })?;
        let stream_id = stream.stream_id;
        let stream_name = stream.name.clone();
        let stream = self.delete_stream_base(stream_id, stream_name)?;
        Ok(stream)
    }

    pub fn delete_stream(&self, session: &Session, id: &Identifier) -> Result<Stream, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {id}")
        })?;
        let stream_id = stream.stream_id;
        let stream_name = stream.name.clone();
        self.permissioner
            .borrow()
            .delete_stream(session.get_user_id(), stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream.stream_id,
                )
            })?;
        let stream = self.delete_stream_base(stream_id, stream_name)?;
        Ok(stream)
    }

    fn delete_stream_base(&self, stream_id: u32, stream_name: String) -> Result<Stream, IggyError> {
        let stream = self.streams.borrow_mut().remove(&stream_id).unwrap();
        self.streams_ids.borrow_mut().remove(&stream_name);

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(stream.get_topics_count());
        self.metrics
            .decrement_partitions(stream.get_partitions_count());
        self.metrics.decrement_messages(stream.get_messages_count());
        self.metrics.decrement_segments(stream.get_segments_count());
        let current_stream_id = CURRENT_STREAM_ID.load(Ordering::SeqCst);
        if current_stream_id > stream_id {
            CURRENT_STREAM_ID.store(stream_id, Ordering::SeqCst);
        }

        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_stream(stream_id);
        Ok(stream)
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
            .borrow()
            .purge_stream(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream.stream_id,
                )
            })?;
        self.purge_stream_base(stream.stream_id).await?;
        Ok(())
    }

    pub async fn purge_stream_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        self.purge_stream_base(stream.stream_id).await?;
        Ok(())
    }

    async fn purge_stream_base(&self, stream_id: u32) -> Result<(), IggyError> {
        let stream = self.get_stream_ref(stream_id);
        for topic in stream.get_topics() {
            let topic_id = topic.topic_id;
            for partition in topic.get_partitions() {
                let mut partition = partition.write().await;
                let partition_id = partition.partition_id;
                let namespace = IggyNamespace::new(stream_id, topic_id, partition_id);
                let shard_info = self.find_shard_table_record(&namespace).unwrap();
                if shard_info.id() == self.id {
                    partition.purge().await?;
                }
            }
        }
        Ok(())
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

    //TODO: Fixme
    /*
    #[tokio::test]
    async fn should_get_stream_by_id_and_name() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Rc::new(SystemConfig {
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
    */
}
