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
use crate::slab::traits_ext::{DeleteCell, EntityMarker, InsertCell};

use crate::streaming::session::Session;
use crate::streaming::stats::stats::StreamStats;
use crate::streaming::streams::storage2::{create_stream_file_hierarchy, delete_stream_from_disk};
use crate::streaming::streams::{self, stream2};
use error_set::ErrContext;

use iggy_common::locking::IggyRwLockFn;
use iggy_common::{Identifier, IggyError, IggyTimestamp};
use std::sync::Arc;

impl IggyShard {
    pub async fn create_stream2(
        &self,
        session: &Session,
        _stream_id: Option<u32>,
        name: String,
    ) -> Result<stream2::Stream, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .create_stream(session.get_user_id())?;
        let exists = self
            .streams2
            .exists(&Identifier::from_str_value(&name).unwrap());

        if exists {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }
        let stream = self.create_and_insert_stream_mem(name);
        self.metrics.increment_streams(1);
        create_stream_file_hierarchy(self.id, stream.id(), &self.config.system).await?;
        Ok(stream)
    }

    fn create_and_insert_stream_mem(&self, name: String) -> stream2::Stream {
        let now = IggyTimestamp::now();
        let stats = Arc::new(StreamStats::new());
        let mut stream = stream2::Stream::new(name, stats, now);
        let id = self.streams2.insert(stream.clone());
        stream.update_id(id);
        stream
    }

    pub fn create_stream2_bypass_auth(&self, stream: stream2::Stream) -> usize {
        self.streams2.insert(stream)
    }

    pub fn update_stream2_bypass_auth(&self, id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.update_stream2_base(id, name.to_string())?;
        Ok(())
    }

    pub fn update_stream2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        let id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());

        self.permissioner
            .borrow()
            .update_stream(session.get_user_id(), id as u32)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;
        self.update_stream2_base(stream_id, name)?;
        Ok(())
    }

    fn update_stream2_base(&self, id: &Identifier, name: String) -> Result<(), IggyError> {
        let old_name = self
            .streams2
            .with_stream_by_id(id, streams::helpers::get_stream_name());

        if old_name == name {
            return Ok(());
        }
        if self.streams2.with_index(|index| index.contains_key(&name)) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_string()));
        }

        self.streams2
            .with_stream_by_id_mut(id, streams::helpers::update_stream_name(name.clone()));
        self.streams2.with_index_mut(|index| {
            // Rename the key inside of hashmap
            let idx = index.remove(&old_name).expect("Rename key: key not found");
            index.insert(name, idx);
        });
        Ok(())
    }

    pub fn delete_stream2_bypass_auth(&self, id: &Identifier) -> stream2::Stream {
        let stream = self.delete_stream2_base(id);
        Ok(stream)
    }

    fn delete_stream2_base(&self, id: &Identifier) -> stream2::Stream {
        let stream_index = self.streams2.get_index(id);
        let stream = self.streams2.delete(stream_index);
        let stats = stream.stats();

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(0); // TODO: stats doesn't have topic count
        self.metrics.decrement_partitions(0); // TODO: stats doesn't have partition count
        self.metrics
            .decrement_messages(stats.messages_count_inconsistent());
        self.metrics
            .decrement_segments(stats.segments_count_inconsistent());

        /*
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_stream(stream_id as u32);
        */
        stream
    }

    pub async fn delete_stream2(
        &self,
        session: &Session,
        id: &Identifier,
    ) -> Result<stream2::Stream, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(id)?;
        let stream_id = self
            .streams2
            .with_stream_by_id(id, streams::helpers::get_stream_id());
        self.permissioner
            .borrow()
            .delete_stream(session.get_user_id(), stream_id as u32)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id,
                )
            })?;
        let mut stream = self.delete_stream2_base(id);
        delete_stream_from_disk(self.id, &mut stream, &self.config.system).await?;
        Ok(stream)
    }

    pub fn purge_stream2(&self, session: &Session, id: &Identifier) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        // self.ensure_stream_exists(id)?;
        let get_stream_id = crate::streaming::streams::helpers::get_stream_id();
        let stream_id = self.streams2.with_stream_by_id(id, get_stream_id);
        self.permissioner
            .borrow()
            .purge_stream(session.get_user_id(), stream_id as u32)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id,
                )
            })?;
        self.purge_stream2_base(id)?;
        Ok(())
    }

    pub fn purge_stream2_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.purge_stream2_base(stream_id)?;
        Ok(())
    }

    fn purge_stream2_base(&self, _stream_id: &Identifier) -> Result<(), IggyError> {
        // TODO
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
