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
use crate::streaming::streams::storage2::{create_stream_file_hierarchy, delete_stream_from_disk};
use crate::streaming::streams::{self, stream2};
use error_set::ErrContext;

use iggy_common::{Identifier, IggyError};

impl IggyShard {
    pub async fn create_stream2(
        &self,
        session: &Session,
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
        let stream = stream2::create_and_insert_stream_mem(&self.streams2, name);
        self.metrics.increment_streams(1);
        create_stream_file_hierarchy(self.id, stream.id(), &self.config.system).await?;
        Ok(stream)
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
        self.delete_stream2_base(id)
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
        // Clean up consumer groups from ClientManager for this stream
        let stream_id_usize = stream.id();
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_stream(stream_id_usize);
        delete_stream_from_disk(self.id, &mut stream, &self.config.system).await?;
        Ok(stream)
    }

    pub async fn purge_stream2(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        {
            let get_stream_id = crate::streaming::streams::helpers::get_stream_id();
            let stream_id = self.streams2.with_stream_by_id(stream_id, get_stream_id);
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
        }

        self.purge_stream2_base(stream_id).await
    }

    pub async fn purge_stream2_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.purge_stream2_base(stream_id).await?;
        Ok(())
    }

    async fn purge_stream2_base(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        // Get all topic IDs in the stream
        let topic_ids = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_topic_ids());

        // Purge each topic in the stream using bypass auth
        for topic_id in topic_ids {
            let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();
            self.purge_topic2_bypass_auth(stream_id, &topic_identifier)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::server::ServerConfig;
    use crate::shard::ShardInfo;
    use crate::shard::transmission::connector::ShardConnector;
    use crate::slab::streams::Streams;
    use crate::state::{MockState, StateKind};
    use crate::streaming::session::Session;
    use crate::streaming::streams;
    use crate::streaming::users::user::User;
    use crate::streaming::utils::ptr::EternalPtr;
    use crate::versioning::SemanticVersion;
    use ahash::HashMap;
    use dashmap::DashMap;
    use iggy_common::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::rc::Rc;

    fn create_test_shard() -> Rc<IggyShard> {
        let _tempdir = tempfile::TempDir::new().unwrap();
        let config = ServerConfig::default();

        let streams = Streams::default();
        let shards_table = Box::new(DashMap::new());
        let shards_table = Box::leak(shards_table);
        let shards_table: EternalPtr<DashMap<crate::shard::namespace::IggyNamespace, ShardInfo>> =
            shards_table.into();

        let users = HashMap::default();
        let state = StateKind::Mock(MockState::new());

        // Create single shard connection
        let connections = vec![ShardConnector::new(0, 1)];

        let builder = IggyShard::builder();
        let shard = builder
            .id(0)
            .streams(streams)
            .shards_table(shards_table)
            .state(state)
            .users(users)
            .connections(connections)
            .config(config)
            .encryptor(None)
            .version(SemanticVersion::current().unwrap())
            .build();

        Rc::new(shard)
    }

    #[compio::test]
    async fn should_create_and_get_stream_by_id_and_name() {
        let shard = create_test_shard();

        let stream_name = "test_stream";

        // Initialize root user and session
        let root = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        let session = Session::new(
            1,
            root.id,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234),
        );

        shard.users.borrow_mut().insert(root.id, root);
        shard
            .permissioner
            .borrow_mut()
            .init(&[&User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)]);

        // Create stream
        let stream = shard
            .create_stream2(&session, stream_name.to_string())
            .await
            .unwrap();

        let stream_id = stream.id();
        assert_eq!(stream.root().name(), stream_name);

        // Verify stream exists by ID
        assert!(
            shard
                .streams2
                .exists(&Identifier::numeric(stream_id as u32).unwrap())
        );

        // Verify stream exists by name
        assert!(
            shard
                .streams2
                .exists(&Identifier::from_str_value(stream_name).unwrap())
        );

        // Verify we can access stream data by ID
        let retrieved_name = shard.streams2.with_stream_by_id(
            &Identifier::numeric(stream_id as u32).unwrap(),
            streams::helpers::get_stream_name(),
        );
        assert_eq!(retrieved_name, stream_name);

        // Verify we can access stream data by name
        let retrieved_id = shard.streams2.with_stream_by_id(
            &Identifier::from_str_value(stream_name).unwrap(),
            streams::helpers::get_stream_id(),
        );
        assert_eq!(retrieved_id, stream_id);
    }

    #[compio::test]
    async fn should_update_stream_name() {
        let shard = create_test_shard();

        let initial_name = "initial_stream";
        let updated_name = "updated_stream";

        // Initialize root user and session
        let root = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        let session = Session::new(
            1,
            root.id,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234),
        );

        shard.users.borrow_mut().insert(root.id, root);
        shard
            .permissioner
            .borrow_mut()
            .init(&[&User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)]);

        // Create stream
        let stream = shard
            .create_stream2(&session, initial_name.to_string())
            .await
            .unwrap();

        let stream_id = stream.id();

        // Update stream name
        shard
            .update_stream2(
                &session,
                &Identifier::numeric(stream_id as u32).unwrap(),
                updated_name.to_string(),
            )
            .unwrap();

        // Verify old name doesn't exist
        assert!(
            !shard
                .streams2
                .exists(&Identifier::from_str_value(initial_name).unwrap())
        );

        // Verify new name exists
        assert!(
            shard
                .streams2
                .exists(&Identifier::from_str_value(updated_name).unwrap())
        );

        // Verify stream data
        let retrieved_name = shard.streams2.with_stream_by_id(
            &Identifier::numeric(stream_id as u32).unwrap(),
            streams::helpers::get_stream_name(),
        );
        assert_eq!(retrieved_name, updated_name);
    }

    #[compio::test]
    async fn should_delete_stream() {
        let shard = create_test_shard();

        let stream_name = "to_be_deleted";

        // Initialize root user and session
        let root = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        let session = Session::new(
            1,
            root.id,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234),
        );

        shard.users.borrow_mut().insert(root.id, root);
        shard
            .permissioner
            .borrow_mut()
            .init(&[&User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)]);

        // Create stream
        let stream = shard
            .create_stream2(&session, stream_name.to_string())
            .await
            .unwrap();

        let stream_id = stream.id();

        // Verify stream exists
        assert!(
            shard
                .streams2
                .exists(&Identifier::numeric(stream_id as u32).unwrap())
        );

        // Delete stream
        let deleted_stream = shard
            .delete_stream2(&session, &Identifier::numeric(stream_id as u32).unwrap())
            .await
            .unwrap();

        assert_eq!(deleted_stream.id(), stream_id);
        assert_eq!(deleted_stream.root().name(), stream_name);

        // Verify stream no longer exists
        assert!(
            !shard
                .streams2
                .exists(&Identifier::numeric(stream_id as u32).unwrap())
        );
        assert!(
            !shard
                .streams2
                .exists(&Identifier::from_str_value(stream_name).unwrap())
        );
    }
}
