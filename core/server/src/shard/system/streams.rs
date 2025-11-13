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
use crate::streaming::streams::storage::{create_stream_file_hierarchy, delete_stream_from_disk};
use crate::streaming::streams::{self, stream};
use err_trail::ErrContext;
use iggy_common::{Identifier, IggyError};

impl IggyShard {
    pub async fn create_stream(
        &self,
        session: &Session,
        name: String,
    ) -> Result<stream::Stream, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .create_stream(session.get_user_id())?;
        let exists = self
            .streams
            .exists(&Identifier::from_str_value(&name).unwrap());

        if exists {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }
        let stream = stream::create_and_insert_stream_mem(&self.streams, name);
        self.metrics.increment_streams(1);
        create_stream_file_hierarchy(stream.id(), &self.config.system).await?;
        Ok(stream)
    }

    pub fn create_stream_bypass_auth(&self, stream: stream::Stream) -> usize {
        self.streams.insert(stream)
    }

    pub fn update_stream_bypass_auth(&self, id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.update_stream_base(id, name.to_string())?;
        Ok(())
    }

    pub fn update_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        let id = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());

        self.permissioner
            .borrow()
            .update_stream(session.get_user_id(), id)
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;
        self.update_stream_base(stream_id, name)?;
        Ok(())
    }

    fn update_stream_base(&self, id: &Identifier, name: String) -> Result<(), IggyError> {
        let old_name = self
            .streams
            .with_stream_by_id(id, streams::helpers::get_stream_name());

        if old_name == name {
            return Ok(());
        }
        if self.streams.with_index(|index| index.contains_key(&name)) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_string()));
        }

        self.streams
            .with_stream_by_id_mut(id, streams::helpers::update_stream_name(name.clone()));
        self.streams.with_index_mut(|index| {
            // Rename the key inside of hashmap
            let idx = index.remove(&old_name).expect("Rename key: key not found");
            index.insert(name, idx);
        });
        Ok(())
    }

    pub fn delete_stream_bypass_auth(&self, id: &Identifier) -> stream::Stream {
        self.delete_stream_base(id)
    }

    fn delete_stream_base(&self, id: &Identifier) -> stream::Stream {
        let stream_index = self.streams.get_index(id);
        let stream = self.streams.delete(stream_index);
        let stats = stream.stats();

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(0);
        self.metrics.decrement_partitions(0);
        self.metrics
            .decrement_messages(stats.messages_count_inconsistent());
        self.metrics
            .decrement_segments(stats.segments_count_inconsistent());
        stream
    }

    pub async fn delete_stream(
        &self,
        session: &Session,
        id: &Identifier,
    ) -> Result<stream::Stream, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(id)?;
        let stream_id = self
            .streams
            .with_stream_by_id(id, streams::helpers::get_stream_id());
        self.permissioner
            .borrow()
            .delete_stream(session.get_user_id(), stream_id)
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id,
                )
            })?;
        let mut stream = self.delete_stream_base(id);
        let stream_id_usize = stream.id();

        // Clean up consumer groups from ClientManager for this stream
        self.client_manager
            .delete_consumer_groups_for_stream(stream_id_usize);

        // Remove all entries from shards_table for this stream (all topics and partitions)
        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == stream_id_usize {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        delete_stream_from_disk(&mut stream, &self.config.system).await?;
        Ok(stream)
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        {
            let get_stream_id = crate::streaming::streams::helpers::get_stream_id();
            let stream_id = self.streams.with_stream_by_id(stream_id, get_stream_id);
            self.permissioner
                .borrow()
                .purge_stream(session.get_user_id(), stream_id)
                .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id,
                )
            })?;
        }

        self.purge_stream_base(stream_id).await
    }

    pub async fn purge_stream_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.purge_stream_base(stream_id).await?;
        Ok(())
    }

    async fn purge_stream_base(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        // Get all topic IDs in the stream
        let topic_ids = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_topic_ids());

        // Purge each topic in the stream using bypass auth
        for topic_id in topic_ids {
            let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();
            self.purge_topic_bypass_auth(stream_id, &topic_identifier)
                .await?;
        }

        Ok(())
    }
}
