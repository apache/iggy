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

use crate::binary::handlers::topics::get_topic_handler;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::topics::{COMPONENT, consumer_group};
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::locking::IggySharedMutFn;
use iggy_common::{IdKind, Identifier};
use std::cell::{Ref, RefMut};
use std::sync::atomic::Ordering;
use tracing::info;

impl Topic {
    pub fn reassign_consumer_groups(&mut self) {
        if self.consumer_groups.borrow().is_empty() {
            return;
        }

        let partitions_count = self.partitions.len() as u32;
        info!(
            "Reassigning consumer groups for topic with ID: {} for stream with ID with {}, partitions count: {}",
            self.topic_id, self.stream_id, partitions_count
        );
        for (_, consumer_group) in self.consumer_groups.borrow_mut().iter_mut() {
            consumer_group.reassign_partitions(partitions_count);
        }
    }

    pub fn get_consumer_groups(&self) -> Vec<ConsumerGroup> {
        self.consumer_groups.borrow().values().cloned().collect()
    }

    pub fn get_consumer_group(
        &self,
        identifier: &Identifier,
    ) -> Result<Ref<'_, ConsumerGroup>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_consumer_group_by_id(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_consumer_group_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn try_get_consumer_group(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<Ref<'_, ConsumerGroup>>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => Ok(self.try_get_consumer_group_by_id(&identifier.get_u32_value()?)),
            IdKind::String => {
                Ok(self.try_get_consumer_group_by_name(&identifier.get_cow_str_value()?))
            }
        }
    }
    fn try_get_consumer_group_by_id(&self, id: &u32) -> Option<Ref<'_, ConsumerGroup>> {
        let consumer_groups = self.consumer_groups.borrow();
        let exists = consumer_groups.contains_key(id);
        if !exists {
            return None;
        }

        Some(Ref::map(consumer_groups, |cg| {
            let consumer_group = cg.get(id);
            consumer_group.unwrap()
        }))
    }

    fn try_get_consumer_group_by_name(&self, name: &str) -> Option<Ref<'_, ConsumerGroup>> {
        let consumer_groups = self.consumer_groups.borrow();
        let exists = self.consumer_groups_ids.contains_key(name);
        let id = self.consumer_groups_ids.get(name).unwrap();
        if !exists {
            return None;
        }

        Some(Ref::map(consumer_groups, |cg| {
            let consumer_group = cg.get(id);
            consumer_group.unwrap()
        }))
    }

    pub fn get_consumer_group_by_name(
        &self,
        name: &str,
    ) -> Result<Ref<'_, ConsumerGroup>, IggyError> {
        let group_id = self.consumer_groups_ids.get(name);
        if group_id.is_none() {
            return Err(IggyError::ConsumerGroupNameNotFound(
                name.to_string(),
                self.name.to_owned(),
            ));
        }

        self.get_consumer_group_by_id(*group_id.unwrap())
    }

    pub fn get_consumer_group_by_id(&self, id: u32) -> Result<Ref<'_, ConsumerGroup>, IggyError> {
        let consumer_groups = self.consumer_groups.borrow();
        let exists = consumer_groups.contains_key(&id);
        if !exists {
            return Err(IggyError::ConsumerGroupIdNotFound(id, self.topic_id));
        }
        let consumer_group = Ref::map(consumer_groups, |cg| {
            let consumer_group = cg.get(&id);
            consumer_group.unwrap()
        });

        Ok(consumer_group)
    }

    pub fn get_consumer_group_mut(
        &self,
        identifier: &Identifier,
    ) -> Result<RefMut<'_, ConsumerGroup>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => {
                self.get_consumer_group_by_id_mut(identifier.get_u32_value().unwrap())
            }
            IdKind::String => self.get_consumer_group_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_consumer_group_by_id_mut(
        &self,
        id: u32,
    ) -> Result<RefMut<'_, ConsumerGroup>, IggyError> {
        let consumer_groups = self.consumer_groups.borrow_mut();
        let exists = consumer_groups.contains_key(&id);
        if !exists {
            return Err(IggyError::ConsumerGroupIdNotFound(id, self.topic_id));
        }
        let consumer_group = RefMut::map(consumer_groups, |cg| {
            let consumer_group = cg.get_mut(&id);
            consumer_group.unwrap()
        });
        Ok(consumer_group)
    }

    pub fn get_consumer_group_by_name_mut(
        &self,
        name: &str,
    ) -> Result<RefMut<'_, ConsumerGroup>, IggyError> {
        let group_id = self.consumer_groups_ids.get(name).copied();
        if group_id.is_none() {
            return Err(IggyError::ConsumerGroupNameNotFound(
                name.to_string(),
                self.name.to_owned(),
            ));
        }

        self.get_consumer_group_by_id_mut(group_id.unwrap())
    }

    pub fn create_consumer_group(
        &mut self,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<Identifier, IggyError> {
        if self.consumer_groups_ids.contains_key(name) {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name.to_owned(),
                self.topic_id,
            ));
        }

        let mut id;
        if group_id.is_none() {
            id = self
                .current_consumer_group_id
                .fetch_add(1, Ordering::SeqCst);
            loop {
                if self.consumer_groups.borrow().contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::ConsumerGroupIdAlreadyExists(id, self.topic_id));
                    }
                    id = self
                        .current_consumer_group_id
                        .fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = group_id.unwrap();
        }

        if self.consumer_groups.borrow().contains_key(&id) {
            return Err(IggyError::ConsumerGroupIdAlreadyExists(id, self.topic_id));
        }

        let consumer_group =
            ConsumerGroup::new(self.topic_id, id, name, self.partitions.len() as u32);
        self.consumer_groups_ids.insert(name.to_owned(), id);
        self.consumer_groups.borrow_mut().insert(id, consumer_group);
        info!(
            "Created consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            id, self.topic_id, self.stream_id
        );
        Ok(Identifier::numeric(id)?)
    }

    pub async fn delete_consumer_group(
        &mut self,
        id: &Identifier,
    ) -> Result<ConsumerGroup, IggyError> {
        let group_id;
        {
            let consumer_group = self.get_consumer_group(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {id}")
            })?;
            group_id = consumer_group.group_id;
        }

        let mut consumer_groups = self.consumer_groups.borrow_mut();
        let consumer_group = consumer_groups.remove(&group_id);
        if consumer_group.is_none() {
            return Err(IggyError::ConsumerGroupIdNotFound(group_id, self.topic_id));
        }
        let consumer_group = consumer_group.unwrap();
        let consumer_group = consumer_group.clone();
        drop(consumer_groups);
        {
            self.consumer_groups_ids.remove(&consumer_group.name);
            let current_group_id = self.current_consumer_group_id.load(Ordering::SeqCst);
            if current_group_id > group_id {
                self.current_consumer_group_id
                    .store(group_id, Ordering::SeqCst);
            }

            for (_, partition) in self.partitions.iter() {
                let partition = partition.read().await;
                if let Some((_, offset)) = partition.consumer_group_offsets.remove(&group_id) {
                    self.storage
                        .partition
                        .delete_consumer_offset(&offset.path)
                        .await?;
                }
            }

            info!(
                "Deleted consumer group with ID: {} from topic with ID: {} and stream with ID: {}.",
                id, self.topic_id, self.stream_id
            );
        }

        Ok(consumer_group)
    }

    pub fn join_consumer_group(
        &mut self,
        group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), IggyError> {
        let topic_id = self.topic_id;
        let stream_id = self.stream_id;
        let mut consumer_group = self.get_consumer_group_mut(group_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {group_id}")
        })?;
        consumer_group.add_member(member_id);
        info!(
            "Member with ID: {} has joined consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, topic_id, stream_id
        );
        Ok(())
    }

    pub fn leave_consumer_group(
        &mut self,
        group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), IggyError> {
        let topic_id = self.topic_id;
        let stream_id = self.stream_id;
        let mut consumer_group = self.get_consumer_group_mut(group_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {group_id}")
        })?;
        consumer_group.delete_member(member_id);
        info!(
            "Member with ID: {} has left consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, topic_id, stream_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use crate::streaming::utils::MemoryPool;
    use iggy_common::{CompressionAlgorithm, IggyExpiry, MaxTopicSize};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let topic_id = topic.topic_id;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_ok());
        {
            let created_consumer_group_id = result.unwrap();
            assert_eq!(created_consumer_group_id.get_u32_value().unwrap(), group_id);
        }

        assert_eq!(topic.consumer_groups.borrow().len(), 1);
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap();
        assert_eq!(consumer_group.group_id, group_id);
        assert_eq!(consumer_group.name, name);
        assert_eq!(consumer_group.topic_id, topic_id);
        assert_eq!(
            consumer_group.partitions_count,
            topic.partitions.len() as u32
        );
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
        let result = topic.create_consumer_group(Some(group_id), "test2");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, IggyError::ConsumerGroupIdAlreadyExists(_, _)));
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_name() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
        let group_id = group_id + 1;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            IggyError::ConsumerGroupNameAlreadyExists(_, _)
        ));
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
    }

    #[tokio::test]
    async fn should_be_deleted_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
        let result = topic
            .delete_consumer_group(&Identifier::numeric(group_id).unwrap())
            .await;
        assert!(result.is_ok());
        assert!(topic.consumer_groups.borrow().is_empty());
    }

    #[tokio::test]
    async fn should_not_be_deleted_given_non_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
        let group_id = group_id + 1;
        let result = topic
            .delete_consumer_group(&Identifier::numeric(group_id).unwrap())
            .await;
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.borrow().len(), 1);
    }

    #[tokio::test]
    async fn should_be_joined_by_new_member() {
        let group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic().await;
        topic.create_consumer_group(Some(group_id), name).unwrap();
        let result = topic.join_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id);
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap();
        let members = consumer_group.get_members();
        assert_eq!(members.len(), 1);
    }

    #[tokio::test]
    async fn should_be_left_by_existing_member() {
        let group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic().await;
        topic.create_consumer_group(Some(group_id), name).unwrap();
        topic
            .join_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id)
            .unwrap();
        let result = topic.leave_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id);
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap();
        let members = consumer_group.get_members();
        assert!(members.is_empty())
    }

    async fn get_topic() -> Topic {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Rc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        MemoryPool::init_pool(config.clone());
        let stream_id = 1;
        let id = 2;
        let name = "test";
        let compression_algorithm = CompressionAlgorithm::None;
        let partitions_count = 3;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Arc::new(AtomicU32::new(0));

        Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            config,
            storage,
            size_of_parent_stream,
            messages_count_of_parent_stream,
            segments_count_of_parent_stream,
            IggyExpiry::NeverExpire,
            compression_algorithm,
            MaxTopicSize::ServerDefault,
            1,
        )
        .await
        .unwrap()
    }
}
