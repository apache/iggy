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

use crate::streaming::streams::COMPONENT;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::CreatedTopicInfo;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use iggy_common::CompressionAlgorithm;
use iggy_common::IggyError;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{IdKind, Identifier};
use std::sync::atomic::Ordering;
use tracing::info;

impl Stream {
    pub fn get_topics_count(&self) -> u32 {
        self.topics.len() as u32
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &mut self,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(Identifier, Vec<u32>), IggyError> {
        let max_topic_size = Topic::get_max_topic_size(max_topic_size, &self.config)?;
        if self.topics_ids.contains_key(name) {
            return Err(IggyError::TopicNameAlreadyExists(
                name.to_owned(),
                self.stream_id,
            ));
        }

        let mut id;
        if topic_id.is_none() {
            id = self.current_topic_id.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.topics.contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::TopicIdAlreadyExists(id, self.stream_id));
                    }
                    id = self.current_topic_id.fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = topic_id.unwrap();
        }

        if self.topics.contains_key(&id) {
            return Err(IggyError::TopicIdAlreadyExists(id, self.stream_id));
        }

        let CreatedTopicInfo {
            topic,
            partition_ids,
        } = Topic::create(
            self.stream_id,
            id,
            name,
            partitions_count,
            self.config.clone(),
            self.storage.clone(),
            self.size_bytes.clone(),
            self.messages_count.clone(),
            self.segments_count.clone(),
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )?;

        self.topics_ids.insert(name.to_owned(), id);
        self.topics.insert(id, topic);
        Ok((Identifier::numeric(id).unwrap(), partition_ids))
    }

    pub fn update_topic(
        &mut self,
        id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        let message_expiry = Topic::get_message_expiry(message_expiry, &self.config);
        let max_topic_size = Topic::get_max_topic_size(max_topic_size, &self.config)?;
        let topic_id;
        {
            let topic = self.get_topic(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get topic with id: {id}")
            })?;
            topic_id = topic.topic_id;
        }

        {
            if let Some(topic_id_by_name) = self.topics_ids.get(name) {
                if *topic_id_by_name != topic_id {
                    return Err(IggyError::TopicNameAlreadyExists(
                        name.to_owned(),
                        self.stream_id,
                    ));
                }
            }
        }

        let old_topic_name = {
            let topic = self.get_topic(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get topic with id: {id}")
            })?;
            topic.name.clone()
        };

        {
            let topic = self.get_topic_mut(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get mutable reference to topic with id {id}")
            })?;

            topic.name = name.to_owned();
            topic.message_expiry = message_expiry;
            topic.compression_algorithm = compression_algorithm;
            topic.max_topic_size = max_topic_size;
            topic.replication_factor = replication_factor;
        }

            self.topics_ids.remove(&old_topic_name.clone());
            self.topics_ids.insert(name.to_owned(), topic_id);

        Ok(())
    }

    pub fn remove_topic(&mut self, identifier: &Identifier) -> Result<Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.remove_topic_by_id(identifier.get_u32_value()?),
            IdKind::String => self.remove_topic_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn try_get_topic(&self, identifier: &Identifier) -> Result<Option<&Topic>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => Ok(self.topics.get(&identifier.get_u32_value()?)),
            IdKind::String => Ok(self.try_get_topic_by_name(&identifier.get_cow_str_value()?)),
        }
    }

    fn try_get_topic_by_name(&self, name: &str) -> Option<&Topic> {
        self.topics_ids.get(name).and_then(|id| self.topics.get(id))
    }

    pub fn get_topic(&self, identifier: &Identifier) -> Result<&Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_topic_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_topic_mut(&mut self, identifier: &Identifier) -> Result<&mut Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_topic_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_topic_by_id(&self, id: u32) -> Result<&Topic, IggyError> {
        self.topics
            .get(&id)
            .ok_or(IggyError::TopicIdNotFound(id, self.stream_id))
    }

    fn get_topic_by_name(&self, name: &str) -> Result<&Topic, IggyError> {
        self.topics_ids
            .get(name)
            .map(|topic_id| self.get_topic_by_id(*topic_id))
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_string(), self.name.to_owned()))?
    }

    fn get_topic_by_id_mut(&mut self, id: u32) -> Result<&mut Topic, IggyError> {
        self.topics
            .get_mut(&id)
            .ok_or(IggyError::TopicIdNotFound(id, self.stream_id))
    }

    fn get_topic_by_name_mut(&mut self, name: &str) -> Result<&mut Topic, IggyError> {
        self.topics_ids
            .get(name)
            .and_then(|topic_id| self.topics.get_mut(topic_id))
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_string(), self.name.to_owned()))
    }

    fn remove_topic_by_id(&mut self, id: u32) -> Result<Topic, IggyError> {
        let topic = self
            .topics
            .remove(&id)
            .ok_or(IggyError::TopicIdNotFound(id, self.stream_id))?;

        self.topics_ids
            .remove(&topic.name)
            .ok_or_else(|| IggyError::TopicNameNotFound(topic.name.clone(), self.name.clone()))?;
        Ok(topic)
    }

    fn remove_topic_by_name(&mut self, name: &str) -> Result<Topic, IggyError> {
        let topic_id = self
            .topics_ids
            .remove(name)
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_owned(), self.name.clone()))?;

        self.topics
            .remove(&topic_id)
            .ok_or(IggyError::TopicIdNotFound(topic_id, self.stream_id))
    }

    pub fn delete_topic(&mut self, id: &Identifier) -> Result<Topic, IggyError> {
        let topic = self.remove_topic(id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to remove topic with id: {id}")
        })?;
        let topic_id = topic.topic_id;
        let current_topic_id = self.current_topic_id.load(Ordering::SeqCst);
        if current_topic_id > topic_id {
            self.current_topic_id.store(topic_id, Ordering::SeqCst);
        }

        Ok(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        configs::system::SystemConfig,
        streaming::{
            persistence::persister::{FileWithSyncPersister, PersisterKind},
            storage::SystemStorage,
            utils::MemoryPool,
        },
    };
    use iggy_common::IggyByteSize;
    use std::{rc::Rc, sync::Arc};

    #[tokio::test]
    async fn should_get_topic_by_id_and_name() {
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
        let stream_name = "test_stream";
        let topic_id = 2;
        let topic_name = "test_topic";
        let message_expiry = IggyExpiry::NeverExpire;
        let compression_algorithm = CompressionAlgorithm::None;
        let max_topic_size = 2 * config.segment.size.as_bytes_u64();
        let max_topic_size = MaxTopicSize::Custom(IggyByteSize::from(max_topic_size));
        let mut stream = Stream::create(stream_id, stream_name, config, storage);
        stream
            .create_topic(
                Some(topic_id),
                topic_name,
                1,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                1,
            )
            .unwrap();

        let topic = stream.get_topic(&Identifier::numeric(topic_id).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.compression_algorithm, compression_algorithm);

        let topic = stream.get_topic(&Identifier::named(topic_name).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.compression_algorithm, compression_algorithm);
    }
}
