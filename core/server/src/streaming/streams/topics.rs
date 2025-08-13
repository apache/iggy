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
use iggy_common::{IdKind, Identifier};
use std::sync::atomic::Ordering;

impl Stream {
    pub fn get_topics_count(&self) -> u32 {
        self.topics.len() as u32
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
        let result = self
            .topics_ids
            .get(name)
            .map(|topic_id| self.get_topic_by_id(*topic_id))
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_string(), self.name.to_owned()))?;
        result
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
}
