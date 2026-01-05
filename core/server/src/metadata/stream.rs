// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::metadata::TopicMeta;
use iggy_common::IggyTimestamp;
use imbl::HashMap as ImHashMap;

/// Stream metadata stored in the shared snapshot.
#[derive(Debug, Clone)]
pub struct StreamMeta {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,

    /// Topic metadata indexed by topic ID
    pub topics: ImHashMap<usize, TopicMeta>,

    /// Topic name to ID index
    pub topic_index: ImHashMap<String, usize>,
}

impl StreamMeta {
    pub fn new(id: usize, name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id,
            name,
            created_at,
            topics: ImHashMap::new(),
            topic_index: ImHashMap::new(),
        }
    }

    pub fn topics_count(&self) -> usize {
        self.topics.len()
    }

    pub fn add_topic(&mut self, topic: TopicMeta) {
        self.topic_index.insert(topic.name.clone(), topic.id);
        self.topics.insert(topic.id, topic);
    }

    pub fn remove_topic(&mut self, topic_id: usize) -> Option<TopicMeta> {
        if let Some(topic) = self.topics.remove(&topic_id) {
            self.topic_index.remove(&topic.name);
            Some(topic)
        } else {
            None
        }
    }

    pub fn get_topic(&self, topic_id: usize) -> Option<&TopicMeta> {
        self.topics.get(&topic_id)
    }

    pub fn get_topic_mut(&mut self, topic_id: usize) -> Option<&mut TopicMeta> {
        self.topics.get_mut(&topic_id)
    }

    pub fn get_topic_id_by_name(&self, name: &str) -> Option<usize> {
        self.topic_index.get(name).copied()
    }

    pub fn topic_exists(&self, name: &str) -> bool {
        self.topic_index.contains_key(name)
    }
}
