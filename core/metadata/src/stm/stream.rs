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

use crate::define_state_command;
use crate::stats::{StreamStats, TopicStats};
use crate::stm::ApplyState;
use ahash::AHashMap;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::delete_segments::DeleteSegments;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
};
use slab::Slab;
use std::cell::RefCell;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: usize,
}

impl Partition {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Partitions {
    items: RefCell<Slab<Partition>>,
}

impl Partitions {
    pub fn new() -> Self {
        Self {
            items: RefCell::new(Slab::with_capacity(1024)),
        }
    }

    pub fn insert(&self, partition: Partition) -> usize {
        let mut items = self.items.borrow_mut();
        let id = items.insert(partition);
        items[id].id = id;
        id
    }

    pub fn get(&self, id: usize) -> Option<Partition> {
        self.items.borrow().get(id).cloned()
    }

    pub fn remove(&self, id: usize) -> Option<Partition> {
        let mut items = self.items.borrow_mut();
        if items.contains(id) {
            Some(items.remove(id))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    pub fn iter(&self) -> Vec<Partition> {
        self.items
            .borrow()
            .iter()
            .map(|(_, p): (usize, &Partition)| p.clone())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
}

impl ConsumerGroup {
    pub fn new(name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroups {
    index: RefCell<AHashMap<String, usize>>,
    items: RefCell<Slab<ConsumerGroup>>,
}

impl ConsumerGroups {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(256)),
            items: RefCell::new(Slab::with_capacity(256)),
        }
    }

    pub fn insert(&self, group: ConsumerGroup) -> usize {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        let name = group.name.clone();
        let id = items.insert(group);
        items[id].id = id;
        index.insert(name, id);
        id
    }

    pub fn get(&self, id: usize) -> Option<ConsumerGroup> {
        self.items.borrow().get(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<ConsumerGroup> {
        let index = self.index.borrow();
        if let Some(&id) = index.get(name) {
            self.items.borrow().get(id).cloned()
        } else {
            None
        }
    }

    pub fn remove(&self, id: usize) -> Option<ConsumerGroup> {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !items.contains(id) {
            return None;
        }

        let group = items.remove(id);
        index.remove(&group.name);
        Some(group)
    }

    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub replication_factor: u8,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,

    pub stats: Arc<TopicStats>,
    pub partitions: Partitions,
    pub consumer_groups: ConsumerGroups,
}

impl Topic {
    pub fn new(
        name: String,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        stream_stats: Arc<StreamStats>,
    ) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            replication_factor,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            stats: Arc::new(TopicStats::new(stream_stats)),
            partitions: Partitions::new(),
            consumer_groups: ConsumerGroups::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Topics {
    index: RefCell<AHashMap<String, usize>>,
    items: RefCell<Slab<Topic>>,
}

impl Topics {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(1024)),
            items: RefCell::new(Slab::with_capacity(1024)),
        }
    }

    pub fn insert(&self, topic: Topic) -> usize {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        let name = topic.name.clone();
        let id = items.insert(topic);
        items[id].id = id;
        index.insert(name, id);
        id
    }

    pub fn get(&self, id: usize) -> Option<Topic> {
        self.items.borrow().get(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Topic> {
        let index = self.index.borrow();
        if let Some(&id) = index.get(name) {
            self.items.borrow().get(id).cloned()
        } else {
            None
        }
    }

    pub fn get_by_identifier(&self, identifier: &Identifier) -> Option<Topic> {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                if let Ok(id) = identifier.get_u32_value() {
                    self.get(id as usize)
                } else {
                    None
                }
            }
            iggy_common::IdKind::String => {
                if let Ok(name) = identifier.get_string_value() {
                    self.get_by_name(&name)
                } else {
                    None
                }
            }
        }
    }

    pub fn remove(&self, id: usize) -> Option<Topic> {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !items.contains(id) {
            return None;
        }

        let topic = items.remove(id);
        index.remove(&topic.name);
        Some(topic)
    }

    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,

    pub stats: Arc<StreamStats>,
    pub topics: Topics,
}

impl Stream {
    pub fn new(name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            stats: Arc::new(StreamStats::default()),
            topics: Topics::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Streams {
    index: RefCell<AHashMap<String, usize>>,
    items: RefCell<Slab<Stream>>,
}

impl Streams {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(256)),
            items: RefCell::new(Slab::with_capacity(256)),
        }
    }

    pub fn insert(&self, stream: Stream) -> usize {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        let name = stream.name.clone();
        let id = items.insert(stream);
        items[id].id = id;
        index.insert(name, id);
        id
    }

    pub fn get(&self, id: usize) -> Option<Stream> {
        self.items.borrow().get(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Stream> {
        let index = self.index.borrow();
        if let Some(&id) = index.get(name) {
            self.items.borrow().get(id).cloned()
        } else {
            None
        }
    }

    pub fn get_by_identifier(&self, identifier: &Identifier) -> Option<Stream> {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                if let Ok(id) = identifier.get_u32_value() {
                    self.get(id as usize)
                } else {
                    None
                }
            }
            iggy_common::IdKind::String => {
                if let Ok(name) = identifier.get_string_value() {
                    self.get_by_name(&name)
                } else {
                    None
                }
            }
        }
    }

    pub fn remove(&self, id: usize) -> Option<Stream> {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !items.contains(id) {
            return None;
        }

        let stream = items.remove(id);
        index.remove(&stream.name);
        Some(stream)
    }

    pub fn update_name(&self, identifier: &Identifier, new_name: String) -> Result<(), IggyError> {
        let stream = self.get_by_identifier(identifier);
        if let Some(stream) = stream {
            let mut items = self.items.borrow_mut();
            let mut index = self.index.borrow_mut();

            index.remove(&stream.name);
            if let Some(s) = items.get_mut(stream.id) {
                s.name = new_name.clone();
            }
            index.insert(new_name, stream.id);
            Ok(())
        } else {
            Err(IggyError::ResourceNotFound("Stream".to_string()))
        }
    }

    pub fn purge(&self, id: usize) -> Result<(), IggyError> {
        let items = self.items.borrow();
        if let Some(_stream) = items.get(id) {
            // TODO: Purge all topics in the stream
            Ok(())
        } else {
            Err(IggyError::ResourceNotFound("Stream".to_string()))
        }
    }

    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    pub fn iter(&self) -> Vec<Stream> {
        self.items
            .borrow()
            .iter()
            .map(|(_, s): (usize, &Stream)| s.clone())
            .collect()
    }
}

// Define StreamsCommand enum and StateCommand implementation using the macro
define_state_command! {
    Streams,
    StreamsCommand,
    [CreateStream, UpdateStream, DeleteStream, PurgeStream]
}

impl ApplyState for Streams {
    type Output = ();

    fn do_apply(&self, cmd: Self::Command) -> Self::Output {
        match cmd {
            StreamsCommand::CreateStream(payload) => {
                todo!("Handle Create stream with {:?}", payload)
            }
            StreamsCommand::UpdateStream(payload) => {
                todo!("Handle Update stream with {:?}", payload)
            }
            StreamsCommand::DeleteStream(payload) => {
                todo!("Handle Delete stream with {:?}", payload)
            }
            StreamsCommand::PurgeStream(payload) => todo!("Handle Purge stream with {:?}", payload),
        }
    }
}

// Define TopicsCommand enum and StateCommand implementation using the macro
define_state_command! {
    Topics,
    TopicsCommand,
    [CreateTopic, UpdateTopic, DeleteTopic, PurgeTopic]
}

impl ApplyState for Topics {
    type Output = ();

    fn do_apply(&self, cmd: Self::Command) -> Self::Output {
        match cmd {
            TopicsCommand::CreateTopic(payload) => todo!("Handle Create topic with {:?}", payload),
            TopicsCommand::UpdateTopic(payload) => todo!("Handle Update topic with {:?}", payload),
            TopicsCommand::DeleteTopic(payload) => todo!("Handle Delete topic with {:?}", payload),
            TopicsCommand::PurgeTopic(payload) => todo!("Handle Purge topic with {:?}", payload),
        }
    }
}

// Define PartitionsCommand enum and StateCommand implementation using the macro
define_state_command! {
    Partitions,
    PartitionsCommand,
    [CreatePartitions, DeletePartitions, DeleteSegments]
}

impl ApplyState for Partitions {
    type Output = ();

    fn do_apply(&self, cmd: Self::Command) -> Self::Output {
        match cmd {
            PartitionsCommand::CreatePartitions(payload) => {
                todo!("Handle Create partitions with {:?}", payload)
            }
            PartitionsCommand::DeletePartitions(payload) => {
                todo!("Handle Delete partitions with {:?}", payload)
            }
            PartitionsCommand::DeleteSegments(payload) => {
                todo!("Handle Delete segments with {:?}", payload)
            }
        }
    }
}
