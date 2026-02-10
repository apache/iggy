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

use crate::stm::StateHandler;
use crate::{collect_handlers, define_state};
use ahash::AHashMap;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::{IdKind, Identifier};
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u32,
    pub partitions: Vec<usize>,
    pub partition_index: Arc<AtomicUsize>,
}

impl ConsumerGroupMember {
    pub fn new(id: usize, client_id: u32) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
            partition_index: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: usize,
    pub name: Arc<str>,
    pub partitions: Vec<usize>,
    pub members: Slab<ConsumerGroupMember>,
}

impl ConsumerGroup {
    pub fn new(name: Arc<str>) -> Self {
        Self {
            id: 0,
            name,
            partitions: Vec::new(),
            members: Slab::new(),
        }
    }

    pub fn rebalance_members(&mut self) {
        let partition_count = self.partitions.len();
        let member_count = self.members.len();

        if member_count == 0 || partition_count == 0 {
            return;
        }

        let member_ids: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_ids {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
            }
        }

        for (i, &partition_id) in self.partitions.iter().enumerate() {
            let member_idx = i % member_count;
            if let Some(&member_id) = member_ids.get(member_idx)
                && let Some(member) = self.members.get_mut(member_id)
            {
                member.partitions.push(partition_id);
            }
        }
    }
}

define_state! {
    ConsumerGroups {
        name_index: AHashMap<Arc<str>, usize>,
        topic_index: AHashMap<(usize, usize), Vec<usize>>,
        topic_name_index: AHashMap<(Arc<str>, Arc<str>), Vec<usize>>,
        items: Slab<ConsumerGroup>,
    }
}

collect_handlers! {
    ConsumerGroups {
        CreateConsumerGroup,
        DeleteConsumerGroup,
    }
}

impl ConsumerGroupsInner {
    fn resolve_consumer_group_id_by_identifiers(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Option<usize> {
        if let (Ok(s), Ok(t)) = (stream_id.get_u32_value(), topic_id.get_u32_value()) {
            let groups_in_topic = self.topic_index.get(&(s as usize, t as usize))?;

            return match group_id.kind {
                IdKind::Numeric => {
                    let g_id = group_id.get_u32_value().ok()? as usize;
                    groups_in_topic.contains(&g_id).then_some(g_id)
                }
                IdKind::String => {
                    let g_name = group_id.get_string_value().ok()?;
                    groups_in_topic
                        .iter()
                        .find(|&&id| {
                            self.items
                                .get(id)
                                .is_some_and(|g| g.name.as_ref() == g_name)
                        })
                        .copied()
                }
            };
        }

        if let (Ok(s), Ok(t)) = (stream_id.get_string_value(), topic_id.get_string_value()) {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            let groups_in_topic = self.topic_name_index.get(&key)?;

            return match group_id.kind {
                IdKind::Numeric => {
                    let g_id = group_id.get_u32_value().ok()? as usize;
                    groups_in_topic.contains(&g_id).then_some(g_id)
                }
                IdKind::String => {
                    let g_name = group_id.get_string_value().ok()?;
                    groups_in_topic
                        .iter()
                        .find(|&&id| {
                            self.items
                                .get(id)
                                .is_some_and(|g| g.name.as_ref() == g_name)
                        })
                        .copied()
                }
            };
        }

        None
    }
}

// TODO: This is all a hack, we need to figure out how to do this in a way where `Identifier`
// does not reach this stage of execution.

impl StateHandler for CreateConsumerGroup {
    type State = ConsumerGroupsInner;
    fn apply(&self, state: &mut ConsumerGroupsInner) {
        let name: Arc<str> = Arc::from(self.name.as_str());
        if state.name_index.contains_key(&name) {
            return;
        }

        let group = ConsumerGroup::new(name.clone());
        let id = state.items.insert(group);
        state.items[id].id = id;

        state.name_index.insert(name.clone(), id);

        if let (Ok(s), Ok(t)) = (
            self.stream_id.get_u32_value(),
            self.topic_id.get_u32_value(),
        ) {
            state
                .topic_index
                .entry((s as usize, t as usize))
                .or_default()
                .push(id);
        }

        if let (Ok(s), Ok(t)) = (
            self.stream_id.get_string_value(),
            self.topic_id.get_string_value(),
        ) {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            state.topic_name_index.entry(key).or_default().push(id);
        }
    }
}

impl StateHandler for DeleteConsumerGroup {
    type State = ConsumerGroupsInner;
    fn apply(&self, state: &mut ConsumerGroupsInner) {
        let Some(id) = state.resolve_consumer_group_id_by_identifiers(
            &self.stream_id,
            &self.topic_id,
            &self.group_id,
        ) else {
            return;
        };

        let group = state.items.remove(id);
        state.name_index.remove(&group.name);

        if let (Ok(s), Ok(t)) = (
            self.stream_id.get_u32_value(),
            self.topic_id.get_u32_value(),
        ) && let Some(vec) = state.topic_index.get_mut(&(s as usize, t as usize))
        {
            vec.retain(|&x| x != id);
        }

        if let (Ok(s), Ok(t)) = (
            self.stream_id.get_string_value(),
            self.topic_id.get_string_value(),
        ) {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            if let Some(vec) = state.topic_name_index.get_mut(&key) {
                vec.retain(|&x| x != id);
            }
        }
    }
}
