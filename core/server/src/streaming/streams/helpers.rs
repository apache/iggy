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

use crate::{
    configs::system::SystemConfig,
    slab::{
        streams,
        traits_ext::{ComponentsById, EntityComponentSystem, IntoComponents},
    },
    streaming::{
        partitions,
        streams::stream::{StreamRef, StreamRefMut},
    },
};
use iggy_common::Identifier;

pub fn get_stream_id() -> impl FnOnce(ComponentsById<StreamRef>) -> streams::ContainerId {
    |(root, _)| root.id()
}

pub fn get_stream_name() -> impl FnOnce(ComponentsById<StreamRef>) -> String {
    |(root, _)| root.name().clone()
}

pub fn update_stream_name(name: String) -> impl FnOnce(ComponentsById<StreamRefMut>) {
    move |(mut root, _)| {
        root.set_name(name);
    }
}

pub fn get_topic_ids() -> impl FnOnce(ComponentsById<StreamRef>) -> Vec<usize> {
    |(root, _)| {
        root.topics().with_components(|components| {
            let (topic_roots, ..) = components.into_components();
            topic_roots
                .iter()
                .map(|(_, topic)| topic.id())
                .collect::<Vec<_>>()
        })
    }
}

pub fn store_consumer_offset(
    consumer_id: usize,
    topic_id: &Identifier,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<StreamRef>) {
    move |(root, ..)| {
        let stream_id = root.id();
        root.topics().with_topic_by_id(topic_id, |(root, ..)| {
            let topic_id = root.id();
            root.partitions().with_components_by_id(
                partition_id,
                partitions::helpers::store_consumer_offset(
                    consumer_id,
                    stream_id,
                    topic_id,
                    partition_id,
                    offset,
                    config,
                ),
            )
        })
    }
}

pub fn store_consumer_group_offset(
    consumer_group_id: crate::streaming::polling_consumer::ConsumerGroupId,
    topic_id: &Identifier,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<StreamRef>) {
    move |(root, ..)| {
        let stream_id = root.id();
        root.topics().with_topic_by_id(topic_id, |(root, ..)| {
            let topic_id = root.id();
            root.partitions().with_components_by_id(
                partition_id,
                partitions::helpers::store_consumer_group_offset(
                    consumer_group_id,
                    stream_id,
                    topic_id,
                    partition_id,
                    offset,
                    config,
                ),
            )
        })
    }
}
