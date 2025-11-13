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
    slab::{
        consumer_groups::ConsumerGroups, partitions::Partitions, topics::Topics,
        traits_ext::ComponentsById,
    },
    streaming::{
        streams::stream::StreamRef,
        topics::topic::{TopicRef, TopicRefMut},
    },
};

// Helpers
pub fn topics<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> FnOnce(&'a Topics) -> O,
{
    |(root, ..)| f(root.topics())
}

pub fn topics_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> FnOnce(&'a Topics) -> O,
{
    |(root, ..)| f(root.topics())
}

pub fn partitions<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> FnOnce(&'a Partitions) -> O,
{
    |(root, ..)| f(root.partitions())
}

pub fn partitions_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut Partitions) -> O,
{
    |(mut root, ..)| f(root.partitions_mut())
}

pub fn consumer_groups<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> FnOnce(&'a ConsumerGroups) -> O,
{
    |(root, ..)| f(root.consumer_groups())
}

pub fn consumer_groups_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut ConsumerGroups) -> O,
{
    |(mut root, ..)| f(root.consumer_groups_mut())
}
