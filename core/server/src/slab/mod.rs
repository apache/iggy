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

pub mod consumer_groups;
pub mod helpers;
pub mod partitions;
pub mod streams;
pub mod topics;
pub mod traits_ext;
pub mod users;

use std::fmt::{Debug, Display};

// General rules how to implement `with_*` methods on any slab"
// 1. When implementing method that accepts closure f, make sure that the caller can supply closure only with 1 depth of callbacks.
// for example, observe following code snippet:
// ```rust
// let topic_id = self.streams.with_topic_by_id(stream_id, topic_id, get_topic_id());
// ```
// if we would not provide a `with_topic_by_id` method and purely relied only ony `with_topics`, we would have to write:
// ```rust
// let topic_id = self.streams.with_topics(stream_id, get_topic_by_id(topic_id, get_topic_id())); // `get_topic_id` is a closure that retrieves the topic id.
// ```
// we need to supply a nested closure to `get_topic_by_id`.

pub trait Keyed {
    type Key: Eq + std::hash::Hash + Clone + Debug + Display;
    fn key(&self) -> &Self::Key;
}
