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

use crate::Partition;
use crate::ResourceOptions;
use crate::TopicResolvedOptions;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};

/// `Topic` represents the medium level of logical separation of data as it's a part of the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size`: the total size of the topic in bytes.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: u32,
    /// Settings: client-explicit keys plus admission-derived defaults,
    /// distinguished by each entry's `explicit` flag. Message expiry,
    /// compression and the size cap live here, read them typed through
    /// `resolved_options`.
    #[serde(default, with = "crate::resource_options_json")]
    pub options: ResourceOptions,
}

/// `TopicDetails` represents the detailed information about the topic.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size`: the total size of the topic.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
/// - `partitions`: the collection of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicDetails {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic.
    pub size: IggyByteSize,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: u32,
    /// The collection of partitions in the topic.
    pub partitions: Vec<Partition>,
    /// Settings: client-explicit keys plus admission-derived defaults,
    /// distinguished by each entry's `explicit` flag. Message expiry,
    /// compression and the size cap live here, read them typed through
    /// `resolved_options`.
    #[serde(default, with = "crate::resource_options_json")]
    pub options: ResourceOptions,
}

impl Topic {
    /// Every setting of the topic, typed and resolved against the catalog
    /// defaults.
    #[must_use]
    pub fn resolved_options(&self) -> TopicResolvedOptions {
        TopicResolvedOptions::from(&self.options)
    }
}

impl TopicDetails {
    /// Every setting of the topic, typed and resolved against the catalog
    /// defaults.
    #[must_use]
    pub fn resolved_options(&self) -> TopicResolvedOptions {
        TopicResolvedOptions::from(&self.options)
    }
}
