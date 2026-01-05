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

//! Shared metadata module providing a single source of truth for all shards.
//!
//! This module replaces the broadcast-based metadata synchronization with an
//! `ArcSwap`-based approach where all shards read from a shared snapshot,
//! and only shard 0 can write (swap in new snapshots).

mod consumer_group;
mod consumer_offsets_store;
mod partition;
mod shared;
mod snapshot;
mod stats_store;
mod stream;
mod topic;
mod user;

pub use consumer_group::{ConsumerGroupMember, ConsumerGroupMeta};
pub use consumer_offsets_store::SharedConsumerOffsetsStore;
pub use partition::PartitionMeta;
pub use shared::SharedMetadata;
pub use snapshot::MetadataSnapshot;
pub use stats_store::SharedStatsStore;
pub use stream::StreamMeta;
pub use topic::TopicMeta;
pub use user::{PersonalAccessTokenMeta, UserMeta};
