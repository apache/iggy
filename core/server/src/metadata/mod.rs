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
//! This module provides an `ArcSwap`-based approach where all shards read from
//! a shared snapshot, and only shard 0 can write (swap in new snapshots).
//!
//! # Architecture
//!
//! - `GlobalMetadata` (snapshot.rs): Immutable snapshot with all metadata
//! - `SharedMetadata` (shared.rs): Thread-safe wrapper with ArcSwap
//! - Entity types: `StreamMeta`, `TopicMeta`, `PartitionMeta`, `UserMeta`, `ConsumerGroupMeta`
//! - Consumer offsets are stored in `PartitionMeta` for cross-shard visibility

mod consumer_group;
mod consumer_group_member;
mod partition;
mod shared;
mod snapshot;
mod stream;
mod topic;
mod user;

pub use consumer_group::ConsumerGroupMeta;
pub use consumer_group_member::ConsumerGroupMemberMeta;
pub use partition::PartitionMeta;
pub use shared::Metadata;
pub use snapshot::InnerMetadata;
pub use stream::StreamMeta;
pub use topic::TopicMeta;
pub use user::UserMeta;

pub type StreamId = usize;
pub type TopicId = usize;
pub type PartitionId = usize;
pub type UserId = u32;
pub type ClientId = u32;
pub type ConsumerGroupId = usize;
pub type ConsumerGroupMemberId = usize;
pub type ConsumerGroupKey = (StreamId, TopicId, ConsumerGroupId);

/// Segment size for SegmentedSlab (1024 entries per segment).
pub const SLAB_SEGMENT_SIZE: usize = 1024;
