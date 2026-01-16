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
//! This module provides a `LeftRight`-based approach where all shards read from
//! a shared snapshot, and only shard 0 can write.
//!
//! # Architecture
//!
//! - `InnerMetadata` (inner.rs): Immutable snapshot with all metadata
//! - `Metadata` (reader.rs): Thread-safe read handle for querying metadata
//! - Entity types: `StreamMeta`, `TopicMeta`, `PartitionMeta`, `UserMeta`, `ConsumerGroupMeta`
//! - Consumer offsets are stored in `PartitionMeta` for cross-shard visibility

mod absorb;
mod consumer_group;
mod consumer_group_member;
mod inner;
pub mod ops;
mod partition;
mod reader;
mod stream;
mod topic;
mod user;
mod writer;

pub use consumer_group::ConsumerGroupMeta;
pub use consumer_group_member::ConsumerGroupMemberMeta;
pub use inner::InnerMetadata;
pub use ops::MetadataOp;
pub use partition::PartitionMeta;
pub use reader::{Metadata, PartitionInitInfo};
pub use stream::StreamMeta;
pub use topic::TopicMeta;
pub use user::UserMeta;
pub use writer::MetadataWriter;

pub type MetadataReadHandle = left_right::ReadHandle<InnerMetadata>;
pub type StreamId = usize;
pub type TopicId = usize;
pub type PartitionId = usize;
pub type UserId = u32;
pub type ClientId = u32;
pub type ConsumerGroupId = usize;
pub type ConsumerGroupMemberId = usize;
pub type ConsumerGroupKey = (StreamId, TopicId, ConsumerGroupId);

pub fn create_metadata_handles() -> (MetadataWriter, MetadataReadHandle) {
    let (write_handle, read_handle) = left_right::new::<InnerMetadata, MetadataOp>();
    let mut writer = MetadataWriter::new(write_handle);
    writer.publish();
    (writer, read_handle)
}
