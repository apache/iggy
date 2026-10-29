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

use crate::PartitionContext;
use serde::{Deserialize, Serialize};

/// `ConsumerOffsetInfo` represents the information about a consumer offset.
/// It consists of the following fields:
/// - `partition_id`: the unique identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `stored_offset`: the stored offset by the consumer in the partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerOffsetInfo {
    /// The unique identifier of the partition.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The stored offset by the consumer in the partition.
    pub stored_offset: u64,
}

/// The position after one delivered message. A commit stores its offset, so it covers all
/// earlier messages of the partition. A checkpoint retains the partition incarnation and owner
/// that delivered the message, so a commit after a change of the partition history fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    partition_id: u32,
    offset: u64,
    context: PartitionContext,
    #[serde(skip)]
    stamp: Option<ReaderStamp>,
}

/// The reader and the partition generation that delivered a message. The SDK reader refuses a
/// commit of a checkpoint that another reader delivered, and skips one of an older generation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderStamp {
    pub reader: u64,
    pub generation: u64,
}

impl Checkpoint {
    /// The checkpoint after the message at `offset`, delivered under `context`.
    #[doc(hidden)]
    pub const fn new(partition_id: u32, offset: u64, context: PartitionContext) -> Self {
        Self::stamped(partition_id, offset, context, None)
    }

    /// The checkpoint after the message at `offset`, delivered by the reader of `stamp`.
    #[doc(hidden)]
    pub const fn stamped(
        partition_id: u32,
        offset: u64,
        context: PartitionContext,
        stamp: Option<ReaderStamp>,
    ) -> Self {
        Self {
            partition_id,
            offset,
            context,
            stamp,
        }
    }

    /// The partition of the delivered message.
    pub const fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// The offset of the delivered message, which a commit stores.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Partition incarnation and owner captured by the read; a delayed commit keeps both.
    #[doc(hidden)]
    pub const fn context(&self) -> PartitionContext {
        self.context
    }

    #[doc(hidden)]
    pub const fn stamp(&self) -> Option<ReaderStamp> {
        self.stamp
    }
}
