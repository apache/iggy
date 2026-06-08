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

use super::{LocalIdx, ShardId};

/// `local_idx = None` on non-owning rows so peer-shard inserts can't
/// clobber the owner's real index via unconditional papaya insert.
#[derive(Debug, Clone, Copy)]
pub struct PartitionLocation {
    pub shard_id: ShardId,
    pub local_idx: Option<LocalIdx>,
    /// `Partition::created_revision` from the committed metadata at the
    /// time this row was written. The reconciler compares it
    /// against the STM's current value to detect a stale local partition
    /// after a delete+recreate reused the same slab-key namespace tuple.
    pub epoch: u64,
}

impl PartitionLocation {
    pub fn owned(shard_id: ShardId, local_idx: LocalIdx, epoch: u64) -> Self {
        Self {
            shard_id,
            local_idx: Some(local_idx),
            epoch,
        }
    }

    pub fn routed(shard_id: ShardId, epoch: u64) -> Self {
        Self {
            shard_id,
            local_idx: None,
            epoch,
        }
    }
}
