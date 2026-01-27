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

use crate::metadata::PartitionId;
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::stats::PartitionStats;
use iggy_common::IggyTimestamp;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PartitionMeta {
    pub id: PartitionId,
    pub created_at: IggyTimestamp,
    /// Monotonically increasing version to detect stale local_partitions entries.
    /// Set to the Metadata version when the partition was created.
    pub revision_id: u64,
    pub stats: Arc<PartitionStats>,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
}
