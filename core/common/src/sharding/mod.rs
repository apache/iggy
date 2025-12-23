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

mod local_idx;
mod namespace;
mod partition_location;
mod shard_id;

pub use local_idx::LocalIdx;
pub use namespace::{
    IggyNamespace, MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, PARTITION_BITS, PARTITION_MASK,
    PARTITION_SHIFT, STREAM_BITS, STREAM_MASK, STREAM_SHIFT, TOPIC_BITS, TOPIC_MASK, TOPIC_SHIFT,
};
pub use partition_location::PartitionLocation;
pub use shard_id::ShardId;
