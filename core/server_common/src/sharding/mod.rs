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
    IggyNamespace, MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_GROUP, PACKED_NAMESPACE_BITS,
    PACKED_NAMESPACE_MAX, PARTITION_BITS, PARTITION_MASK, PARTITION_SHIFT, STREAM_BITS,
    STREAM_MASK, STREAM_SHIFT, TOPIC_BITS, TOPIC_MASK, TOPIC_SHIFT,
};
pub use partition_location::PartitionLocation;
pub use shard_id::ShardId;

/// Maximum time a client-list gather waits for all shard replies.
pub const LIST_CLIENTS_GATHER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Maximum time spent retrying one gathered consumer-session report.
pub const CONSUMER_SESSION_REPORT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(100);

pub const MIN_CONSUMER_SESSION_HEARTBEAT_SIZE: usize =
    iggy_binary_protocol::HEADER_SIZE + iggy_binary_protocol::ConsumerSession::ENCODED_SIZE;
