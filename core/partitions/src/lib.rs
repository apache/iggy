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

mod iggy_partition;
mod iggy_partitions;
mod types;

pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{PollMetadata, PollingArgs, PollingConsumer, SendMessagesResult};

/// The core abstraction for partition operations in clustering.
///
/// This trait defines the data-plane operations for partitions that
/// need to be coordinated across a cluster using viewstamped replication.
/// Implementations can vary between single-node and clustered deployments.
pub trait Partitions {
    // TODO(hubcio): define partition operations like poll, send, create, delete, etc.
}
