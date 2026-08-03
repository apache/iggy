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

use crate::Identifier;
use crate::error::IggyError;
use crate::types::message::IggyMessage;
use std::fmt::Debug;

/// The trait represent the logic responsible for calculating the partition ID and is used by the `IggyClient`.
///
/// Iggy uses a hierarchical model for append-only logs. A stream contains topics which hold partitions. Each partition is an append-only log.[^note]
/// A producer of messages such as an [`IggyProducer`], that appends messages to the log, might want to choose to which partition to write the messages.
/// To do that, a producer can take a type that implements this trait.
/// This might be especially useful when computing the partition ID requires some client side info, i.e. stream ID, topic ID and [`IggyMessage`] attributes.
///
/// Note, that the [`Partitioning`] of a producer defines what _partitioning strategy_ is triggered on the server.
/// Using a [`Partitioner`] in a producer sets the strategy in to request a specific partition [`PartitioningKind::PartitionID`] calculated with [`Partitioner::calculate_partition_id()`].
///
/// [^note]: [Website docs on how Iggy organizes data.](https://iggy.apache.org/docs/#how-iggy-organizes-data)
pub trait Partitioner: Send + Sync + Debug {
    /// Calculate a partition ID.
    fn calculate_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        messages: &[IggyMessage],
    ) -> Result<u32, IggyError>;
}
