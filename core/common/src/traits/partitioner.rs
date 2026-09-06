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
/// A producer of messages such as an `IggyProducer`, that appends messages to the log, may want to choose which partition to write the messages into.
/// To do that, a producer can take a type that implements this trait.
/// This is especially useful when computing the partition ID requires some client side info, i.e. stream ID, topic ID and/ or [`IggyMessage`] attributes.
///
/// Note the difference between [`Partitioning`] and [`Partitioner`]. [`Partitioning`] is a type used to set the _partitioning strategy_ for a producer.
/// If you use both, the [`Partitioner`] overwrites the strategy, sets it to [`PartitioningKind::PartitionId`] and the partition ID is
/// calculated with the logic implemented in [`Partitioner::calculate_partition_id()`].
///
/// [^note]: [Website docs on how Iggy organizes data.](https://iggy.apache.org/docs/#how-iggy-organizes-data)
///
/// [`Partitioning`]: crate::Partitioning
/// [`PartitioningKind::PartitionId`]: crate::PartitioningKind::PartitionId
pub trait Partitioner: Send + Sync + Debug {
    /// Calculate a partition ID.
    fn calculate_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        messages: &[IggyMessage],
    ) -> Result<u32, IggyError>;
}
