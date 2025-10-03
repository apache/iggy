/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use clap::{Args, Subcommand};
use iggy::prelude::Identifier;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum PartitionAction {
    /// Create partitions for the specified topic ID
    /// and stream ID based on the given count.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy partition create 1 1 10
    ///  iggy partition create prod 2 2
    ///  iggy partition create test sensor 2
    ///  iggy partition create 1 sensor 16
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(PartitionCreateArgs),
    /// Delete partitions for the specified topic ID
    /// and stream ID based on the given count.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy partition delete 1 1 10
    ///  iggy partition delete prod 2 2
    ///  iggy partition delete test sensor 2
    ///  iggy partition delete 1 sensor 16
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(PartitionDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PartitionCreateArgs {
    /// Stream ID to create partitions
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to create partitions
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partitions count to be created
    #[arg(value_parser = clap::value_parser!(usize))]
    pub(crate) partitions_count: usize,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PartitionDeleteArgs {
    /// Stream ID to delete partitions
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete partitions
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partitions count to be deleted
    #[arg(value_parser = clap::value_parser!(usize))]
    pub(crate) partitions_count: usize,
}
