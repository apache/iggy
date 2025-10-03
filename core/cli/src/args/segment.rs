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
pub(crate) enum SegmentAction {
    /// Delete segments for the specified topic ID,
    /// stream ID and partition ID based on the given count.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// Partition ID can be specified as a name or ID
    ///
    /// Examples
    ///  iggy segment delete 1 1 1 10
    ///  iggy segment delete prod 2 2 2
    ///  iggy segment delete test sensor 2 2
    ///  iggy segment delete 1 sensor 2 16
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(SegmentDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct SegmentDeleteArgs {
    /// Stream ID to delete segments
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete segments
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partition ID to delete segments
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) partition_id: usize,
    /// Segments count to be deleted
    #[arg(value_parser = clap::value_parser!(u32).range(1..100_001))]
    pub(crate) segments_count: u32,
}
