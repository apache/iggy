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

use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use iggy::prelude::Identifier;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ConsumerGroupAction {
    /// Create consumer group with given ID and name for given stream ID and topic ID.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// If group ID is not provided then the server will automatically assign it
    ///
    /// Examples:
    ///  iggy consumer-group create 1 1 prod
    ///  iggy consumer-group create stream 2 test
    ///  iggy consumer-group create 2 topic receiver
    ///  iggy consumer-group create -g 4 stream topic group
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(ConsumerGroupCreateArgs),
    /// Delete consumer group with given ID for given stream ID and topic ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// Consumer group ID can be specified as a consumer group name or ID
    ///
    /// Examples:
    ///  iggy consumer-group delete 1 2 3
    ///  iggy consumer-group delete stream 2 3
    ///  iggy consumer-group delete 1 topic 3
    ///  iggy consumer-group delete 1 2 group
    ///  iggy consumer-group delete stream topic 3
    ///  iggy consumer-group delete 1 topic group
    ///  iggy consumer-group delete stream 2 group
    ///  iggy consumer-group delete stream topic group
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(ConsumerGroupDeleteArgs),
    /// Get details of a single consumer group with given ID for given stream ID and topic ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// Consumer group ID can be specified as a consumer group name or ID
    ///
    /// Examples:
    ///  iggy consumer-group get 1 2 3
    ///  iggy consumer-group get stream 2 3
    ///  iggy consumer-group get 1 topic 3
    ///  iggy consumer-group get 1 2 group
    ///  iggy consumer-group get stream topic 3
    ///  iggy consumer-group get 1 topic group
    ///  iggy consumer-group get stream 2 group
    ///  iggy consumer-group get stream topic group
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(ConsumerGroupGetArgs),
    /// List all consumer groups for given stream ID and topic ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples:
    ///  iggy consumer-group list 1 1
    ///  iggy consumer-group list stream 2 --list-mode table
    ///  iggy consumer-group list 3 topic -l table
    ///  iggy consumer-group list production sensor -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ConsumerGroupListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConsumerGroupCreateArgs {
    /// Stream ID to create consumer group
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to create consumer group
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Consumer group ID to create
    #[clap(short, long)]
    pub(crate) group_id: Option<u32>,
    /// Consumer group name to create
    pub(crate) name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConsumerGroupDeleteArgs {
    /// Stream ID to delete consumer group
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete consumer group
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Consumer group ID to delete
    ///
    /// Consumer group ID can be specified as a consumer group name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) group_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConsumerGroupGetArgs {
    /// Stream ID to get consumer group
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to get consumer group
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Consumer group ID to get
    ///
    /// Consumer group ID can be specified as a consumer group name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) group_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConsumerGroupListArgs {
    /// Stream ID to list consumer groups
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to list consumer groups
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
