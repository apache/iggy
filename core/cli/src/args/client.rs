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

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ClientAction {
    /// Get details of a single client with given ID
    ///
    /// Client ID is unique numerical identifier not to be confused with the user.
    ///
    /// Examples:
    ///  iggy client get 42
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(ClientGetArgs),
    /// List all currently connected clients to iggy server
    ///
    /// Clients shall not to be confused with the users
    ///
    /// Examples:
    ///  iggy client list
    ///  iggy client list --list-mode table
    ///  iggy client list -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ClientListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ClientGetArgs {
    /// Client ID to get
    pub(crate) client_id: u32,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ClientListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
