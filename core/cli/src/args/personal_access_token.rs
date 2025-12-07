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
use iggy::prelude::PersonalAccessTokenExpiry;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum PersonalAccessTokenAction {
    /// Create personal access token
    ///
    /// Create personal access token which allow authenticating the clients using
    /// a token, instead of the regular credentials (username and password)
    /// In quiet mode only the personal access token name is printed
    ///
    /// Examples
    ///  iggy pat create name
    ///  iggy pat create client 1day
    ///  iggy pat create sensor 3weeks
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(PersonalAccessTokenCreateArgs),
    /// Delete personal access token
    ///
    /// Examples
    ///  iggy pat delete name
    ///  iggy pat delete client
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(PersonalAccessTokenDeleteArgs),
    /// List all personal access tokens
    ///
    /// Examples
    ///  iggy pat list
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(PersonalAccessTokenListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PersonalAccessTokenCreateArgs {
    /// Name of the personal access token
    pub(crate) name: String,
    /// Personal access token expiry time in human-readable format
    ///
    /// Expiry time must be expressed in human-readable format like 15days 2min 2s
    /// ("none" or skipping parameter disables personal access token expiry)
    #[arg(value_parser = clap::value_parser!(PersonalAccessTokenExpiry), group = "store")]
    pub(crate) expiry: Option<Vec<PersonalAccessTokenExpiry>>,
    /// Store token in an underlying platform-specific secure store
    ///
    /// Generated token is stored in a platform-specific secure storage without revealing
    /// its content to the user. It can be used to authenticate on iggy server using
    /// associated name and -n/--token-name command line option instead of -u/--username
    /// and -p/--password or -t/--token. In quiet mode only the token name is printed.
    /// This option can only be used for creating tokens which does not have expiry time set.
    #[clap(short, long, default_value_t = false, group = "store")]
    pub(crate) store_token: bool,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PersonalAccessTokenDeleteArgs {
    /// Personal access token name to delete
    pub(crate) name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PersonalAccessTokenListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
