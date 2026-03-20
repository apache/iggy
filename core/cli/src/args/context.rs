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
use iggy_cli::commands::binary_context::common::ContextConfig;
use iggy_common::ArgsOptional;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ContextAction {
    /// List all contexts
    ///
    /// Examples
    ///  iggy context list
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ContextListArgs),

    /// Set the active context
    ///
    /// Examples
    ///  iggy context use dev
    ///  iggy context use default
    #[clap(verbatim_doc_comment, visible_alias = "u")]
    Use(ContextUseArgs),

    /// Create a new context
    ///
    /// Creates a new named context in the contexts configuration file.
    /// After creating a context, use 'iggy context use <name>' to activate it.
    ///
    /// Examples
    ///  iggy context create production --transport tcp --tcp-server-address 10.0.0.1:8090
    ///  iggy context create dev --transport http --http-api-url http://localhost:3000
    ///  iggy context create local --username iggy --password iggy
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(ContextCreateArgs),

    /// Delete an existing context
    ///
    /// Removes a named context from the contexts configuration file.
    /// The 'default' context cannot be deleted. If the deleted context
    /// was the active context, the active context resets to 'default'.
    ///
    /// Examples
    ///  iggy context delete production
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(ContextDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextUseArgs {
    /// Name of the context to use
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextCreateArgs {
    /// Name of the context to create
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,

    /// Transport protocol (tcp, quic, http, ws)
    #[clap(long)]
    pub(crate) transport: Option<String>,

    /// TCP server address (e.g., 127.0.0.1:8090)
    #[clap(long)]
    pub(crate) tcp_server_address: Option<String>,

    /// HTTP API URL (e.g., http://localhost:3000)
    #[clap(long)]
    pub(crate) http_api_url: Option<String>,

    /// QUIC server address (e.g., 127.0.0.1:8080)
    #[clap(long)]
    pub(crate) quic_server_address: Option<String>,

    /// Enable TLS for TCP transport
    #[clap(long)]
    pub(crate) tcp_tls_enabled: Option<bool>,

    /// Username for authentication
    #[clap(long)]
    pub(crate) username: Option<String>,

    /// Password for authentication
    #[clap(long)]
    pub(crate) password: Option<String>,

    /// Personal access token
    #[clap(long)]
    pub(crate) token: Option<String>,

    /// Personal access token name (for keyring lookup)
    #[clap(long)]
    pub(crate) token_name: Option<String>,
}

impl From<ContextCreateArgs> for ContextConfig {
    fn from(args: ContextCreateArgs) -> Self {
        ContextConfig {
            username: args.username,
            password: args.password,
            token: args.token,
            token_name: args.token_name,
            iggy: ArgsOptional {
                transport: args.transport,
                tcp_server_address: args.tcp_server_address,
                http_api_url: args.http_api_url,
                quic_server_address: args.quic_server_address,
                tcp_tls_enabled: args.tcp_tls_enabled,
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextDeleteArgs {
    /// Name of the context to delete
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,
}
