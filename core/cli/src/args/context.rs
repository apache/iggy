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

    /// Show details of a specific context
    ///
    /// Displays the full configuration of a named context including
    /// transport, server addresses, TLS settings, and credentials.
    ///
    /// Examples
    ///  iggy context show default
    ///  iggy context show production
    #[clap(verbatim_doc_comment, visible_alias = "s")]
    Show(ContextShowArgs),
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

    /// The transport to use
    ///
    /// Valid values are "quic", "http", "tcp" and "ws".
    #[clap(verbatim_doc_comment, long)]
    pub(crate) transport: Option<String>,

    /// The server address for the TCP transport
    #[clap(long)]
    pub(crate) tcp_server_address: Option<String>,

    /// The API URL for the HTTP transport
    #[clap(long)]
    pub(crate) http_api_url: Option<String>,

    /// The server address for the QUIC transport
    #[clap(long)]
    pub(crate) quic_server_address: Option<String>,

    /// Flag to enable TLS for the TCP transport
    #[clap(long)]
    pub(crate) tcp_tls_enabled: Option<bool>,

    /// Iggy server username
    #[clap(short, long)]
    pub(crate) username: Option<String>,

    /// Iggy server password
    #[clap(short, long)]
    pub(crate) password: Option<String>,

    /// Iggy server personal access token
    #[clap(short, long)]
    pub(crate) token: Option<String>,

    /// Iggy server personal access token name
    #[clap(short = 'n', long)]
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
            extra: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextDeleteArgs {
    /// Name of the context to delete
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextShowArgs {
    /// Name of the context to show
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_create_args_to_context_config() {
        let args = ContextCreateArgs {
            context_name: "production".to_string(),
            transport: Some("tcp".to_string()),
            tcp_server_address: Some("10.0.0.1:8090".to_string()),
            http_api_url: None,
            quic_server_address: None,
            tcp_tls_enabled: Some(true),
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
            token: Some("tok123".to_string()),
            token_name: Some("my-token".to_string()),
        };

        let config: ContextConfig = args.into();

        assert_eq!(config.username.as_deref(), Some("admin"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.token.as_deref(), Some("tok123"));
        assert_eq!(config.token_name.as_deref(), Some("my-token"));
        assert_eq!(config.iggy.transport.as_deref(), Some("tcp"));
        assert_eq!(
            config.iggy.tcp_server_address.as_deref(),
            Some("10.0.0.1:8090")
        );
        assert!(config.iggy.http_api_url.is_none());
        assert!(config.iggy.quic_server_address.is_none());
        assert_eq!(config.iggy.tcp_tls_enabled, Some(true));
    }

    #[test]
    fn should_convert_create_args_with_defaults() {
        let args = ContextCreateArgs {
            context_name: "minimal".to_string(),
            transport: None,
            tcp_server_address: None,
            http_api_url: None,
            quic_server_address: None,
            tcp_tls_enabled: None,
            username: None,
            password: None,
            token: None,
            token_name: None,
        };

        let config: ContextConfig = args.into();

        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.token.is_none());
        assert!(config.token_name.is_none());
        assert!(config.iggy.transport.is_none());
    }
}
