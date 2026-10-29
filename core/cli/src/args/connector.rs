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

use std::path::PathBuf;

use crate::args::common::ListMode;
use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ConnectorKind {
    Sink,
    Source,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ConnectorAction {
    /// List all sinks or sources
    ///
    /// Examples:
    ///  iggy connector list sink
    ///  iggy connector list source --list-mode list
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ConnectorListArgs),
    /// Get details of a single sink or source
    ///
    /// Examples:
    ///  iggy connector get sink my-sink
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(ConnectorGetArgs),
    /// List the transforms of a sink or source
    ///
    /// Examples:
    ///  iggy connector transforms sink my-sink
    #[clap(verbatim_doc_comment, visible_alias = "t")]
    Transforms(ConnectorGetArgs),
    /// Restart a sink or source
    ///
    /// On the local provider restart loads the newest stored configuration
    /// version, which may differ from the active one.
    #[clap(verbatim_doc_comment, visible_alias = "r")]
    Restart(ConnectorGetArgs),
    /// Configuration version operations
    #[command(subcommand, visible_alias = "c")]
    Config(ConnectorConfigAction),
    /// Get connectors runtime statistics
    ///
    /// Examples:
    ///  iggy connector stats
    #[clap(verbatim_doc_comment, visible_alias = "s")]
    Stats,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorListArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorGetArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ConnectorConfigAction {
    /// List all configuration versions of a sink or source
    ///
    /// Each version carries whether it is the active one. The responses
    /// include plugin credentials in plaintext.
    ///
    /// Examples:
    ///  iggy connector config list sink my-sink
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ConnectorGetArgs),
    /// Get one configuration version
    ///
    /// Without a version reads the active version. The response includes
    /// plugin credentials in plaintext.
    ///
    /// Examples:
    ///  iggy connector config get sink my-sink
    ///  iggy connector config get sink my-sink --version 3
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(ConnectorConfigGetArgs),
    /// Get the plugin configuration of a sink or source
    ///
    /// Prints the raw plugin configuration in the requested format.
    ///
    /// Examples:
    ///  iggy connector config plugin sink my-sink
    ///  iggy connector config plugin source my-source --format json
    #[clap(verbatim_doc_comment, visible_alias = "p")]
    Plugin(ConnectorConfigPluginArgs),
    /// Create a new configuration version
    ///
    /// Reads the JSON configuration from a file, or from stdin when no file
    /// is given. The new version starts inactive; activate it separately.
    ///
    /// Examples:
    ///  iggy connector config create sink my-sink --file config.json
    ///  cat config.json | iggy connector config create sink my-sink
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(ConnectorConfigCreateArgs),
    /// Delete a configuration version
    ///
    /// Without a version deletes the active version.
    ///
    /// Examples:
    ///  iggy connector config delete sink my-sink --version 3
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(ConnectorConfigDeleteArgs),
    /// Mark a version as active
    ///
    /// Only records the version; the running connector keeps its
    /// configuration until it restarts.
    ///
    /// Examples:
    ///  iggy connector config activate sink my-sink --version 3
    #[clap(verbatim_doc_comment, visible_alias = "a")]
    Activate(ConnectorConfigActivateArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigGetArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
    /// Configuration version, defaults to the active version
    #[clap(short, long)]
    pub(crate) version: Option<u64>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigPluginArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
    /// Output format (json, yaml, toml or text)
    #[clap(short, long)]
    pub(crate) format: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigCreateArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
    /// JSON configuration file, reads stdin when omitted
    #[clap(short, long)]
    pub(crate) file: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigDeleteArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
    /// Configuration version, defaults to the active version
    #[clap(short, long)]
    pub(crate) version: Option<u64>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigActivateArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    pub(crate) key: String,
    /// Configuration version to activate
    #[clap(short, long)]
    pub(crate) version: u64,
}
