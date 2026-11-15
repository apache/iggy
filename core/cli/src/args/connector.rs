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

const MAX_CONNECTOR_KEY_LENGTH: usize = 128;

/// Mirrors the runtime's `ConnectorKey` so a key cannot add `/`, `?`, `#` or
/// `..` to the request path.
fn parse_connector_key(key: &str) -> Result<String, String> {
    let mut chars = key.chars();
    let valid = key.len() <= MAX_CONNECTOR_KEY_LENGTH
        && chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(key.to_string())
    } else {
        Err(format!(
            "must be at most {MAX_CONNECTOR_KEY_LENGTH} bytes, start with a letter or digit and contain only letters, digits, '.', '_' and '-'"
        ))
    }
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
    #[clap(value_parser = parse_connector_key)]
    pub(crate) key: String,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ConnectorConfigAction {
    /// List all configuration versions of a sink or source
    ///
    /// The Running column marks the version the connector runs now. On the
    /// local provider a restart loads the highest stored version, and the
    /// activated one applies only when the runtime process starts. The
    /// responses include plugin credentials in plaintext.
    ///
    /// Examples:
    ///  iggy connector config list sink my-sink
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ConnectorGetArgs),
    /// Get one configuration version
    ///
    /// Without a version reads the version the connector runs now. The
    /// response includes plugin credentials in plaintext.
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
    /// The version is required: the runtime deletes the active version when
    /// none is given. Deleting the active version activates the newest
    /// remaining one.
    ///
    /// Examples:
    ///  iggy connector config delete sink my-sink --version 3
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(ConnectorConfigDeleteArgs),
    /// Mark a version as active
    ///
    /// Only records the version; the running connector keeps its
    /// configuration. On the local provider the marker applies when the
    /// runtime process starts, not on a connector restart.
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
    #[clap(value_parser = parse_connector_key)]
    pub(crate) key: String,
    /// Configuration version, defaults to the running version
    #[clap(short, long)]
    pub(crate) version: Option<u64>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigPluginArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    #[clap(value_parser = parse_connector_key)]
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
    #[clap(value_parser = parse_connector_key)]
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
    #[clap(value_parser = parse_connector_key)]
    pub(crate) key: String,
    /// Configuration version to delete
    #[clap(short, long)]
    pub(crate) version: u64,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ConnectorConfigActivateArgs {
    /// Connector kind
    pub(crate) kind: ConnectorKind,
    /// Connector key
    #[clap(value_parser = parse_connector_key)]
    pub(crate) key: String,
    /// Configuration version to activate
    #[clap(short, long)]
    pub(crate) version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::{Command, IggyConsoleArgs};
    use clap::Parser;
    use clap::error::ErrorKind;
    use iggy_cli::commands::binary_connectors::get_connectors::GetConnectorsOutput;

    fn parse(args: &[&str]) -> Result<IggyConsoleArgs, clap::Error> {
        IggyConsoleArgs::try_parse_from(std::iter::once("iggy").chain(args.iter().copied()))
    }

    fn action(args: &[&str]) -> ConnectorAction {
        match parse(args).unwrap().command {
            Some(Command::Connector(action)) => action,
            _ => panic!("not a connector command"),
        }
    }

    fn config_action(args: &[&str]) -> ConnectorConfigAction {
        match action(args) {
            ConnectorAction::Config(action) => action,
            _ => panic!("not a config command"),
        }
    }

    #[test]
    fn should_list_in_table_mode_by_default() {
        let ConnectorAction::List(args) = action(&["connector", "list", "sink"]) else {
            panic!("not list");
        };

        assert_eq!(args.kind, ConnectorKind::Sink);
        assert!(matches!(args.list_mode, ListMode::Table));
    }

    #[test]
    fn should_accept_aliases() {
        let ConnectorAction::List(args) = action(&["conn", "l", "source", "-l", "list"]) else {
            panic!("not list");
        };

        assert_eq!(args.kind, ConnectorKind::Source);
        assert!(matches!(args.list_mode, ListMode::List));
        assert!(matches!(
            config_action(&["conn", "c", "g", "sink", "k"]),
            ConnectorConfigAction::Get(_)
        ));
    }

    #[test]
    fn should_require_a_valid_kind() {
        assert_eq!(
            parse(&["connector", "get"]).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
        assert_eq!(
            parse(&["connector", "get", "orders-sink"])
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidValue
        );
    }

    #[test]
    fn should_reject_keys_that_change_the_request_path() {
        for key in [
            "x/../../sources/y",
            "..",
            ".hidden",
            "a?b",
            "a#b",
            "a b",
            "",
        ] {
            assert!(
                parse(&["connector", "get", "sink", key]).is_err(),
                "key {key:?} was accepted"
            );
        }
        assert!(parse(&["connector", "get", "sink", "orders_sink-1.v2"]).is_ok());
    }

    #[test]
    fn should_limit_key_length_like_the_runtime() {
        let longest = "k".repeat(MAX_CONNECTOR_KEY_LENGTH);
        assert!(parse(&["connector", "get", "sink", &longest]).is_ok());
        let too_long = "k".repeat(MAX_CONNECTOR_KEY_LENGTH + 1);
        assert!(parse(&["connector", "get", "sink", &too_long]).is_err());
    }

    #[test]
    fn should_require_a_key() {
        assert_eq!(
            parse(&["connector", "restart", "sink"]).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn should_read_active_config_when_no_version_is_given() {
        let ConnectorConfigAction::Get(args) =
            config_action(&["connector", "config", "get", "sink", "k"])
        else {
            panic!("not get");
        };
        assert_eq!(args.version, None);

        let ConnectorConfigAction::Get(args) =
            config_action(&["connector", "config", "get", "sink", "k", "--version", "3"])
        else {
            panic!("not get");
        };
        assert_eq!(args.version, Some(3));
    }

    #[test]
    fn should_require_a_version_to_delete() {
        assert_eq!(
            parse(&["connector", "config", "delete", "sink", "k"])
                .unwrap_err()
                .kind(),
            ErrorKind::MissingRequiredArgument
        );

        let ConnectorConfigAction::Delete(args) =
            config_action(&["connector", "config", "delete", "sink", "k", "-v", "3"])
        else {
            panic!("not delete");
        };
        assert_eq!(args.version, 3);
    }

    #[test]
    fn should_require_a_version_to_activate() {
        assert_eq!(
            parse(&["connector", "config", "activate", "source", "k"])
                .unwrap_err()
                .kind(),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn should_read_create_file_only_when_given() {
        let ConnectorConfigAction::Create(args) =
            config_action(&["connector", "config", "create", "sink", "k"])
        else {
            panic!("not create");
        };
        assert_eq!(args.file, None);

        let ConnectorConfigAction::Create(args) = config_action(&[
            "connector",
            "config",
            "create",
            "sink",
            "k",
            "--file",
            "c.json",
        ]) else {
            panic!("not create");
        };
        assert_eq!(args.file, Some(PathBuf::from("c.json")));
    }

    #[test]
    fn should_pass_plugin_format() {
        let ConnectorConfigAction::Plugin(args) = config_action(&[
            "connector",
            "config",
            "plugin",
            "source",
            "k",
            "--format",
            "yaml",
        ]) else {
            panic!("not plugin");
        };

        assert_eq!(args.format.as_deref(), Some("yaml"));
    }

    #[test]
    fn should_parse_stats_without_kind() {
        assert!(matches!(
            action(&["connector", "stats"]),
            ConnectorAction::Stats
        ));
    }

    #[test]
    fn should_read_connector_options_next_to_the_command() {
        let args = parse(&[
            "--connectors-api-url",
            "https://runtime:8081",
            "--connectors-api-key",
            "secret",
            "--connectors-tls-ca-file",
            "ca.pem",
            "connector",
            "stats",
        ])
        .unwrap();

        assert_eq!(
            args.cli.connectors_api_url.as_deref(),
            Some("https://runtime:8081")
        );
        assert_eq!(args.cli.connectors_api_key.as_deref(), Some("secret"));
        assert_eq!(
            args.cli.connectors_tls_ca_file,
            Some(PathBuf::from("ca.pem"))
        );
    }

    #[test]
    fn should_map_list_mode_to_output() {
        assert!(matches!(
            GetConnectorsOutput::from(ListMode::Table),
            GetConnectorsOutput::Table
        ));
        assert!(matches!(
            GetConnectorsOutput::from(ListMode::List),
            GetConnectorsOutput::List
        ));
    }
}
