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

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    author = "Apache Iggy",
    version,
    about = "Apache Iggy: Hyper-Efficient Message Streaming at Laser Speed",
    long_about = r#"Apache Iggy - a persistent message streaming platform written in Rust

Iggy stores every stream in a replicated log kept consistent by Viewstamped
Replication. One binary serves both the single-node and the clustered
deployment; the loaded configuration decides which one you get.

WEBSITE:
    https://iggy.apache.org

REPOSITORY:
    https://github.com/apache/iggy

DOCUMENTATION:
    https://iggy.apache.org/docs

CONFIGURATION:
    The server reads a TOML configuration file, by default 'core/server/config.toml'
    resolved against the current working directory. Point IGGY_CONFIG_PATH at
    another file to override it.

    Examples:
        iggy-server                                    # Default config file
        IGGY_CONFIG_PATH=custom.toml iggy-server       # Custom config file path

ENVIRONMENT VARIABLES:
    Any configuration value can be overridden with an IGGY_ prefixed variable;
    underscores separate the nested keys (IGGY_TCP_ADDRESS sets [tcp] address).
    A '.env' file in the working directory is loaded during startup, or the one
    named by IGGY_ENV_PATH.

    Common examples:
        IGGY_SYSTEM_PATH=/data/iggy                    # Data directory
        IGGY_TCP_ADDRESS=127.0.0.1:8090                # TCP listener address
        IGGY_HTTP_ADDRESS=0.0.0.0:3000                 # HTTP listener address
        IGGY_NODE_ADVERTISED_ADDRESS=localhost         # Address clients dial, required
                                                       # when a listener binds a wildcard
        IGGY_SYSTEM_LOGGING_LEVEL=debug                # Log level
        IGGY_ROOT_USERNAME=iggy                        # Root user, set with the password
        IGGY_ROOT_PASSWORD=secret                      # Root password, set with the username

TRANSPORT PROTOCOLS:
    - TCP (binary protocol)                            (default: 127.0.0.1:8090)
    - QUIC                                             (default: 127.0.0.1:8080)
    - WebSocket                                        (default: 127.0.0.1:8092)
    - HTTP (REST API)                                  (default: 127.0.0.1:3000)

GETTING STARTED:
    1. Start the server: iggy-server --fresh --with-default-root-credentials
    2. Install the CLI:  cargo install iggy-cli
    3. Create a stream:  iggy stream create my-stream
    4. Create a topic:   iggy topic create my-stream my-topic 1 none
    5. Send messages:    echo "Hello, Iggy!" | iggy message send my-stream my-topic

CLUSTER:
    Every node runs the same configuration file with cluster.enabled = true and
    is told apart only by --replica-id, which selects its own cluster.nodes entry:

        iggy-server --replica-id 0

For more information, visit: https://iggy.apache.org/docs/introduction/getting-started/"#
)]
// These doc comments are rendered verbatim as `--help` output, so environment
// variable names and paths must stay unquoted rather than wear rustdoc backticks.
#[allow(clippy::doc_markdown)]
pub struct Args {
    /// Remove the system path before starting (WARNING: THIS WILL DELETE ALL DATA!)
    ///
    /// Deletes the configured system data directory ('local_data' by default,
    /// see IGGY_SYSTEM_PATH) before the server boots, so it starts on empty
    /// state. Intended for clean development setups and testing.
    ///
    /// In cluster mode this wipes THIS replica only; it rejoins and refills by
    /// state transfer from the others. Wiping a quorum at the same time destroys
    /// committed data, and a service unit file carrying --fresh re-transfers the
    /// whole dataset on every restart.
    ///
    /// Examples:
    ///   iggy-server --fresh                             # Start with a fresh data directory
    ///   iggy-server -f                                  # Short form
    #[arg(short, long, default_value_t = false, verbatim_doc_comment)]
    pub fresh: bool,

    /// Use default root credentials (INSECURE - FOR DEVELOPMENT ONLY!)
    ///
    /// Sets IGGY_ROOT_USERNAME and IGGY_ROOT_PASSWORD to 'iggy' unless they are
    /// already present in the environment, so the flag is equivalent to
    /// exporting both by hand and the environment always takes precedence.
    ///
    /// Only the first creation of the root user reads these values. On an
    /// existing data directory the stored root user is recovered as it is and
    /// the flag has no effect.
    ///
    /// Examples:
    ///   iggy-server --with-default-root-credentials     # Root logs in as iggy/iggy
    #[arg(long, default_value_t = false, verbatim_doc_comment)]
    pub with_default_root_credentials: bool,

    /// Identifies this node within `cluster.nodes` by its replica ID.
    ///
    /// Required when `cluster.enabled = true`. The value must match exactly
    /// one `cluster.nodes[*].replica_id` entry in the loaded configuration.
    #[arg(long, verbatim_doc_comment)]
    pub replica_id: Option<u8>,
}
