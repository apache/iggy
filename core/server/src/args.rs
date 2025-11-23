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

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    author = "Apache Iggy (Incubating)",
    version,
    about = "Apache Iggy: Hyper-Efficient Message Streaming at Laser Speed",
    long_about = r#"Apache Iggy (Incubating) - A persistent message streaming platform written in Rust

Apache Iggy is a high-performance message streaming platform that supports QUIC, TCP, and HTTP
transport protocols, capable of processing millions of messages per second with low latency.

WEBSITE:
    https://iggy.apache.org

REPOSITORY:
    https://github.com/apache/iggy

DOCUMENTATION:
    https://iggy.apache.org/docs

CONFIGURATION:
    The server uses a TOML configuration file. By default, it looks for 'configs/server.toml'
    in the current working directory. You can override this with the IGGY_CONFIG_PATH environment
    variable or use the --config-provider flag.

    Examples:
        iggy-server                                    # Uses default file provider (configs/server.toml)
        iggy-server --config-provider file             # Explicitly use file provider
        IGGY_CONFIG_PATH=custom.toml iggy-server       # Use custom config file path

ENVIRONMENT VARIABLES:
    Any configuration value can be overridden using environment variables with the IGGY_ prefix.
    Use underscores to separate nested configuration keys (e.g., IGGY_TCP_ADDRESS=127.0.0.1:8090).

    Common examples:
        IGGY_TCP_ADDRESS=0.0.0.0:8090                  # Override TCP server address
        IGGY_HTTP_ENABLED=true                         # Enable HTTP transport
        IGGY_SYSTEM_PATH=/data/iggy                    # Set data storage path
        IGGY_SYSTEM_LOGGING_LEVEL=debug                # Set log level to debug

TRANSPORT PROTOCOLS:
    - TCP (binary protocol): High-performance, low-latency (default: 127.0.0.1:8090)
    - QUIC: Modern UDP-based protocol with built-in encryption (default: 127.0.0.1:8080)
    - HTTP: RESTful API for web integration (default: 127.0.0.1:3000, disabled by default)

GETTING STARTED:
    1. Start the server: iggy-server
    2. Install CLI: cargo install iggy-cli
    3. Create a stream: iggy stream create my-stream
    4. Create a topic: iggy topic create my-stream my-topic 1 none
    5. Send messages: echo "Hello, Iggy!" | iggy message send my-stream my-topic

For more information, visit: https://iggy.apache.org/docs/introduction/getting-started/"#
)]
pub struct Args {
    /// Configuration provider type
    ///
    /// Currently only 'file' provider is supported, which loads configuration from a TOML file.
    /// The file path can be specified via IGGY_CONFIG_PATH environment variable.
    #[arg(short, long, default_value = "file", verbatim_doc_comment)]
    pub config_provider: String,

    /// Remove system path before starting (WARNING: THIS WILL DELETE ALL DATA!)
    ///
    /// This flag will completely remove the system data directory (local_data by default)
    /// before starting the server. Use this for clean development setups or testing.
    ///
    /// Examples:
    ///   iggy-server --fresh                          # Start with fresh data directory
    ///   iggy-server -f                               # Short form
    #[arg(short, long, default_value_t = false, verbatim_doc_comment)]
    pub fresh: bool,

    /// Use default root credentials (INSECURE - FOR DEVELOPMENT ONLY!)
    ///
    /// When this flag is set, the root user will be created with username 'iggy'
    /// and password 'iggy' if it doesn't exist. If the root user already exists,
    /// this flag has no effect.
    ///
    /// This flag is equivalent to setting IGGY_ROOT_USERNAME=iggy and IGGY_ROOT_PASSWORD=iggy,
    /// but environment variables take precedence over this flag.
    ///
    /// WARNING: This is insecure and should only be used for development and testing!
    ///
    /// Examples:
    ///   iggy-server --with-default-root-credentials     # Use 'iggy/iggy' as root credentials
    #[arg(long, default_value_t = false, verbatim_doc_comment)]
    pub with_default_root_credentials: bool,

    /// Run server as a follower node (FOR TESTING LEADER REDIRECTION)
    ///
    /// When this flag is set, the server will report itself as a follower node
    /// in cluster metadata responses. This is useful for testing leader-aware
    /// client connections and redirection logic.
    ///
    /// The server will return cluster metadata showing this server as a follower node.
    ///
    /// Examples:
    ///   iggy-server                                      # Run as leader (default)
    ///   iggy-server --follower                           # Run as follower
    ///   IGGY_TCP_ADDRESS=127.0.0.1:8091 iggy-server --follower  # Follower on port 8091
    #[arg(long, default_value_t = false, verbatim_doc_comment)]
    pub follower: bool,
}
