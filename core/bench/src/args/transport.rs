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

use super::defaults::{
    DEFAULT_HTTP_SERVER_ADDRESS, DEFAULT_QUIC_CLIENT_ADDRESS, DEFAULT_QUIC_SERVER_ADDRESS,
    DEFAULT_QUIC_SERVER_NAME, DEFAULT_QUIC_VALIDATE_CERTIFICATE, DEFAULT_TCP_SERVER_ADDRESS,
    DEFAULT_WEBSOCKET_SERVER_ADDRESS,
};
use super::{output::BenchmarkOutputCommand, props::BenchmarkTransportProps};
use clap::{Parser, Subcommand};
use iggy::prelude::TransportProtocol;
use serde::{Serialize, Serializer};

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkTransportCommand {
    Http(HttpArgs),
    Tcp(TcpArgs),
    Quic(QuicArgs),
    #[command(alias = "ws")]
    WebSocket(WebSocketArgs),
}

impl Serialize for BenchmarkTransportCommand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let variant_str = match self {
            Self::Http(_) => "http",
            Self::Tcp(_) => "tcp",
            Self::Quic(_) => "quic",
            Self::WebSocket(_) => "websocket",
        };
        serializer.serialize_str(variant_str)
    }
}

impl BenchmarkTransportProps for BenchmarkTransportCommand {
    fn transport(&self) -> &TransportProtocol {
        self.inner().transport()
    }

    fn server_address(&self) -> &str {
        self.inner().server_address()
    }

    fn validate_certificate(&self) -> bool {
        self.inner().validate_certificate()
    }

    fn client_address(&self) -> &str {
        self.inner().client_address()
    }

    fn nodelay(&self) -> bool {
        self.inner().nodelay()
    }

    fn inner(&self) -> &dyn BenchmarkTransportProps {
        match self {
            Self::Http(args) => args,
            Self::Tcp(args) => args,
            Self::Quic(args) => args,
            Self::WebSocket(args) => args,
        }
    }

    fn output_command(&self) -> Option<&BenchmarkOutputCommand> {
        self.inner().output_command()
    }
}

#[derive(Parser, Debug, Clone)]
pub struct HttpArgs {
    /// Address of the HTTP iggy-server
    #[arg(long, default_value_t = DEFAULT_HTTP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    pub output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for HttpArgs {
    fn transport(&self) -> &TransportProtocol {
        &TransportProtocol::Http
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for HTTP transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for HTTP transport is not supported!")
    }

    fn nodelay(&self) -> bool {
        panic!("Setting nodelay for HTTP transport is not supported!")
    }

    fn output_command(&self) -> Option<&BenchmarkOutputCommand> {
        self.output.as_ref()
    }
}

#[derive(Parser, Debug, Clone)]
pub struct TcpArgs {
    /// Address of the TCP iggy-server
    #[arg(long, default_value_t = DEFAULT_TCP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Disable Nagle's algorithm
    #[arg(long, default_value_t = false)]
    pub nodelay: bool,

    /// Enable TLS encryption
    #[arg(long, default_value_t = false)]
    pub tls: bool,

    /// TLS domain name
    #[arg(long, default_value_t = String::from("localhost"), requires = "tls")]
    pub tls_domain: String,

    /// Validate TLS certificate
    #[arg(long, default_value_t = false, requires = "tls")]
    pub tls_validate_certificate: bool,

    /// Path to CA certificate file for TLS validation
    #[arg(long, requires = "tls")]
    pub tls_ca_file: Option<String>,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for TcpArgs {
    fn transport(&self) -> &TransportProtocol {
        &TransportProtocol::Tcp
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        self.tls_validate_certificate
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for TCP transport is not supported!")
    }

    fn nodelay(&self) -> bool {
        self.nodelay
    }

    fn output_command(&self) -> Option<&BenchmarkOutputCommand> {
        self.output.as_ref()
    }
}

#[derive(Parser, Debug, Clone)]
pub struct QuicArgs {
    /// Address to which the QUIC client will bind
    #[arg(long, default_value_t = DEFAULT_QUIC_CLIENT_ADDRESS.to_owned())]
    pub client_address: String,

    /// Address of the QUIC server
    #[arg(long, default_value_t = DEFAULT_QUIC_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Server name
    #[arg(long, default_value_t = DEFAULT_QUIC_SERVER_NAME.to_owned())]
    pub server_name: String,

    /// Flag, enables certificate validation
    #[arg(long, default_value_t = DEFAULT_QUIC_VALIDATE_CERTIFICATE)]
    pub validate_certificate: bool,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    pub output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for QuicArgs {
    fn transport(&self) -> &TransportProtocol {
        &TransportProtocol::Quic
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        self.validate_certificate
    }

    fn client_address(&self) -> &str {
        &self.client_address
    }

    fn nodelay(&self) -> bool {
        panic!("Setting nodelay for QUIC transport is not supported!")
    }

    fn output_command(&self) -> Option<&BenchmarkOutputCommand> {
        self.output.as_ref()
    }
}

#[derive(Parser, Debug, Clone)]
pub struct WebSocketArgs {
    /// Address of the WebSocket iggy-server
    #[arg(long, default_value_t = DEFAULT_WEBSOCKET_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    pub output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for WebSocketArgs {
    fn transport(&self) -> &TransportProtocol {
        &TransportProtocol::WebSocket
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for WebSocket transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for WebSocket transport is not supported!")
    }

    fn nodelay(&self) -> bool {
        panic!("Setting nodelay for WebSocket transport is not supported!")
    }

    fn output_command(&self) -> Option<&BenchmarkOutputCommand> {
        self.output.as_ref()
    }
}
