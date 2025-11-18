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

use crate::args::common::IggyBenchArgs;
use crate::args::transport::BenchmarkTransportCommand;
use iggy::prelude::TransportProtocol;
use integration::http_client::HttpClientFactory;
use integration::quic_client::QuicClientFactory;
use integration::tcp_client::TcpClientFactory;
use integration::test_server::ClientFactory;
use integration::websocket_client::WebSocketClientFactory;
use std::sync::Arc;

pub fn create_client_factory(args: &IggyBenchArgs) -> Arc<dyn ClientFactory> {
    match &args.transport() {
        TransportProtocol::Http => Arc::new(HttpClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
        TransportProtocol::Tcp => {
            let transport_command = args.transport_command();
            if let BenchmarkTransportCommand::Tcp(tcp_args) = transport_command {
                Arc::new(TcpClientFactory {
                    server_addr: args.server_address().to_owned(),
                    nodelay: args.nodelay(),
                    tls_enabled: tcp_args.tls,
                    tls_domain: tcp_args.tls_domain.clone(),
                    tls_ca_file: tcp_args.tls_ca_file.clone(),
                    tls_validate_certificate: tcp_args.tls_validate_certificate,
                })
            } else {
                unreachable!("Transport is TCP but transport command is not TcpArgs")
            }
        }
        TransportProtocol::Quic => Arc::new(QuicClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
        TransportProtocol::WebSocket => Arc::new(WebSocketClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
    }
}
