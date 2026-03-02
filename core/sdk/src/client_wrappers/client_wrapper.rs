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

use crate::clients::client::IggyClient;
#[cfg(feature = "http")]
use crate::http::http_client::HttpClient;
#[cfg(feature = "quic")]
use crate::quic::quic_client::QuicClient;
#[cfg(feature = "tcp")]
use crate::tcp::tcp_client::TcpClient;
#[cfg(feature = "websocket")]
use crate::websocket::websocket_client::WebSocketClient;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientWrapper {
    Iggy(IggyClient),
    #[cfg(feature = "http")]
    Http(HttpClient),
    #[cfg(feature = "tcp")]
    Tcp(TcpClient),
    #[cfg(feature = "quic")]
    Quic(QuicClient),
    #[cfg(feature = "websocket")]
    WebSocket(WebSocketClient),
}
