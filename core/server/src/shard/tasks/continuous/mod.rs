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

#[cfg(feature = "http")]
mod http_server;
mod message_pump;
#[cfg(feature = "quic")]
mod quic_server;
#[cfg(feature = "tcp")]
mod tcp_server;
#[cfg(feature = "websocket")]
mod websocket_server;

#[cfg(feature = "http")]
pub use http_server::spawn_http_server;
pub use message_pump::spawn_message_pump;
#[cfg(feature = "quic")]
pub use quic_server::spawn_quic_server;
#[cfg(feature = "tcp")]
pub use tcp_server::spawn_tcp_server;
#[cfg(feature = "websocket")]
pub use websocket_server::spawn_websocket_server;
