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

pub mod connection_handler;
pub mod websocket_listener;
pub mod websocket_server;
pub mod websocket_tls_listener;

pub const COMPONENT: &str = "WEBSOCKET";

use crate::configs::websocket::WebSocketConfig;
use compio::ws::tungstenite::protocol::WebSocketConfig as CompioWsConfig;
use iggy_common::IggyByteSize;

/// Build a `compio-ws`-compatible `WebSocketConfig` from the server config.
///
/// The standalone `tungstenite` crate may be a different major version than
/// the one re-exported by `compio-ws`, so we construct the config through
/// `compio::ws::tungstenite` to guarantee type compatibility.
pub fn build_compio_ws_config(config: &WebSocketConfig) -> CompioWsConfig {
    let mut ws = CompioWsConfig::default();

    if let Some(ref s) = config.read_buffer_size
        && let Ok(b) = s.parse::<IggyByteSize>()
    {
        ws = ws.read_buffer_size(b.as_bytes_u64() as usize);
    }
    if let Some(ref s) = config.write_buffer_size
        && let Ok(b) = s.parse::<IggyByteSize>()
    {
        ws = ws.write_buffer_size(b.as_bytes_u64() as usize);
    }
    if let Some(ref s) = config.max_write_buffer_size
        && let Ok(b) = s.parse::<IggyByteSize>()
    {
        ws = ws.max_write_buffer_size(b.as_bytes_u64() as usize);
    }
    if let Some(ref s) = config.max_message_size
        && let Ok(b) = s.parse::<IggyByteSize>()
    {
        ws = ws.max_message_size(Some(b.as_bytes_u64() as usize));
    }
    if let Some(ref s) = config.max_frame_size
        && let Ok(b) = s.parse::<IggyByteSize>()
    {
        ws = ws.max_frame_size(Some(b.as_bytes_u64() as usize));
    }
    ws = ws.accept_unmasked_frames(config.accept_unmasked_frames);
    ws
}
