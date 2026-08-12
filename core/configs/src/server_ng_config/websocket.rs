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

//! Server-ng WebSocket listener schema.
//!
//! Unlike the legacy [`crate::websocket::WebSocketConfig`] it was
//! forked from, this section is the live frame-tuning source for
//! server-ng's WS / WSS plane: the message bus folds the
//! `Option<IggyByteSize>` knobs below into a compio-ws
//! `WebSocketConfig` once at bus construction. The sizes are strictly
//! typed, so a malformed size string fails config load instead of
//! being silently ignored at conversion time. The conversion itself
//! lives in `core/message_bus` because the standalone `tungstenite`
//! dependency and the compio-ws re-export are different major versions
//! with incompatible config types.

use configs::ConfigEnv;
use iggy_common::IggyByteSize;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::fmt::{Display, Formatter};

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub address: String,

    /// Target minimum size of the frame read buffer. `None` keeps the
    /// compio-ws default (currently 128 KiB).
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub read_buffer_size: Option<IggyByteSize>,

    /// Target buffer size for batched writes before compio-ws flushes.
    /// `None` keeps the compio-ws default (currently 128 KiB).
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub write_buffer_size: Option<IggyByteSize>,

    /// Hard ceiling on the write buffer; writes past it error instead
    /// of buffering. Must exceed [`Self::write_buffer_size`] by at
    /// least one frame. `None` keeps the compio-ws default
    /// (unlimited).
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_write_buffer_size: Option<IggyByteSize>,

    /// Hard upper bound on a single inbound WebSocket message
    /// (post-fragment-reassembly). `None` keeps the compio-ws default
    /// (currently 64 MiB).
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_message_size: Option<IggyByteSize>,

    /// Hard upper bound on a single inbound WebSocket frame
    /// (pre-fragment-reassembly). `None` keeps the compio-ws default
    /// (currently 16 MiB).
    #[config_env(leaf)]
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_frame_size: Option<IggyByteSize>,

    /// Whether to accept unmasked frames from clients in violation of
    /// RFC 6455 client-to-server framing rules. Strict (`false`) by
    /// default; enable only for non-browser test clients that emit
    /// unmasked frames.
    #[serde(default)]
    pub accept_unmasked_frames: bool,

    #[serde(default)]
    pub tls: WebSocketTlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct WebSocketTlsConfig {
    pub enabled: bool,
    pub self_signed: bool,
    pub cert_file: String,
    pub key_file: String,
}

impl Display for WebSocketConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, read_buffer_size: {:?}, write_buffer_size: {:?}, max_write_buffer_size: {:?}, max_message_size: {:?}, max_frame_size: {:?}, accept_unmasked_frames: {} }}",
            self.enabled,
            self.address,
            self.read_buffer_size,
            self.write_buffer_size,
            self.max_write_buffer_size,
            self.max_message_size,
            self.max_frame_size,
            self.accept_unmasked_frames
        )
    }
}
