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

use iggy_common::IggyByteSize;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tungstenite::protocol::WebSocketConfig as TungsteniteConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub address: String,
    #[serde(default)]
    pub read_buffer_size: Option<String>,
    #[serde(default)]
    pub write_buffer_size: Option<String>,
    #[serde(default)]
    pub max_write_buffer_size: Option<String>,
    #[serde(default)]
    pub max_message_size: Option<String>,
    #[serde(default)]
    pub max_frame_size: Option<String>,
    #[serde(default)]
    pub accept_unmasked_frames: bool,
    #[serde(default)]
    pub tls: WebSocketTlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebSocketTlsConfig {
    pub enabled: bool,
    pub self_signed: bool,
    pub cert_file: String,
    pub key_file: String,
}

impl WebSocketConfig {
    pub fn to_tungstenite_config(&self) -> TungsteniteConfig {
        let mut config = TungsteniteConfig::default();

        if let Some(read_buf_size_str) = &self.read_buffer_size
            && let Ok(byte_size) = read_buf_size_str.parse::<IggyByteSize>()
        {
            config = config.read_buffer_size(byte_size.as_bytes_u64() as usize);
        }

        if let Some(write_buf_size_str) = &self.write_buffer_size
            && let Ok(byte_size) = write_buf_size_str.parse::<IggyByteSize>()
        {
            config = config.write_buffer_size(byte_size.as_bytes_u64() as usize);
        }

        if let Some(max_write_buf_size_str) = &self.max_write_buffer_size
            && let Ok(byte_size) = max_write_buf_size_str.parse::<IggyByteSize>()
        {
            config = config.max_write_buffer_size(byte_size.as_bytes_u64() as usize);
        }

        if let Some(msg_size_str) = &self.max_message_size
            && let Ok(byte_size) = msg_size_str.parse::<IggyByteSize>()
        {
            config = config.max_message_size(Some(byte_size.as_bytes_u64() as usize));
        }

        if let Some(frame_size_str) = &self.max_frame_size
            && let Ok(byte_size) = frame_size_str.parse::<IggyByteSize>()
        {
            config = config.max_frame_size(Some(byte_size.as_bytes_u64() as usize));
        }

        config = config.accept_unmasked_frames(self.accept_unmasked_frames);

        config
    }
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
