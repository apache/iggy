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

use crate::{ConnectionStringOptions, IggyDuration, IggyError, WebSocketClientReconnectionConfig};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct WebSocketConnectionStringOptions {
    heartbeat_interval: IggyDuration,
    reconnection: WebSocketClientReconnectionConfig,

    read_buffer_size: Option<usize>,
    write_buffer_size: Option<usize>,
    max_write_buffer_size: Option<usize>,
    max_message_size: Option<usize>,
    max_frame_size: Option<usize>,

    accept_unmasked_frames: Option<bool>,

    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    tls_validate_certificate: bool,
}

impl WebSocketConnectionStringOptions {
    pub fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    pub fn reconnection(&self) -> &WebSocketClientReconnectionConfig {
        &self.reconnection
    }

    pub fn read_buffer_size(&self) -> Option<usize> {
        self.read_buffer_size
    }

    pub fn write_buffer_size(&self) -> Option<usize> {
        self.write_buffer_size
    }

    pub fn max_write_buffer_size(&self) -> Option<usize> {
        self.max_write_buffer_size
    }

    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }

    pub fn max_frame_size(&self) -> Option<usize> {
        self.max_frame_size
    }

    pub fn accept_unmasked_frames(&self) -> Option<bool> {
        self.accept_unmasked_frames
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    pub fn tls_ca_file(&self) -> Option<&str> {
        self.tls_ca_file.as_deref()
    }

    pub fn tls_validate_certificate(&self) -> bool {
        self.tls_validate_certificate
    }
}

impl ConnectionStringOptions for WebSocketConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options_str: &str) -> Result<Self, IggyError> {
        let mut parsed_options = WebSocketConnectionStringOptions::default();

        if options_str.is_empty() {
            return Ok(parsed_options);
        }

        for option in options_str.split('&') {
            let parts: Vec<&str> = option.split('=').collect();
            if parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }

            match parts[0] {
                "heartbeat_interval" => {
                    parsed_options.heartbeat_interval = IggyDuration::from_str(parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reconnection_retries" => {
                    let retries = match parts[1] {
                        "unlimited" => None,
                        val => Some(
                            val.parse::<u32>()
                                .map_err(|_| IggyError::InvalidConnectionString)?,
                        ),
                    };
                    parsed_options.reconnection.max_retries = retries;
                }
                "reconnection_interval" => {
                    parsed_options.reconnection.interval = IggyDuration::from_str(parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reestablish_after" => {
                    parsed_options.reconnection.reestablish_after =
                        IggyDuration::from_str(parts[1])
                            .map_err(|_| IggyError::InvalidConnectionString)?;
                }

                "read_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.read_buffer_size = Some(size);
                }
                "write_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.write_buffer_size = Some(size);
                }
                "max_write_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_write_buffer_size = Some(size);
                }
                "max_message_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_message_size = Some(size);
                }
                "max_frame_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_frame_size = Some(size);
                }

                "accept_unmasked_frames" => {
                    parsed_options.accept_unmasked_frames = Some(parts[1] == "true");
                }

                "tls" => {
                    parsed_options.tls_enabled = parts[1] == "true";
                }
                "tls_domain" => {
                    parsed_options.tls_domain = parts[1].to_string();
                }
                "tls_ca_file" => {
                    parsed_options.tls_ca_file = Some(parts[1].to_string());
                }
                "tls_validate_certificate" => {
                    parsed_options.tls_validate_certificate = parts[1] == "true";
                }

                _ => return Err(IggyError::InvalidConnectionString),
            }
        }
        Ok(parsed_options)
    }
}

impl Default for WebSocketConnectionStringOptions {
    fn default() -> Self {
        WebSocketConnectionStringOptions {
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            reconnection: WebSocketClientReconnectionConfig::default(),
            read_buffer_size: None,
            write_buffer_size: None,
            max_write_buffer_size: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: None,
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            tls_validate_certificate: false,
        }
    }
}
