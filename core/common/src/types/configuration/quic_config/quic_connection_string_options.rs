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

use crate::{ConnectionStringOptions, IggyDuration, IggyError, QuicClientReconnectionConfig};
use std::str::FromStr;

#[derive(Debug)]
pub struct QuicConnectionStringOptions {
    reconnection: QuicClientReconnectionConfig,
    response_buffer_size: u64,
    max_concurrent_bidi_streams: u64,
    datagram_send_buffer_size: u64,
    initial_mtu: u16,
    send_window: u64,
    receive_window: u64,
    keep_alive_interval: u64,
    max_idle_timeout: u64,
    validate_certificate: bool,
    heartbeat_interval: IggyDuration,
}

impl QuicConnectionStringOptions {
    pub fn reconnection(&self) -> &QuicClientReconnectionConfig {
        &self.reconnection
    }

    pub fn response_buffer_size(&self) -> u64 {
        self.response_buffer_size
    }

    pub fn max_concurrent_bidi_streams(&self) -> u64 {
        self.max_concurrent_bidi_streams
    }

    pub fn datagram_send_buffer_size(&self) -> u64 {
        self.datagram_send_buffer_size
    }

    pub fn initial_mtu(&self) -> u16 {
        self.initial_mtu
    }

    pub fn send_window(&self) -> u64 {
        self.send_window
    }

    pub fn receive_window(&self) -> u64 {
        self.receive_window
    }

    pub fn keep_alive_interval(&self) -> u64 {
        self.keep_alive_interval
    }

    pub fn max_idle_timeout(&self) -> u64 {
        self.max_idle_timeout
    }

    pub fn validate_certificate(&self) -> bool {
        self.validate_certificate
    }
}

impl ConnectionStringOptions for QuicConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<QuicConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut response_buffer_size = 1000 * 1000 * 10;
        let mut max_concurrent_bidi_streams = 10000;
        let mut datagram_send_buffer_size = 100_000;
        let mut initial_mtu = 1200;
        let mut send_window = 100_000;
        let mut receive_window = 100_000;
        let mut keep_alive_interval = 5000;
        let mut max_idle_timeout = 10000;
        let mut validate_certificate = false;
        let mut heartbeat_interval = "5s".to_owned();

        // For reconnection config
        let mut reconnection_max_retries = "unlimited".to_owned();
        let mut reconnection_interval = "1s".to_owned();
        let mut reconnection_reestablish_after = "5s".to_owned();

        for option in options {
            let option_parts = option.split('=').collect::<Vec<&str>>();
            if option_parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }
            match option_parts[0] {
                "response_buffer_size" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        response_buffer_size = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "max_concurrent_bidi_streams" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        max_concurrent_bidi_streams = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "datagram_send_buffer_size" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        datagram_send_buffer_size = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "initial_mtu" => match option_parts[1].parse::<u16>() {
                    Ok(value) => {
                        initial_mtu = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "send_window" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        send_window = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "receive_window" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        receive_window = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "keep_alive_interval" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        keep_alive_interval = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "max_idle_timeout" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        max_idle_timeout = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "validate_certificate" => {
                    validate_certificate = option_parts[1] == "true";
                }
                "heartbeat_interval" => {
                    heartbeat_interval = option_parts[1].to_string();
                }
                "reconnection_max_retries" => {
                    reconnection_max_retries = option_parts[1].to_string();
                }
                "reconnection_interval" => {
                    reconnection_interval = option_parts[1].to_string();
                }
                "reconnection_reestablish_after" => {
                    reconnection_reestablish_after = option_parts[1].to_string();
                }
                _ => {
                    return Err(IggyError::InvalidConnectionString);
                }
            }
        }

        let reconnection = QuicClientReconnectionConfig {
            enabled: true,
            max_retries: match reconnection_max_retries.as_str() {
                "unlimited" => None,
                _ => Some(
                    reconnection_max_retries
                        .parse()
                        .map_err(|_| IggyError::InvalidNumberValue)?,
                ),
            },
            interval: IggyDuration::from_str(reconnection_interval.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
            reestablish_after: IggyDuration::from_str(reconnection_reestablish_after.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
        };

        let heartbeat_interval = IggyDuration::from_str(heartbeat_interval.as_str())
            .map_err(|_| IggyError::InvalidConnectionString)?;

        let connection_string_options = QuicConnectionStringOptions::new(
            reconnection,
            response_buffer_size,
            max_concurrent_bidi_streams,
            datagram_send_buffer_size,
            initial_mtu,
            send_window,
            receive_window,
            keep_alive_interval,
            max_idle_timeout,
            validate_certificate,
            heartbeat_interval,
        );

        Ok(connection_string_options)
    }
}

impl QuicConnectionStringOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reconnection: QuicClientReconnectionConfig,
        response_buffer_size: u64,
        max_concurrent_bidi_streams: u64,
        datagram_send_buffer_size: u64,
        initial_mtu: u16,
        send_window: u64,
        receive_window: u64,
        keep_alive_interval: u64,
        max_idle_timeout: u64,
        validate_certificate: bool,
        heartbeat_interval: IggyDuration,
    ) -> Self {
        Self {
            reconnection,
            response_buffer_size,
            max_concurrent_bidi_streams,
            datagram_send_buffer_size,
            initial_mtu,
            send_window,
            receive_window,
            keep_alive_interval,
            max_idle_timeout,
            validate_certificate,
            heartbeat_interval,
        }
    }
}

impl Default for QuicConnectionStringOptions {
    fn default() -> Self {
        QuicConnectionStringOptions {
            reconnection: Default::default(),
            response_buffer_size: 1000 * 1000 * 10,
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100_000,
            initial_mtu: 1200,
            send_window: 100_000,
            receive_window: 100_000,
            keep_alive_interval: 5000,
            max_idle_timeout: 10000,
            validate_certificate: false,
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
        }
    }
}
