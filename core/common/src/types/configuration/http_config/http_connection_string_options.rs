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

use crate::{ConnectionStringOptions, IggyDuration, IggyError};
use std::str::FromStr;

#[derive(Debug)]
pub struct HttpConnectionStringOptions {
    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    heartbeat_interval: IggyDuration,
    retries: u32,
}

impl HttpConnectionStringOptions {
    pub fn retries(&self) -> u32 {
        self.retries
    }
}

impl ConnectionStringOptions for HttpConnectionStringOptions {
    fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    fn tls_ca_file(&self) -> &Option<String> {
        &self.tls_ca_file
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<HttpConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut tls_enabled = false;
        let mut tls_domain = "localhost".to_string();
        let mut tls_ca_file = None;
        let mut heartbeat_interval = "5s".to_owned();
        let mut retries = 3;

        for option in options {
            let option_parts = option.split('=').collect::<Vec<&str>>();
            if option_parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }
            match option_parts[0] {
                "tls" => {
                    tls_enabled = option_parts[1] == "true";
                }
                "tls_domain" => {
                    tls_domain = option_parts[1].to_string();
                }
                "tls_ca_file" => {
                    tls_ca_file = Some(option_parts[1].to_string());
                }
                "heartbeat_interval" => {
                    heartbeat_interval = option_parts[1].to_string();
                }
                "retries" => {
                    retries = option_parts[1]
                        .parse::<u32>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                _ => {
                    return Err(IggyError::InvalidConnectionString);
                }
            }
        }

        let heartbeat_interval = IggyDuration::from_str(heartbeat_interval.as_str())
            .map_err(|_| IggyError::InvalidConnectionString)?;

        let connection_string_options = HttpConnectionStringOptions::new(
            tls_enabled,
            tls_domain,
            tls_ca_file,
            heartbeat_interval,
            retries,
        );
        Ok(connection_string_options)
    }
}

impl HttpConnectionStringOptions {
    pub fn new(
        tls_enabled: bool,
        tls_domain: String,
        tls_ca_file: Option<String>,
        heartbeat_interval: IggyDuration,
        retries: u32,
    ) -> Self {
        Self {
            tls_enabled,
            tls_domain,
            tls_ca_file,
            heartbeat_interval,
            retries,
        }
    }
}

impl Default for HttpConnectionStringOptions {
    fn default() -> Self {
        Self {
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            retries: 3,
        }
    }
}
