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
    heartbeat_interval: IggyDuration,
    retries: u32,
}

impl ConnectionStringOptions for HttpConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        Some(self.retries)
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<HttpConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut heartbeat_interval = "5s".to_owned();
        let mut retries = 3;

        for option in options {
            let option_parts = option.split('=').collect::<Vec<&str>>();
            if option_parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }
            match option_parts[0] {
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

        let connection_string_options =
            HttpConnectionStringOptions::new(heartbeat_interval, retries);
        Ok(connection_string_options)
    }
}

impl HttpConnectionStringOptions {
    pub fn new(heartbeat_interval: IggyDuration, retries: u32) -> Self {
        Self {
            heartbeat_interval,
            retries,
        }
    }
}

impl Default for HttpConnectionStringOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            retries: 3,
        }
    }
}
