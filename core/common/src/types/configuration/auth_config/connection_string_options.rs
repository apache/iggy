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
use crate::{IggyDuration, TcpClientReconnectionConfig};
use std::str::FromStr;

#[derive(Debug)]
pub struct ConnectionStringOptions {
    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    reconnection: TcpClientReconnectionConfig,
    heartbeat_interval: IggyDuration,
    nodelay: bool,
}

impl ConnectionStringOptions {
    pub fn new(
        tls_enabled: bool,
        tls_domain: String,
        tls_ca_file: Option<String>,
        reconnection: TcpClientReconnectionConfig,
        heartbeat_interval: IggyDuration,
        nodelay: bool,
    ) -> Self {
        Self {
            tls_enabled,
            tls_domain,
            tls_ca_file,
            reconnection,
            heartbeat_interval,
            nodelay,
        }
    }
}

impl ConnectionStringOptions {
    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    pub fn tls_ca_file(&self) -> &Option<String> {
        &self.tls_ca_file
    }

    pub fn reconnection(&self) -> &TcpClientReconnectionConfig {
        &self.reconnection
    }

    pub fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    pub fn nodelay(&self) -> bool {
        self.nodelay
    }
}

impl Default for ConnectionStringOptions {
    fn default() -> Self {
        ConnectionStringOptions {
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            reconnection: Default::default(),
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            nodelay: false,
        }
    }
}
