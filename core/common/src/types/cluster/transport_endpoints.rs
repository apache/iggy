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

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TransportEndpoints {
    pub tcp: u16,
    pub quic: u16,
    pub http: u16,
    pub websocket: u16,
}

impl TransportEndpoints {
    pub fn new(tcp: u16, quic: u16, http: u16, websocket: u16) -> Self {
        Self {
            tcp,
            quic,
            http,
            websocket,
        }
    }
}

impl Display for TransportEndpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tcp: {}, quic: {}, http: {}, websocket: {}",
            self.tcp, self.quic, self.http, self.websocket
        )
    }
}
