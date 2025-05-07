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

use iggy_common::{IggyByteSize, IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpConfig {
    pub enabled: bool,
    pub address: String,
    pub ipv6: bool,
    pub tls: TcpTlsConfig,
    pub socket: TcpSocketConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpTlsConfig {
    pub enabled: bool,
    pub certificate: String,
    pub password: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TcpSocketConfig {
    pub override_defaults: bool,
    pub recv_buffer_size: IggyByteSize,
    pub send_buffer_size: IggyByteSize,
    pub keepalive: bool,
    pub nodelay: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub linger: IggyDuration,
}
