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

use iggy_common::ConfigEnv;
use iggy_common::IggyByteSize;
use iggy_common::IggyDuration;
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicConfig {
    pub enabled: bool,
    pub address: String,
    pub max_concurrent_bidi_streams: u64,
    #[config_env(leaf)]
    pub datagram_send_buffer_size: IggyByteSize,
    #[config_env(leaf)]
    pub initial_mtu: IggyByteSize,
    #[config_env(leaf)]
    pub send_window: IggyByteSize,
    #[config_env(leaf)]
    pub receive_window: IggyByteSize,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub keep_alive_interval: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub max_idle_timeout: IggyDuration,
    pub certificate: QuicCertificateConfig,
    pub socket: QuicSocketConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicSocketConfig {
    pub override_defaults: bool,
    #[config_env(leaf)]
    pub recv_buffer_size: IggyByteSize,
    #[config_env(leaf)]
    pub send_buffer_size: IggyByteSize,
    pub keepalive: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicCertificateConfig {
    pub self_signed: bool,
    pub cert_file: String,
    pub key_file: String,
}
