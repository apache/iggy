/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
use iggy_common::IggyTimestamp;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Clone,
    Copy,
    AsRefStr,
    StrumDisplay,
    EnumString,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ConnectorStatus {
    /// Connector is initializing
    Starting,
    /// Connector is running normally
    Running,
    /// Connector is shutting down
    Stopping,
    /// Connector is stopped (disabled or shut down cleanly)
    #[default]
    Stopped,
    /// Connector has encountered an error
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorError {
    pub message: String,
    pub timestamp: IggyTimestamp,
}

impl ConnectorError {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            timestamp: IggyTimestamp::now(),
        }
    }
}
