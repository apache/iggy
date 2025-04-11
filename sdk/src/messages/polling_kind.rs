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

use crate::error::IggyError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `PollingKind` is an enum which specifies from where to start polling messages and is used by `PollingStrategy`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PollingKind {
    #[default]
    /// Start polling from the specified offset.
    Offset,
    /// Start polling from the specified timestamp.
    Timestamp,
    /// Start polling from the first message in the partition.
    First,
    /// Start polling from the last message in the partition.
    Last,
    /// Start polling from the next message after the last polled message based on the stored consumer offset. Should be used with `auto_commit` set to `true`.
    Next,
}

impl PollingKind {
    /// Returns code of the polling kind.
    pub fn as_code(&self) -> u8 {
        match self {
            PollingKind::Offset => 1,
            PollingKind::Timestamp => 2,
            PollingKind::First => 3,
            PollingKind::Last => 4,
            PollingKind::Next => 5,
        }
    }

    /// Returns polling kind from the specified code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(PollingKind::Offset),
            2 => Ok(PollingKind::Timestamp),
            3 => Ok(PollingKind::First),
            4 => Ok(PollingKind::Last),
            5 => Ok(PollingKind::Next),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl FromStr for PollingKind {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "o" | "offset" => Ok(PollingKind::Offset),
            "t" | "timestamp" => Ok(PollingKind::Timestamp),
            "f" | "first" => Ok(PollingKind::First),
            "l" | "last" => Ok(PollingKind::Last),
            "n" | "next" => Ok(PollingKind::Next),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for PollingKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingKind::Offset => write!(f, "offset"),
            PollingKind::Timestamp => write!(f, "timestamp"),
            PollingKind::First => write!(f, "first"),
            PollingKind::Last => write!(f, "last"),
            PollingKind::Next => write!(f, "next"),
        }
    }
}
