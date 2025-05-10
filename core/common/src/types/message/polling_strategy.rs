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
use crate::types::message::polling_kind::PollingKind;
use crate::utils::timestamp::IggyTimestamp;
use crate::BytesSerializable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::Display;

/// Default value for the polling strategy.
const DEFAULT_POLLING_STRATEGY_VALUE: u64 = 0;

/// `PollingStrategy` specifies from where to start polling messages.
/// It has the following kinds:
/// - `Offset` - start polling from the specified offset.
/// - `Timestamp` - start polling from the specified timestamp.
/// - `First` - start polling from the first message in the partition.
/// - `Last` - start polling from the last message in the partition.
/// - `Next` - start polling from the next message after the last polled message based on the stored consumer offset.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub struct PollingStrategy {
    /// Kind of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "PollingStrategy::default_kind")]
    pub kind: PollingKind,
    /// Value of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "PollingStrategy::default_value")]
    pub value: u64,
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self {
            kind: PollingKind::Offset,
            value: 0,
        }
    }
}

impl Display for PollingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.value)
    }
}

impl PollingStrategy {
    /// Poll messages from the specified offset.
    pub fn offset(value: u64) -> Self {
        Self {
            kind: PollingKind::Offset,
            value,
        }
    }

    /// Poll messages from the specified timestamp.
    pub fn timestamp(value: IggyTimestamp) -> Self {
        Self {
            kind: PollingKind::Timestamp,
            value: value.into(),
        }
    }

    /// Poll messages from the first message in the partition.
    pub fn first() -> Self {
        Self {
            kind: PollingKind::First,
            value: 0,
        }
    }

    /// Poll messages from the last message in the partition.
    pub fn last() -> Self {
        Self {
            kind: PollingKind::Last,
            value: 0,
        }
    }

    /// Poll messages from the next message after the last polled message based on the stored consumer offset. Should be used with `auto_commit` set to `true`.
    pub fn next() -> Self {
        Self {
            kind: PollingKind::Next,
            value: 0,
        }
    }

    /// Change the value of the polling strategy, affects only `Offset` and `Timestamp` kinds.
    pub fn set_value(&mut self, value: u64) {
        if self.kind == PollingKind::Offset || self.kind == PollingKind::Timestamp {
            self.value = value;
        }
    }

    /// Returns default value of the polling strategy.
    pub fn default_value() -> u64 {
        DEFAULT_POLLING_STRATEGY_VALUE
    }

    /// Returns default kind of the polling strategy.
    pub fn default_kind() -> PollingKind {
        PollingKind::default()
    }
}

impl BytesSerializable for PollingStrategy {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(9);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u64_le(self.value);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() != 9 {
            return Err(IggyError::InvalidCommand);
        }

        let kind = PollingKind::from_code(bytes[0])?;
        let value = u64::from_le_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let strategy = PollingStrategy { kind, value };
        Ok(strategy)
    }
}
