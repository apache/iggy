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

use crate::{BytesSerializable, IggyError};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

/// Node role within the cluster
#[derive(
    Debug, Serialize, Deserialize, PartialEq, Clone, Copy, AsRefStr, StrumDisplay, EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ClusterNodeRole {
    /// Primary/Leader node - handles all writes
    Leader,
    /// Follower/Secondary node - read replica
    Follower,
}

impl BytesSerializable for ClusterNodeRole {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(1);
        self.write_to_buffer(&mut bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<ClusterNodeRole, IggyError> {
        if bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        ClusterNodeRole::try_from(bytes[0])
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u8(*self as u8);
    }

    fn get_buffer_size(&self) -> usize {
        1
    }
}

impl TryFrom<u8> for ClusterNodeRole {
    type Error = IggyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            v if v == ClusterNodeRole::Leader as u8 => Ok(ClusterNodeRole::Leader),
            v if v == ClusterNodeRole::Follower as u8 => Ok(ClusterNodeRole::Follower),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
