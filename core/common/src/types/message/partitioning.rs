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

use super::PartitioningKind;
use crate::{BytesSerializable, IggyByteSize, Sizeable, error::IggyError};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::{
    fmt::Display,
    hash::{Hash, Hasher},
};

/// `Partitioning` is used to specify to which partition the messages should be sent.
/// It has the following kinds:
/// - `Balanced` - the partition ID is calculated by the server using the round-robin algorithm.
/// - `PartitionId` - the partition ID is provided by the client.
/// - `MessagesKey` - the partition ID is calculated by the server using the hash of the provided messages key.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Partitioning {
    /// The kind of partitioning.
    pub kind: PartitioningKind,
    #[serde(skip)]
    /// The length of the value payload.
    pub length: u8,
    #[serde_as(as = "Base64")]
    /// The binary value payload.
    pub value: Vec<u8>,
}

impl Display for Partitioning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            PartitioningKind::Balanced => write!(f, "{}|0", self.kind),
            PartitioningKind::PartitionId => write!(
                f,
                "{}|{}",
                self.kind,
                u32::from_le_bytes(self.value[..4].try_into().unwrap())
            ),
            PartitioningKind::MessagesKey => {
                write!(f, "{}|{}", self.kind, String::from_utf8_lossy(&self.value))
            }
        }
    }
}

impl Partitioning {
    /// Partition the messages using the balanced round-robin algorithm on the server.
    pub fn balanced() -> Self {
        Partitioning {
            kind: PartitioningKind::Balanced,
            length: 0,
            value: vec![],
        }
    }

    /// Partition the messages using the provided partition ID.
    pub fn partition_id(partition_id: usize) -> Self {
        Partitioning {
            kind: PartitioningKind::PartitionId,
            length: 4,
            value: partition_id.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key.
    pub fn messages_key(value: &[u8]) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Partitioning {
            kind: PartitioningKind::MessagesKey,
            #[allow(clippy::cast_possible_truncation)]
            length: length as u8,
            value: value.to_vec(),
        })
    }

    /// Partition the messages using the provided messages key as str.
    pub fn messages_key_str(value: &str) -> Result<Self, IggyError> {
        Self::messages_key(value.as_bytes())
    }

    /// Partition the messages using the provided messages key as u32.
    pub fn messages_key_u32(value: u32) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 4,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key as u64.
    pub fn messages_key_u64(value: u64) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 8,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key as u128.
    pub fn messages_key_u128(value: u128) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 16,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Create the partitioning from the provided partitioning.
    pub fn from_partitioning(partitioning: &Partitioning) -> Self {
        Partitioning {
            kind: partitioning.kind,
            length: partitioning.length,
            value: partitioning.value.clone(),
        }
    }

    /// Create the partitioning from BytesMut.
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, IggyError> {
        let kind = PartitioningKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Partitioning {
            kind,
            length,
            value,
        })
    }

    /// Maximum size of the Partitioning struct
    pub const fn maximum_byte_size() -> usize {
        2 + 255
    }
}

impl Hash for Partitioning {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.length.hash(state);
        self.value.hash(state);
    }
}

impl Default for Partitioning {
    fn default() -> Self {
        Partitioning::balanced()
    }
}

impl Sizeable for Partitioning {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(u64::from(self.length) + 2)
    }
}

impl BytesSerializable for Partitioning {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.length as usize);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.put_slice(&self.value);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 2 {
            return Err(IggyError::InvalidCommand);
        }

        let kind = PartitioningKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Partitioning {
            kind,
            length,
            value,
        })
    }

    fn write_to_buffer(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.put_slice(&self.value);
    }

    fn get_buffer_size(&self) -> usize {
        2 + self.length as usize
    }
}
