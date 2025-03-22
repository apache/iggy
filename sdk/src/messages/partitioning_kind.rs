use crate::{
    error::IggyError,
    prelude::{BytesSerializable, IggyByteSize, Sizeable},
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::{
    fmt::Display,
    hash::{Hash, Hasher},
};

/// `PartitioningKind` is an enum which specifies the kind of partitioning and is used by `Partitioning`.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningKind {
    /// The partition ID is calculated by the server using the round-robin algorithm.
    #[default]
    Balanced,
    /// The partition ID is provided by the client.
    PartitionId,
    /// The partition ID is calculated by the server using the hash of the provided messages key.
    MessagesKey,
}

impl Hash for PartitioningKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_code().hash(state);
    }
}

impl PartitioningKind {
    /// Get the code of the partitioning kind.
    pub fn as_code(&self) -> u8 {
        match self {
            PartitioningKind::Balanced => 1,
            PartitioningKind::PartitionId => 2,
            PartitioningKind::MessagesKey => 3,
        }
    }

    /// Get the partitioning kind from the provided code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(PartitioningKind::Balanced),
            2 => Ok(PartitioningKind::PartitionId),
            3 => Ok(PartitioningKind::MessagesKey),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for PartitioningKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitioningKind::Balanced => write!(f, "balanced"),
            PartitioningKind::PartitionId => write!(f, "partition_id"),
            PartitioningKind::MessagesKey => write!(f, "messages_key"),
        }
    }
}