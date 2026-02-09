// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;

use crate::stm::consumer_group::ConsumerGroupsSnapshot;
use crate::stm::stream::StreamsSnapshot;
use crate::stm::user::UsersSnapshot;

#[derive(Debug)]
pub enum SnapshotError {
    /// A required section is missing from the snapshot.
    MissingSection(&'static str),
    /// Serialization failed.
    Serialize(rmp_serde::encode::Error),
    /// Deserialization failed.
    Deserialize(rmp_serde::decode::Error),
    /// Slab ID mismatch during snapshot restore.
    SlabIdMismatch {
        section: &'static str,
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::MissingSection(name) => {
                write!(f, "missing snapshot section: {}", name)
            }
            SnapshotError::Serialize(e) => write!(f, "snapshot serialization failed: {}", e),
            SnapshotError::Deserialize(e) => write!(f, "snapshot deserialization failed: {}", e),
            SnapshotError::SlabIdMismatch {
                section,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "slab ID mismatch in section '{}': expected {}, got {}",
                    section, expected, actual
                )
            }
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SnapshotError::Serialize(e) => Some(e),
            SnapshotError::Deserialize(e) => Some(e),
            _ => None,
        }
    }
}

/// The snapshot container for all metadata state machines.
/// Each field corresponds to one state machine's serialized state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// Timestamp when the snapshot was created (microseconds since epoch).
    pub created_at: u64,
    /// Monotonically increasing snapshot sequence number.
    pub sequence_number: u64,
    /// Users state machine snapshot data.
    pub users: Option<UsersSnapshot>,
    /// Streams state machine snapshot data.
    pub streams: Option<StreamsSnapshot>,
    /// Consumer groups state machine snapshot data.
    pub consumer_groups: Option<ConsumerGroupsSnapshot>,
}

impl MetadataSnapshot {
    /// Create a new snapshot with the given sequence number.
    pub fn new(sequence_number: u64) -> Self {
        Self {
            created_at: iggy_common::IggyTimestamp::now().as_micros(),
            sequence_number,
            users: None,
            streams: None,
            consumer_groups: None,
        }
    }

    /// Encode the snapshot to msgpack bytes.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        rmp_serde::to_vec(self).map_err(SnapshotError::Serialize)
    }

    /// Decode a snapshot from msgpack bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        rmp_serde::from_slice(bytes).map_err(SnapshotError::Deserialize)
    }
}

/// Trait implemented by each `{Name}Inner` state machine to support snapshotting.
/// Each state machine defines its own snapshot
/// type for serialization and provides conversion methods.
pub trait Snapshotable {
    /// The serde-serializable snapshot representation of this state.
    /// This should be a plain struct with only serializable types and no wrappers
    /// like `Arc`, `AtomicUsize`, or other non-serializable wrappers.
    type Snapshot: Serialize + DeserializeOwned;

    /// Convert the current in-memory state into a serializable snapshot.
    fn to_snapshot(&self) -> Self::Snapshot;

    /// Restore in-memory state from a snapshot representation.
    fn from_snapshot(snapshot: Self::Snapshot) -> Result<Self, SnapshotError>
    where
        Self: Sized;
}

/// Trait for filling a typed snapshot with state machine data.
///
/// Each state machine implements this to write its serialized state
/// to its specific field in the `MetadataSnapshot` struct.
pub trait FillSnapshot {
    /// Fill the snapshot with this state machine's data.
    fn fill_snapshot(&self, snapshot: &mut MetadataSnapshot) -> Result<(), SnapshotError>;
}

/// Trait for restoring state machine data from a typed snapshot.
///
/// Each state machine implements this to read its state from
/// its specific field in the `MetadataSnapshot` struct.
pub trait RestoreSnapshot: Sized {
    /// Restore this state machine from the snapshot.
    fn restore_snapshot(snapshot: &MetadataSnapshot) -> Result<Self, SnapshotError>;
}

/// Base case for the recursive tuple pattern - unit type terminates the recursion.
impl FillSnapshot for () {
    fn fill_snapshot(&self, _snapshot: &mut MetadataSnapshot) -> Result<(), SnapshotError> {
        Ok(())
    }
}

impl RestoreSnapshot for () {
    fn restore_snapshot(_snapshot: &MetadataSnapshot) -> Result<Self, SnapshotError> {
        Ok(())
    }
}

/// Generates `FillSnapshot` and `RestoreSnapshot` implementations for a wrapper type.
///
/// The wrapper type (e.g. `Streams`) must implement `Snapshotable`.
///
/// # Example
///
/// ```ignore
/// impl_fill_restore!(Users, users);
/// ```
#[macro_export]
macro_rules! impl_fill_restore {
    ($wrapper:ident, $field:ident) => {
        impl $crate::stm::snapshot::FillSnapshot for $wrapper {
            fn fill_snapshot(
                &self,
                snapshot: &mut $crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<(), $crate::stm::snapshot::SnapshotError> {
                use $crate::stm::snapshot::Snapshotable;
                snapshot.$field = Some(self.to_snapshot());
                Ok(())
            }
        }

        impl $crate::stm::snapshot::RestoreSnapshot for $wrapper {
            fn restore_snapshot(
                snapshot: &$crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<Self, $crate::stm::snapshot::SnapshotError> {
                use $crate::stm::snapshot::{SnapshotError, Snapshotable};
                let snap = snapshot
                    .$field
                    .clone()
                    .ok_or(SnapshotError::MissingSection(stringify!($field)))?;
                Self::from_snapshot(snap)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::stream::{StatsSnapshot, StreamSnapshot};
    use iggy_common::IggyTimestamp;

    #[test]
    fn test_metadata_snapshot_roundtrip() {
        let snapshot = MetadataSnapshot::new(42);

        let encoded = snapshot.encode().unwrap();
        let decoded = MetadataSnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence_number, 42);
        assert!(decoded.users.is_none());
        assert!(decoded.streams.is_none());
        assert!(decoded.consumer_groups.is_none());
    }

    #[test]
    fn roundtrip_with_data() {
        let ts = IggyTimestamp::from(1694968446131680u64);

        let mut snapshot = MetadataSnapshot::new(100);
        snapshot.streams = Some(StreamsSnapshot {
            items: vec![(
                0,
                StreamSnapshot {
                    id: 0,
                    name: "events".to_string(),
                    created_at: ts,
                    stats: StatsSnapshot {
                        size_bytes: 1024,
                        messages_count: 50,
                        segments_count: 2,
                    },
                    topics: vec![],
                },
            )],
        });

        let encoded = snapshot.encode().unwrap();
        let decoded = MetadataSnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence_number, 100);
        assert!(decoded.users.is_none());
        assert!(decoded.consumer_groups.is_none());

        let streams = decoded.streams.as_ref().unwrap();
        assert_eq!(streams.items.len(), 1);

        let (slab_id, stream) = &streams.items[0];
        assert_eq!(*slab_id, 0);
        assert_eq!(stream.name, "events");
        assert_eq!(stream.created_at.as_micros(), ts.as_micros());
        assert_eq!(stream.stats.size_bytes, 1024);
        assert_eq!(stream.stats.messages_count, 50);
        assert_eq!(stream.stats.segments_count, 2);
        assert_eq!(stream.topics.len(), 0);
    }
}
