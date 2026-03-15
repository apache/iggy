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
    /// Serialization failed.
    Serialize(rmp_serde::encode::Error),
    /// Deserialization failed.
    Deserialize(rmp_serde::decode::Error),
    /// I/O error during snapshot persist/load.
    Io(std::io::Error),
    /// I/O error during a specific stage of snapshot persistence.
    /// The caller can inspect the stage to decide whether to retry
    /// (e.g. `Rename`) or start from scratch (e.g. `Write`, `Sync`).
    Persist {
        stage: PersistStage,
        source: std::io::Error,
    },
    /// Checksum mismatch on snapshot load.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Snapshot file is too short to contain a valid checksum.
    Truncated { size: u64 },
}

/// Stage at which snapshot persistence failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistStage {
    /// Creating or writing the temp file failed. The temp file may contain
    /// partial data. Safe to delete and retry from scratch.
    Write,
    /// Fsync of the temp file failed. The data may not be durable.
    /// Safe to delete the temp file and retry from scratch.
    Sync,
    /// Atomic rename of temp -> final path failed. The temp file contains
    /// a complete, synced snapshot. Safe to retry just the rename.
    Rename,
    /// Fsync of the parent directory after rename failed. The rename
    /// succeeded but may not be durable. Safe to retry just the dir sync.
    DirSync,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize(e) => write!(f, "snapshot serialization failed: {e}"),
            Self::Deserialize(e) => write!(f, "snapshot deserialization failed: {e}"),
            Self::Io(e) => write!(f, "snapshot I/O error: {e}"),
            Self::Persist { stage, source } => {
                write!(f, "snapshot persist failed at {stage:?} stage: {source}")
            }
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "snapshot checksum mismatch: expected {expected:#010x}, actual {actual:#010x}"
                )
            }
            Self::Truncated { size } => {
                write!(
                    f,
                    "snapshot file truncated: {size} bytes (too short for checksum)"
                )
            }
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize(e) => Some(e),
            Self::Deserialize(e) => Some(e),
            Self::Io(e) | Self::Persist { source: e, .. } => Some(e),
            Self::ChecksumMismatch { .. } | Self::Truncated { .. } => None,
        }
    }
}

impl From<std::io::Error> for SnapshotError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// The snapshot container for all metadata state machines.
/// Each field corresponds to one state machine's serialized state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// Snapshot format version for forward/backward compatibility.
    /// TODO(krishvishal): Properly handle versioning for snapshot. This is a placeholder for now.
    pub version: u32,
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

impl Default for MetadataSnapshot {
    fn default() -> Self {
        Self::new(0)
    }
}

impl MetadataSnapshot {
    /// Create a new snapshot with the given sequence number.
    #[must_use]
    pub fn new(sequence_number: u64) -> Self {
        Self {
            version: 1,
            created_at: iggy_common::IggyTimestamp::now().as_micros(),
            sequence_number,
            users: None,
            streams: None,
            consumer_groups: None,
        }
    }

    /// Encode the snapshot to msgpack bytes.
    ///
    /// # Errors
    /// Returns `SnapshotError::Serialize` if msgpack serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        rmp_serde::to_vec(self).map_err(SnapshotError::Serialize)
    }

    /// Decode a snapshot from msgpack bytes.
    ///
    /// # Errors
    /// Returns `SnapshotError::Deserialize` if msgpack deserialization fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        rmp_serde::from_slice(bytes).map_err(SnapshotError::Deserialize)
    }
}

/// Trait for metadata snapshot implementations.
///
/// This is the high-level interface that concrete snapshot types (e.g. `IggySnapshot`)
/// must satisfy. It provides methods for creating, encoding, and decoding snapshots.
#[allow(clippy::missing_errors_doc)]
pub trait Snapshot: Sized {
    /// The error type for snapshot operations.
    type Error: std::error::Error;

    /// The type used for snapshot sequence numbers.
    type SequenceNumber;

    /// The type used for snapshot timestamps.
    type Timestamp;

    /// The inner snapshot data structure that state machines fill and restore from.
    type Inner;

    /// Create a snapshot from the current state of a state machine.
    ///
    /// # Arguments
    /// * `stm` - The state machine to snapshot
    /// * `sequence_number` - Monotonically increasing snapshot sequence number
    fn create<T>(stm: &T, sequence_number: Self::SequenceNumber) -> Result<Self, Self::Error>
    where
        T: FillSnapshot<Self::Inner>;

    /// Encode the snapshot to msgpack bytes.
    fn encode(&self) -> Result<Vec<u8>, Self::Error>;

    /// Decode a snapshot from msgpack bytes.
    fn decode(bytes: &[u8]) -> Result<Self, Self::Error>;

    /// Get the snapshot sequence number.
    fn sequence_number(&self) -> Self::SequenceNumber;

    /// Get the timestamp when this snapshot was created.
    fn created_at(&self) -> Self::Timestamp;
}

/// Trait implemented by each `{Name}Inner` state machine to support snapshotting.
/// Each state machine defines its own snapshot
/// type for serialization and provides conversion methods.
#[allow(clippy::missing_errors_doc)]
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
/// Each state machine implements this to write its serialized state.
#[allow(clippy::missing_errors_doc)]
pub trait FillSnapshot<S> {
    /// Fill the snapshot with this state machine's data.
    fn fill_snapshot(&self, snapshot: &mut S) -> Result<(), SnapshotError>;
}

/// Trait for restoring state machine data from a typed snapshot.
///
/// Each state machine implements this to read its state.
#[allow(clippy::missing_errors_doc)]
pub trait RestoreSnapshot<S>: Sized {
    /// Restore this state machine from the snapshot.
    fn restore_snapshot(snapshot: &S) -> Result<Self, SnapshotError>;
}

/// Base case for the recursive tuple pattern - unit type terminates the recursion.
impl<S> FillSnapshot<S> for () {
    fn fill_snapshot(&self, _snapshot: &mut S) -> Result<(), SnapshotError> {
        Ok(())
    }
}

impl<S> RestoreSnapshot<S> for () {
    fn restore_snapshot(_snapshot: &S) -> Result<Self, SnapshotError> {
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
        impl $crate::stm::snapshot::FillSnapshot<$crate::stm::snapshot::MetadataSnapshot>
            for $wrapper
        {
            fn fill_snapshot(
                &self,
                snapshot: &mut $crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<(), $crate::stm::snapshot::SnapshotError> {
                use $crate::stm::snapshot::Snapshotable;
                snapshot.$field = Some(self.to_snapshot());
                Ok(())
            }
        }

        impl $crate::stm::snapshot::RestoreSnapshot<$crate::stm::snapshot::MetadataSnapshot>
            for $wrapper
        {
            fn restore_snapshot(
                snapshot: &$crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<Self, $crate::stm::snapshot::SnapshotError> {
                use serde::de::Error as _;
                use $crate::stm::snapshot::{SnapshotError, Snapshotable};
                let snap = snapshot.$field.clone().ok_or_else(|| {
                    SnapshotError::Deserialize(rmp_serde::decode::Error::custom(format_args!(
                        "Snapshot Restore Error: {}",
                        stringify!($field)
                    )))
                })?;
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
        let ts = IggyTimestamp::from(1_694_968_446_131_680_u64);

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

    #[test]
    fn roundtrip_with_slab_gaps() {
        use crate::stm::stream::StreamsSnapshot;
        use crate::stm::user::{PermissionerSnapshot, UserSnapshot, UsersSnapshot};
        use iggy_common::UserStatus;

        let ts = IggyTimestamp::from(1_694_968_446_131_680_u64);

        let users_snap = UsersSnapshot {
            items: vec![
                (
                    0,
                    UserSnapshot {
                        id: 0,
                        username: "alice".to_string(),
                        password_hash: "hash_a".to_string(),
                        status: UserStatus::Active,
                        created_at: ts,
                        permissions: None,
                    },
                ),
                (
                    2,
                    UserSnapshot {
                        id: 2,
                        username: "charlie".to_string(),
                        password_hash: "hash_c".to_string(),
                        status: UserStatus::Active,
                        created_at: ts,
                        permissions: None,
                    },
                ),
            ],
            personal_access_tokens: vec![],
            permissioner: PermissionerSnapshot {
                users_permissions: vec![],
                users_streams_permissions: vec![],
                users_that_can_poll_messages_from_all_streams: vec![],
                users_that_can_send_messages_to_all_streams: vec![],
                users_that_can_poll_messages_from_specific_streams: vec![],
                users_that_can_send_messages_to_specific_streams: vec![],
            },
        };

        let streams_snap = StreamsSnapshot {
            items: vec![
                (
                    0,
                    StreamSnapshot {
                        id: 0,
                        name: "stream-0".to_string(),
                        created_at: ts,
                        stats: StatsSnapshot {
                            size_bytes: 100,
                            messages_count: 10,
                            segments_count: 1,
                        },
                        topics: vec![],
                    },
                ),
                (
                    3,
                    StreamSnapshot {
                        id: 3,
                        name: "stream-3".to_string(),
                        created_at: ts,
                        stats: StatsSnapshot {
                            size_bytes: 200,
                            messages_count: 20,
                            segments_count: 2,
                        },
                        topics: vec![],
                    },
                ),
            ],
        };

        let mut snapshot = MetadataSnapshot::new(99);
        snapshot.users = Some(users_snap);
        snapshot.streams = Some(streams_snap);

        let encoded = snapshot.encode().unwrap();
        let decoded = MetadataSnapshot::decode(&encoded).unwrap();

        let restored_users: crate::stm::user::Users =
            RestoreSnapshot::restore_snapshot(&decoded).unwrap();

        let mut verify = MetadataSnapshot::new(0);
        restored_users.fill_snapshot(&mut verify).unwrap();
        let users_snap = verify.users.unwrap();
        assert_eq!(users_snap.items.len(), 2);
        assert_eq!(users_snap.items[0].0, 0);
        assert_eq!(users_snap.items[0].1.username, "alice");
        assert_eq!(users_snap.items[0].1.id, 0);
        assert_eq!(users_snap.items[1].0, 2);
        assert_eq!(users_snap.items[1].1.username, "charlie");
        assert_eq!(users_snap.items[1].1.id, 2);

        let restored_streams: crate::stm::stream::Streams =
            RestoreSnapshot::restore_snapshot(&decoded).unwrap();

        let mut verify = MetadataSnapshot::new(0);
        restored_streams.fill_snapshot(&mut verify).unwrap();
        let streams_snap = verify.streams.unwrap();
        assert_eq!(streams_snap.items.len(), 2);
        assert_eq!(streams_snap.items[0].0, 0);
        assert_eq!(streams_snap.items[0].1.name, "stream-0");
        assert_eq!(streams_snap.items[1].0, 3);
        assert_eq!(streams_snap.items[1].1.name, "stream-3");
    }
}
