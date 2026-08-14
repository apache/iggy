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

use iggy_binary_protocol::IGGY_PROTOCOL_VERSION;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;

use crate::stm::stream::StreamsSnapshot;
use crate::stm::user::UsersSnapshot;

/// The version of the snapshot format in use, reserved for breaking changes.
///
/// One version means exactly one serialized shape, and [`MetadataSnapshot::decode`]
/// accepts nothing else. Bump it in the same change that alters the shape: append,
/// remove, reorder, retype, or redefine the meaning of any field, at any depth
/// under [`MetadataSnapshot`]. There is no accepted range and no per-version
/// translation. A snapshot this build cannot read is refused, not best-effort
/// decoded.
///
/// Nothing softer would hold. msgpack encodes a struct positionally, so a field one
/// build appends reaches another as an unexplained extra array element; without the
/// version the reader either fails on an unrelated msgpack error or, for a
/// same-length change, silently reads one field's bytes as another's.
///
/// Bumping it invalidates every `snapshot.bin` already on disk, and a node whose
/// snapshot is refused refuses boot. That is deliberate. The metadata plane is
/// pre-production, so the cost is clearing a data directory; once it ships, a bump
/// needs an explicit translation path added here alongside it.
///
/// Version 2: `status` sits at reply-header offset 216 (version 1 carried a
/// `namespace` word before it), which the client table's cached replies embed as raw
/// wire bytes msgpack cannot introspect.
///
/// Version 3: `TopicSnapshot` dropped `replication_factor` from the middle of the
/// struct and gained `options`. Both changes land in the same element count, so a
/// version 2 file decoded under this shape does not fail cleanly: msgpack reads the
/// old `replication_factor` byte as `message_expiry` and walks every later field one
/// position out of place. The bump is what turns that into
/// `UnsupportedFormatVersion` instead of an opaque deserializer error, or worse a
/// silent misread.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 3;

/// The release that wrote a snapshot: the packed `iggy_binary_protocol` semver of
/// this build. [`iggy_binary_protocol::ProtocolVersion`] documents the packing and
/// renders it as `major.minor.patch`.
///
/// Provenance, never a gate. The format version says whether the bytes are
/// readable; this says which build produced them, which matters most for a snapshot
/// that arrived over state transfer from a machine whose build this node otherwise
/// has no record of.
///
/// A build constant, identical on every replica, so two replicas holding identical
/// state still serialize identically (see [`MetadataSnapshot`]).
pub const SNAPSHOT_WRITER_RELEASE: u32 = IGGY_PROTOCOL_VERSION;

const _: () = assert!(SNAPSHOT_WRITER_RELEASE > 0);

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
    /// The snapshot's integrity trailer does not match its payload: a torn or
    /// bit-rotted checkpoint. Refuse to restore from it rather than feed corrupt state
    /// into the state machine.
    ChecksumMismatch { expected: u128, actual: u128 },
    /// Snapshot file is too short to contain a valid checksum.
    Truncated { size: u64 },
    /// The snapshot was written in a format version this build does not read: a
    /// different build wrote it, on this disk or on a state-transfer peer. Refuse it
    /// rather than let msgpack read one field's bytes as another's. The version is
    /// peeked ahead of the rest of the payload, so this fires even when nothing past
    /// it is recognizable.
    UnsupportedFormatVersion { found: u32, expected: u32 },
    /// A state-transfer descriptor's frontiers contradict its artifacts: a
    /// `commit_op` below the snapshot the same offer ships, or a client-table
    /// frontier above that commit point. Both are impossible from a
    /// caught-up-primary offer, so the install is refused and the receiver falls
    /// back to journal repair rather than moving its frontiers off a bad
    /// manifest.
    IncoherentManifest {
        snapshot_seq: u64,
        commit_op: u64,
        table_frontier: u64,
    },
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
                    "snapshot checksum mismatch: expected {expected:#034x}, actual {actual:#034x}"
                )
            }
            Self::Truncated { size } => {
                write!(
                    f,
                    "snapshot file truncated: {size} bytes (too short for checksum)"
                )
            }
            Self::UnsupportedFormatVersion { found, expected } => {
                write!(
                    f,
                    "snapshot format version {found} is incompatible with this build, which \
                     reads version {expected}"
                )
            }
            Self::IncoherentManifest {
                snapshot_seq,
                commit_op,
                table_frontier,
            } => {
                write!(
                    f,
                    "incoherent state transfer manifest: snapshot at op {snapshot_seq}, \
                     commit_op {commit_op}, table frontier {table_frontier}"
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
            Self::ChecksumMismatch { .. }
            | Self::Truncated { .. }
            | Self::UnsupportedFormatVersion { .. }
            | Self::IncoherentManifest { .. } => None,
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
///
/// Serialized-form invariant: every collection here and in the nested `*Snapshot`
/// types must keep a deterministic order (`Vec` or `BTreeMap`, never an unordered
/// `HashMap`/`HashSet`/`AHashMap`). The checkpoint pairing hashes bytes on both sides
/// (`impls::metadata::checkpoint_checksum`), so it no longer depends on this, but
/// cross-replica state comparison and any future content-addressed transfer do: two
/// replicas with identical state must serialize identically. Regression guards:
/// `stream::tests::populated_streams_snapshot_reencode_is_byte_stable` and
/// `impls::metadata::tests::populated_snapshot_reencode_and_checksum_are_stable`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// The version of the snapshot format in use, reserved for breaking changes.
    /// See [`SNAPSHOT_FORMAT_VERSION`]; [`Self::decode`] refuses anything else.
    ///
    /// First field deliberately: msgpack encodes the struct positionally, so this is
    /// the one element [`peek_format_version`] can pull out before it knows the rest
    /// of the layout.
    pub version: u32,
    /// Timestamp when the snapshot was created (microseconds since epoch).
    pub created_at: u64,
    /// Monotonically increasing snapshot sequence number.
    pub sequence_number: u64,
    /// Users state machine snapshot data.
    pub users: Option<UsersSnapshot>,
    /// Streams state machine snapshot data (consumer groups are nested inside
    /// each topic).
    pub streams: Option<StreamsSnapshot>,
    /// Client-table state (sessions + at-most-once dedup replies) at the checkpoint
    /// op. Not a state machine, but folded in so a restart that drained the WAL
    /// prefix still recovers pre-checkpoint sessions.
    pub client_table: Option<consensus::ClientTableSnapshot>,
    /// The release that wrote this snapshot, as a packed `iggy_binary_protocol`
    /// semver. Provenance only, never a gate. See [`SNAPSHOT_WRITER_RELEASE`].
    pub writer_release: u32,
}

impl Default for MetadataSnapshot {
    fn default() -> Self {
        Self::new(0)
    }
}

impl MetadataSnapshot {
    /// Create a new snapshot with the given sequence number.
    #[must_use]
    pub const fn new(sequence_number: u64) -> Self {
        Self {
            version: SNAPSHOT_FORMAT_VERSION,
            // Deterministic placeholder. The real creation time is stamped by
            // `Snapshot::create` from the consensus-injected clock (see
            // `VsrConsensus::clock_realtime_micros`) so a replayed simulator
            // seed reproduces identical snapshot bytes; reading the wall clock
            // here would reintroduce a nondeterministic input.
            created_at: 0,
            sequence_number,
            users: None,
            streams: None,
            client_table: None,
            writer_release: SNAPSHOT_WRITER_RELEASE,
        }
    }

    /// Encode the snapshot to msgpack bytes.
    ///
    /// # Errors
    /// Returns `SnapshotError::Serialize` if msgpack serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        rmp_serde::to_vec(self).map_err(SnapshotError::Serialize)
    }

    /// Decode a snapshot from msgpack bytes, refusing a format version this build
    /// does not read.
    ///
    /// The version is checked before the payload is deserialized, so a snapshot
    /// whose layout past that field is unknown is refused by version rather than by
    /// whichever later field happened to misparse.
    ///
    /// # Errors
    /// [`SnapshotError::UnsupportedFormatVersion`] when the stamped version is not
    /// [`SNAPSHOT_FORMAT_VERSION`], or [`SnapshotError::Deserialize`] if msgpack
    /// deserialization fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        // Bytes carrying no readable version are not a snapshot at all, so they fall
        // through to the deserializer, whose error names what actually went wrong.
        if let Some(version) = peek_format_version(bytes)
            && version != SNAPSHOT_FORMAT_VERSION
        {
            return Err(SnapshotError::UnsupportedFormatVersion {
                found: version,
                expected: SNAPSHOT_FORMAT_VERSION,
            });
        }
        rmp_serde::from_slice(bytes).map_err(SnapshotError::Deserialize)
    }
}

/// The format version stamped in encoded snapshot bytes, read without decoding the
/// rest of them.
///
/// `None` for anything that is not a msgpack array whose first element is an
/// unsigned integer, which covers every input that is not a snapshot.
///
/// Reading the version on its own is what keeps the check working across a layout
/// change: deserializing the whole struct first would let some later field fail and
/// report a msgpack error naming no version, which is the difference between "this
/// file came from a newer build" and "this file is corrupt". `journal::superblock`
/// reads its own version field the same way, ahead of the length and checksum it
/// cannot yet trust.
#[must_use]
pub fn peek_format_version(encoded: &[u8]) -> Option<u32> {
    let mut cursor = encoded;
    rmp::decode::read_array_len(&mut cursor).ok()?;
    rmp::decode::read_int(&mut cursor).ok()
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
    /// * `created_at` - Creation time from the injected clock; must be
    ///   deterministic under simulation, never a wall-clock read
    fn create<T>(
        stm: &T,
        sequence_number: Self::SequenceNumber,
        created_at: Self::Timestamp,
    ) -> Result<Self, Self::Error>
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

/// Restore a LIVE state machine from a snapshot without reconstructing it.
///
/// [`RestoreSnapshot`] builds a fresh instance -- boot-time only, because a
/// running system shares read handles (left-right factories) across shards
/// that a swap would orphan. This variant replaces the state THROUGH the
/// existing write handle (an absorbed `RestoreSnapshot` command), so every
/// reader observes the restored state on its next read. State transfer
/// installs with this.
#[allow(clippy::missing_errors_doc)]
pub trait RestoreSnapshotInPlace<S> {
    /// Replace this state machine's contents from the snapshot.
    fn restore_snapshot_in_place(&self, snapshot: &S) -> Result<(), SnapshotError>;

    /// Whether this state machine can restore from `snapshot`, WITHOUT
    /// mutating anything.
    ///
    /// A mux restores its halves one at a time, so a snapshot missing the
    /// second half would otherwise leave the first restored and the second on
    /// pre-transfer state -- a split the caller cannot undo, because the
    /// transferred snapshot was already persisted and its pairing seeded.
    /// Every half agrees here before any half mutates.
    fn check_restorable(&self, snapshot: &S) -> Result<(), SnapshotError>;
}

/// Base case for the recursive tuple pattern - unit type terminates the recursion.
impl<S> RestoreSnapshotInPlace<S> for () {
    fn restore_snapshot_in_place(&self, _snapshot: &S) -> Result<(), SnapshotError> {
        Ok(())
    }

    fn check_restorable(&self, _snapshot: &S) -> Result<(), SnapshotError> {
        Ok(())
    }
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

        impl $crate::stm::snapshot::RestoreSnapshotInPlace<$crate::stm::snapshot::MetadataSnapshot>
            for $wrapper
        {
            fn restore_snapshot_in_place(
                &self,
                snapshot: &$crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<(), $crate::stm::snapshot::SnapshotError> {
                use serde::de::Error as _;
                use $crate::stm::snapshot::SnapshotError;
                paste::paste! {
                    let snap = snapshot.$field.clone().ok_or_else(|| {
                        SnapshotError::Deserialize(rmp_serde::decode::Error::custom(
                            format_args!("Snapshot Restore Error: {}", stringify!($field)),
                        ))
                    })?;
                    self.inner
                        .try_apply([<$wrapper Command>]::RestoreSnapshot(snap))
                        .map_err(|err| {
                            SnapshotError::Io(std::io::Error::other(format!(
                                "in-place restore on a reader-only state machine: {err}"
                            )))
                        })
                }
            }

            fn check_restorable(
                &self,
                snapshot: &$crate::stm::snapshot::MetadataSnapshot,
            ) -> Result<(), $crate::stm::snapshot::SnapshotError> {
                use serde::de::Error as _;
                use $crate::stm::snapshot::SnapshotError;
                if snapshot.$field.is_none() {
                    return Err(SnapshotError::Deserialize(
                        rmp_serde::decode::Error::custom(format_args!(
                            "Snapshot Restore Error: {}",
                            stringify!($field)
                        )),
                    ));
                }
                Ok(())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::stream::{PartitionSnapshot, StatsSnapshot, StreamSnapshot, TopicSnapshot};
    use crate::stm::user::UserSnapshot;
    use iggy_common::{
        CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize, ResourceOptions,
    };

    #[test]
    fn test_metadata_snapshot_roundtrip() {
        let snapshot = MetadataSnapshot::new(42);

        let encoded = snapshot.encode().unwrap();
        let decoded = MetadataSnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence_number, 42);
        assert_eq!(decoded.version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(decoded.writer_release, SNAPSHOT_WRITER_RELEASE);
        assert!(decoded.users.is_none());
        assert!(decoded.streams.is_none());
        assert!(decoded.client_table.is_none());
    }

    #[test]
    fn encoded_shape_is_pinned_to_the_format_version() {
        // The gate is only as good as the discipline that bumps it, and nothing else
        // fails when a field is appended and the bump is forgotten. msgpack encodes
        // positionally, so the element count is the shape's fingerprint: pinning it
        // turns "shape changed, version did not" into a failing test rather than an
        // operator's boot reading one field's bytes as another's. Changing either
        // number is the reminder to change the other.
        const FIELD_COUNT: u32 = 7;
        const PINNED_VERSION: u32 = 3;

        let encoded = MetadataSnapshot::new(0).encode().unwrap();
        let mut cursor = encoded.as_slice();
        assert_eq!(
            rmp::decode::read_array_len(&mut cursor).unwrap(),
            FIELD_COUNT,
            "MetadataSnapshot's field count changed; bump SNAPSHOT_FORMAT_VERSION with it"
        );
        assert_eq!(
            SNAPSHOT_FORMAT_VERSION, PINNED_VERSION,
            "SNAPSHOT_FORMAT_VERSION moved; confirm the shape moved with it"
        );
    }

    #[test]
    fn nested_snapshot_shapes_are_pinned_too() {
        // The top-level count above is blind to everything under it, which is how
        // `TopicSnapshot` once swapped a removed field for an added one and kept
        // the same element count: a version 2 file then decoded field-by-field
        // one position out of place instead of failing. The types the STM
        // actually grows need their own pins.
        const TOPIC_FIELD_COUNT: u32 = 11;
        const STREAM_FIELD_COUNT: u32 = 6;

        let topic = TopicSnapshot {
            id: 0,
            name: String::new(),
            created_at: IggyTimestamp::default(),
            message_expiry: IggyExpiry::default(),
            compression_algorithm: CompressionAlgorithm::default(),
            max_topic_size: MaxTopicSize::default(),
            stats: StatsSnapshot {
                size_bytes: 0,
                messages_count: 0,
                segments_count: 0,
            },
            partitions: Vec::new(),
            consumer_groups: Vec::new(),
            next_consumer_group_id: 0,
            options: ResourceOptions::new(),
        };
        let encoded = rmp_serde::to_vec(&topic).unwrap();
        assert_eq!(
            rmp::decode::read_array_len(&mut encoded.as_slice()).unwrap(),
            TOPIC_FIELD_COUNT,
            "TopicSnapshot's field count changed; bump SNAPSHOT_FORMAT_VERSION with it"
        );

        let stream = StreamSnapshot {
            id: 0,
            name: String::new(),
            created_at: IggyTimestamp::default(),
            stats: StatsSnapshot {
                size_bytes: 0,
                messages_count: 0,
                segments_count: 0,
            },
            topics: Vec::new(),
            options: ResourceOptions::new(),
        };
        let encoded = rmp_serde::to_vec(&stream).unwrap();
        assert_eq!(
            rmp::decode::read_array_len(&mut encoded.as_slice()).unwrap(),
            STREAM_FIELD_COUNT,
            "StreamSnapshot's field count changed; bump SNAPSHOT_FORMAT_VERSION with it"
        );
    }

    #[test]
    fn a_build_decodes_what_it_writes() {
        // Exact-equality versioning makes this the one thing that could go wrong
        // silently: a stamp the writer picks and the reader rejects would refuse every
        // node its own snapshot on the next boot.
        let encoded = MetadataSnapshot::new(5).encode().unwrap();
        MetadataSnapshot::decode(&encoded).expect("a build must read its own snapshot");
    }

    #[test]
    fn release_stamp_is_a_build_constant() {
        // Two replicas holding identical state must serialize identically, which a
        // per-node or per-write release stamp would break.
        assert_eq!(
            MetadataSnapshot::new(1).encode().unwrap(),
            MetadataSnapshot::new(1).encode().unwrap()
        );
    }

    #[test]
    fn encoded_snapshot_leads_with_the_version_element() {
        // `peek_format_version` rests on this shape: a msgpack array (`rmp_serde`'s
        // compact struct encoding) whose first element is `version`. Switching to
        // `to_vec_named` would encode a map instead and silently blind the peek, so
        // pin it here.
        let encoded = MetadataSnapshot::new(3).encode().unwrap();
        assert_eq!(
            encoded[0] & 0xf0,
            0x90,
            "snapshot must encode as a msgpack fixarray"
        );
        assert_eq!(
            peek_format_version(&encoded),
            Some(SNAPSHOT_FORMAT_VERSION),
            "the first array element must be the format version"
        );
    }

    #[test]
    fn peek_format_version_ignores_bytes_that_are_not_a_snapshot() {
        assert_eq!(peek_format_version(&[]), None);
        // A msgpack string, not an array.
        assert_eq!(peek_format_version(&[0xa1, b'x']), None);
        // An array whose first element is not an unsigned integer.
        assert_eq!(peek_format_version(&[0x91, 0xc0]), None);
    }

    #[test]
    fn decode_refuses_any_other_format_version() {
        // Newer and older alike: there is no accepted window, so both directions are
        // the same refusal. Version 0 is what a zeroed or absent stamp reads as.
        for forged in [SNAPSHOT_FORMAT_VERSION + 1, 0] {
            let mut snapshot = MetadataSnapshot::new(9);
            snapshot.version = forged;
            let encoded = snapshot.encode().unwrap();

            match MetadataSnapshot::decode(&encoded) {
                Err(SnapshotError::UnsupportedFormatVersion { found, expected }) => {
                    assert_eq!(found, forged);
                    assert_eq!(expected, SNAPSHOT_FORMAT_VERSION);
                }
                other => panic!("expected an unsupported-version refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn decode_refuses_a_shape_change_by_version_not_by_msgpack() {
        // The reason the version is peeked instead of read off the decoded struct: a
        // future build's layout need not decode at all under this one, and the refusal
        // must still name the version rather than whichever field happened to
        // misparse. Emulated by appending an element, which is what a field append
        // looks like on the wire.
        let mut snapshot = MetadataSnapshot::new(4);
        snapshot.version = SNAPSHOT_FORMAT_VERSION + 1;
        let current = snapshot.encode().unwrap();
        assert_eq!(current[0] & 0xf0, 0x90, "expected a msgpack fixarray");
        let mut future = vec![0x90 | ((current[0] & 0x0f) + 1)];
        future.extend_from_slice(&current[1..]);
        future.push(0xc0);

        match MetadataSnapshot::decode(&future) {
            Err(SnapshotError::UnsupportedFormatVersion { found, .. }) => {
                assert_eq!(found, SNAPSHOT_FORMAT_VERSION + 1);
            }
            other => panic!("expected an unsupported-version refusal, got {other:?}"),
        }
    }

    #[test]
    fn decode_reports_deserialization_when_no_version_is_readable() {
        // Bytes carrying no version field are corruption, not a version skew, and the
        // msgpack error names that far better than a fabricated version would.
        match MetadataSnapshot::decode(&[0xa3, b'n', b'o', b'!']) {
            Err(SnapshotError::Deserialize(_)) => {}
            other => panic!("expected a deserialization error, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_with_data() {
        let ts = IggyTimestamp::from(1_694_968_446_131_680_u64);

        let mut snapshot = MetadataSnapshot::new(100);
        snapshot.streams = Some(StreamsSnapshot {
            revision: 0,
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
                    topics: vec![(
                        0,
                        TopicSnapshot {
                            id: 0,
                            name: "topic".to_string(),
                            created_at: ts,
                            message_expiry: IggyExpiry::default(),
                            compression_algorithm: CompressionAlgorithm::default(),
                            max_topic_size: MaxTopicSize::default(),
                            stats: StatsSnapshot {
                                size_bytes: 256,
                                messages_count: 12,
                                segments_count: 1,
                            },
                            partitions: vec![PartitionSnapshot {
                                id: 0,
                                consensus_group_id: 33,
                                created_at: ts,
                                created_revision: 0,
                                deleted_up_to_offset: 0,
                                purge_generation: 0,
                            }],
                            consumer_groups: Vec::new(),
                            // Nonzero and distinct from every id above so the
                            // roundtrip assert below proves the field survives
                            // instead of matching a default.
                            next_consumer_group_id: 5,
                            options: ResourceOptions::default(),
                        },
                    )],
                    options: ResourceOptions::default(),
                },
            )],
        });

        let encoded = snapshot.encode().unwrap();
        let decoded = MetadataSnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence_number, 100);
        assert!(decoded.users.is_none());

        let streams = decoded.streams.as_ref().unwrap();
        assert_eq!(streams.items.len(), 1);

        let (slab_id, stream) = &streams.items[0];
        assert_eq!(*slab_id, 0);
        assert_eq!(stream.name, "events");
        assert_eq!(stream.created_at.as_micros(), ts.as_micros());
        assert_eq!(stream.stats.size_bytes, 1024);
        assert_eq!(stream.stats.messages_count, 50);
        assert_eq!(stream.stats.segments_count, 2);
        assert_eq!(stream.topics.len(), 1);
        let (_, topic) = &stream.topics[0];
        assert_eq!(topic.partitions.len(), 1);
        assert_eq!(topic.partitions[0].consensus_group_id, 33);
        // Never-reuse counter: `#[serde(default)]` would silently restore 0 if
        // the field were dropped from the wire format, so pin its survival.
        assert_eq!(topic.next_consumer_group_id, 5);
    }

    fn user_snapshot_fixture(id: u32, username: &str, password_hash: &str) -> UserSnapshot {
        use iggy_common::UserStatus;
        UserSnapshot {
            id,
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            status: UserStatus::Active,
            created_at: IggyTimestamp::from(1_694_968_446_131_680_u64),
            permissions: None,
            options: ResourceOptions::default(),
        }
    }

    fn stream_snapshot_fixture(id: usize, name: &str, stats: StatsSnapshot) -> StreamSnapshot {
        StreamSnapshot {
            id,
            name: name.to_string(),
            created_at: IggyTimestamp::from(1_694_968_446_131_680_u64),
            stats,
            topics: vec![],
            options: ResourceOptions::default(),
        }
    }

    #[test]
    fn roundtrip_with_slab_gaps() {
        use crate::stm::stream::StreamsSnapshot;
        use crate::stm::user::{PermissionerSnapshot, UsersSnapshot};

        let users_snap = UsersSnapshot {
            items: vec![
                (0, user_snapshot_fixture(0, "alice", "hash_a")),
                (2, user_snapshot_fixture(2, "charlie", "hash_c")),
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
            revision: 0,
            items: vec![
                (
                    0,
                    stream_snapshot_fixture(
                        0,
                        "stream-0",
                        StatsSnapshot {
                            size_bytes: 100,
                            messages_count: 10,
                            segments_count: 1,
                        },
                    ),
                ),
                (
                    3,
                    stream_snapshot_fixture(
                        3,
                        "stream-3",
                        StatsSnapshot {
                            size_bytes: 200,
                            messages_count: 20,
                            segments_count: 2,
                        },
                    ),
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
