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

use crate::change_log::store::{ChangeEntry, ChangeLog, ChangeLogCapacities, MAX_CAPACITY, Ring};
use crate::stm::snapshot::SnapshotError;
use bytes::Bytes;
use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeKind;
use iggy_common::{MetadataChangeClass, MetadataChangeGroup};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// The change log as the metadata snapshot stores it. Entries keep their
/// encoded bodies and ring order, so identical logs encode identically.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeLogSnapshot {
    pub log_id: Option<u128>,
    pub last_record_op: u64,
    pub resource: RingSnapshot,
    pub activity: RingSnapshot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingSnapshot {
    pub evicted_through_op: u64,
    pub evicted_through_ordinal: u32,
    pub evicted_through_timestamp: u64,
    pub entries: Vec<ChangeEntrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEntrySnapshot {
    pub op: u64,
    pub ordinal: u32,
    pub checksum: u128,
    pub timestamp: u64,
    pub actor_user_id: Option<u32>,
    pub group: u8,
    pub kind: u8,
    /// A msgpack blob: the default sequence form spends two bytes on every
    /// byte at or above 0x80.
    #[serde(with = "body_bytes")]
    pub body: Bytes,
}

impl ChangeLogSnapshot {
    /// Checks the log against the snapshot it rides in.
    ///
    /// # Errors
    /// [`SnapshotError::InvalidState`] naming the first rule the log breaks.
    pub fn validate(&self, sequence_number: u64) -> Result<(), SnapshotError> {
        if self.last_record_op > sequence_number {
            return Err(SnapshotError::InvalidState(
                "change log holds a record above the snapshot",
            ));
        }
        let mut operations: BTreeMap<u64, (u128, u64)> = BTreeMap::new();
        let mut positions = Vec::new();
        for (ring, class) in [
            (&self.resource, MetadataChangeClass::Resource),
            (&self.activity, MetadataChangeClass::Activity),
        ] {
            if ring.entries.len() > MAX_CAPACITY {
                return Err(SnapshotError::InvalidState(
                    "change log ring is larger than any capacity",
                ));
            }
            let floor = (ring.evicted_through_op, ring.evicted_through_ordinal);
            if floor.0 > self.last_record_op {
                return Err(SnapshotError::InvalidState(
                    "change log floor is above its newest record",
                ));
            }
            let mut previous = floor;
            for entry in &ring.entries {
                let kind = WireMetadataChangeKind::from_code(entry.kind).map_err(|_| {
                    SnapshotError::InvalidState("change log holds an unknown record kind")
                })?;
                if kind.group().as_u8() != entry.group
                    || MetadataChangeGroup::from(kind.group()).class() != class
                {
                    return Err(SnapshotError::InvalidState(
                        "change log record sits in the wrong group or class",
                    ));
                }
                let position = (entry.op, entry.ordinal);
                if position <= previous {
                    return Err(SnapshotError::InvalidState(
                        "change log ring is not ascending above its floor",
                    ));
                }
                if entry.op > self.last_record_op {
                    return Err(SnapshotError::InvalidState(
                        "change log record is above its newest record op",
                    ));
                }
                let stamp = (entry.checksum, entry.timestamp);
                if *operations.entry(entry.op).or_insert(stamp) != stamp {
                    return Err(SnapshotError::InvalidState(
                        "change log records of one operation disagree on its checksum",
                    ));
                }
                positions.push(position);
                previous = position;
            }
        }
        positions.sort_unstable();
        if positions.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(SnapshotError::InvalidState(
                "change log rings repeat an operation ordinal",
            ));
        }
        let holds_history = !positions.is_empty()
            || self.resource.evicted_through_op != 0
            || self.activity.evicted_through_op != 0;
        if holds_history && self.log_id.is_none() {
            return Err(SnapshotError::InvalidState(
                "change log holds history without a log id",
            ));
        }
        Ok(())
    }

    /// The log this snapshot describes, each ring trimmed from the front to
    /// its local capacity.
    ///
    /// # Errors
    /// [`SnapshotError::InvalidState`] for a record kind this build does not
    /// know, which [`Self::validate`] refuses first.
    pub fn restore(&self, capacities: ChangeLogCapacities) -> Result<ChangeLog, SnapshotError> {
        Ok(ChangeLog::from_parts(
            self.log_id,
            self.last_record_op,
            self.resource.restore(capacities.resource)?,
            self.activity.restore(capacities.activity)?,
        ))
    }
}

impl RingSnapshot {
    fn restore(&self, capacity: usize) -> Result<Ring, SnapshotError> {
        let entries = self
            .entries
            .iter()
            .map(|entry| {
                Ok(Arc::new(ChangeEntry {
                    op: entry.op,
                    ordinal: entry.ordinal,
                    checksum: entry.checksum,
                    timestamp: entry.timestamp,
                    actor_user_id: entry.actor_user_id,
                    kind: WireMetadataChangeKind::from_code(entry.kind).map_err(|_| {
                        SnapshotError::InvalidState("change log holds an unknown record kind")
                    })?,
                    body: entry.body.clone(),
                }))
            })
            .collect::<Result<VecDeque<_>, SnapshotError>>()?;
        Ok(Ring::restored(
            entries,
            (self.evicted_through_op, self.evicted_through_ordinal),
            self.evicted_through_timestamp,
            capacity,
        ))
    }
}

impl From<&ChangeLog> for ChangeLogSnapshot {
    fn from(log: &ChangeLog) -> Self {
        Self {
            log_id: log.log_id(),
            last_record_op: log.last_record_op(),
            resource: RingSnapshot::from(log.ring(MetadataChangeClass::Resource)),
            activity: RingSnapshot::from(log.ring(MetadataChangeClass::Activity)),
        }
    }
}

impl From<&Ring> for RingSnapshot {
    fn from(ring: &Ring) -> Self {
        let (evicted_through_op, evicted_through_ordinal) = ring.evicted_through();
        Self {
            evicted_through_op,
            evicted_through_ordinal,
            evicted_through_timestamp: ring.evicted_through_timestamp(),
            entries: ring
                .entries()
                .iter()
                .map(|entry| ChangeEntrySnapshot {
                    op: entry.op,
                    ordinal: entry.ordinal,
                    checksum: entry.checksum,
                    timestamp: entry.timestamp,
                    actor_user_id: entry.actor_user_id,
                    group: entry.group().as_u8(),
                    kind: entry.kind.as_u8(),
                    body: entry.body.clone(),
                })
                .collect(),
        }
    }
}

mod body_bytes {
    use bytes::Bytes;
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
        deserializer.deserialize_byte_buf(BodyVisitor)
    }

    struct BodyVisitor;

    impl Visitor<'_> for BodyVisitor {
        type Value = Bytes;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("change record body bytes")
        }

        fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Ok(Bytes::copy_from_slice(bytes))
        }

        fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
            Ok(Bytes::from(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::AppliedChange;
    use crate::change_log::store::MetadataChangeLog;
    use iggy_binary_protocol::PrepareHeader;
    use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeKind as Kind;

    type Corruption = Box<dyn Fn(&mut ChangeLogSnapshot)>;

    #[test]
    fn given_log_when_round_tripping_snapshot_should_restore_the_same_log_and_bytes() {
        let log = populated_log();
        let snapshot = log
            .read(|log| ChangeLogSnapshot::from(log))
            .expect("readable");
        let encoded = rmp_serde::to_vec(&snapshot).expect("encode");

        let decoded: ChangeLogSnapshot = rmp_serde::from_slice(&encoded).expect("decode");
        decoded.validate(10).expect("valid");
        let restored = MetadataChangeLog::new(ChangeLogCapacities::default());
        restored
            .restore(
                decoded
                    .restore(ChangeLogCapacities::default())
                    .expect("restore"),
            )
            .expect("writer");

        let reencoded = restored
            .read(|log| rmp_serde::to_vec(&ChangeLogSnapshot::from(log)).expect("encode"))
            .expect("readable");
        assert_eq!(decoded, snapshot);
        assert_eq!(reencoded, encoded, "identical logs must encode identically");
    }

    #[test]
    fn given_known_zero_and_unknown_log_ids_when_round_tripping_should_keep_them_apart() {
        for log_id in [None, Some(0)] {
            let snapshot = ChangeLogSnapshot {
                log_id,
                ..ChangeLogSnapshot::default()
            };
            let encoded = rmp_serde::to_vec(&snapshot).expect("encode");
            let decoded: ChangeLogSnapshot = rmp_serde::from_slice(&encoded).expect("decode");
            assert_eq!(decoded.log_id, log_id);
            decoded
                .validate(5)
                .expect("an empty log is valid at any sequence");
        }
    }

    #[test]
    fn given_ring_above_local_capacity_when_restoring_should_trim_front_and_advance_floor() {
        let snapshot = populated_log()
            .read(|log| ChangeLogSnapshot::from(log))
            .expect("readable");

        let restored = snapshot
            .restore(ChangeLogCapacities {
                resource: 1,
                activity: 1,
            })
            .expect("restore");

        let resource = restored.ring(MetadataChangeClass::Resource);
        assert_eq!(
            resource
                .entries()
                .iter()
                .map(|entry| entry.position())
                .collect::<Vec<_>>(),
            [(10, 0)]
        );
        assert_eq!(resource.evicted_through(), (4, 1));
        assert_eq!(resource.evicted_through_timestamp(), 400);
        assert_eq!(restored.last_record_op(), 10);
    }

    #[test]
    fn given_broken_log_when_validating_should_refuse_each_rule() {
        let valid = populated_log()
            .read(|log| ChangeLogSnapshot::from(log))
            .expect("readable");
        let cases: Vec<(&str, Corruption, u64)> = vec![
            ("record above the snapshot", Box::new(|_| {}), 9),
            (
                "unsorted ring",
                Box::new(|log| log.resource.entries.swap(0, 1)),
                10,
            ),
            (
                "checksum differs within an operation",
                Box::new(|log| log.resource.entries[1].checksum += 1),
                10,
            ),
            (
                "ordinal repeated across rings",
                Box::new(|log| log.activity.entries[0].ordinal = 0),
                10,
            ),
            (
                "record at or below its floor",
                Box::new(|log| {
                    log.resource.evicted_through_op = 4;
                    log.resource.evicted_through_ordinal = 0;
                }),
                10,
            ),
            (
                "floor above the newest record",
                Box::new(|log| log.activity.evicted_through_op = 11),
                11,
            ),
            (
                "record above the newest record op",
                Box::new(|log| log.last_record_op = 4),
                10,
            ),
            (
                "history without a log id",
                Box::new(|log| log.log_id = None),
                10,
            ),
            (
                "unknown kind",
                Box::new(|log| log.resource.entries[0].kind = 99),
                10,
            ),
            (
                "kind under another group",
                Box::new(|log| log.resource.entries[0].group = 2),
                10,
            ),
            (
                "activity kind in the resource ring",
                Box::new(|log| {
                    let mut entry = log.activity.entries.remove(0);
                    entry.ordinal = 5;
                    entry.op = 10;
                    log.resource.entries.push(entry);
                }),
                10,
            ),
        ];

        for (rule, corrupt, sequence_number) in cases {
            let mut snapshot = valid.clone();
            corrupt(&mut snapshot);
            assert!(
                matches!(
                    snapshot.validate(sequence_number),
                    Err(SnapshotError::InvalidState(_))
                ),
                "{rule} must be refused"
            );
        }
        valid.validate(10).expect("the untouched log is valid");
    }

    #[test]
    fn given_ring_above_the_largest_capacity_when_validating_should_refuse_it() {
        let entry = ChangeEntrySnapshot {
            op: 1,
            ordinal: 0,
            checksum: 1,
            timestamp: 1,
            actor_user_id: None,
            group: Kind::StreamCreated.group().as_u8(),
            kind: Kind::StreamCreated.as_u8(),
            body: Bytes::new(),
        };
        let snapshot = ChangeLogSnapshot {
            log_id: Some(1),
            last_record_op: 1,
            resource: RingSnapshot {
                entries: vec![entry; MAX_CAPACITY + 1],
                ..RingSnapshot::default()
            },
            activity: RingSnapshot::default(),
        };

        assert!(matches!(
            snapshot.validate(1),
            Err(SnapshotError::InvalidState(_))
        ));
    }

    /// Resource records at ops 4 and 10, an activity record at op 4.
    fn populated_log() -> MetadataChangeLog {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        let change = |kind| AppliedChange {
            kind,
            actor_user_id: Some(1),
            body: Bytes::from_static(&[0x80, 0xff, 1]),
        };
        log.append(&header(1, 0x0d4b, 100), Vec::new())
            .expect("append");
        log.append(
            &header(4, 0x44, 400),
            vec![
                change(Kind::UserDeleted),
                change(Kind::UserPermissionsUpdated),
                change(Kind::ConsumerGroupMemberLeft),
            ],
        )
        .expect("append");
        log.append(&header(10, 0x10, 1_000), vec![change(Kind::StreamCreated)])
            .expect("append");
        log
    }

    fn header(op: u64, checksum: u128, timestamp: u64) -> PrepareHeader {
        PrepareHeader {
            op,
            checksum,
            timestamp,
            ..PrepareHeader::default()
        }
    }
}
