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

//! The positions of the consumers of a partition of a geo topic or a shadow
//! topic, as every datacenter that reads the partition's log sees them.
//!
//! A position belongs to a [`PositionHolder`]: a consumer group, the
//! external offsets of a consumer group, which the Kafka gateway keeps for a
//! Kafka group, or a plain consumer. It is a [`GeoPosition`]: a reset counter
//! and a value. Of two positions the higher one wins, the reset first. This
//! cluster folds what its consumers commit into the machine with
//! [`NOTE_GEO_POSITIONS`], and takes the positions that other datacenters
//! publish with [`ADOPT_GEO_POSITIONS`]. Both commit through the partition's
//! consensus, so every replica holds the same positions, and a restart or a
//! new primary keeps them.
//!
//! A group is known across datacenters by its name: group ids belong to one
//! cluster, and a deleted group's id is never reused. A plain consumer is
//! known by its id, which its client picks. An entry binds to the local
//! reader of its holder, and records the offset that reader had committed
//! when the machine last looked, its base. While the reader's offset is still
//! its base, the reader reads from the entry's position, see
//! [`IggyPartition::geo_group_offset`]. Once the reader stores another
//! offset, it reads from its own until the next fold:
//!
//! - an offset below the entry's value is a move backwards: the fold adds one
//!   to the reset, so the move wins over the higher offsets that other
//!   datacenters still hold;
//! - any other offset becomes the value at the same reset.
//!
//! An entry whose group was deleted becomes a tombstone, one reset higher, so
//! a position that another datacenter still holds does not come back. A
//! plain consumer is never deleted, only its offset.

use std::collections::{BTreeMap, HashMap};

use iggy_binary_protocol::RoutedRequestHeader;
use iggy_common::{ConsumerGroupId, ConsumerKind};
use iggy_object_store::{
    GeoPosition, MAX_GROUP_NAME_LEN, POSITIONS_PER_KIND_MAX, POSITIONS_PER_PARTITION_MAX,
    PositionHolder, PositionValue,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;
use std::sync::atomic::Ordering;

use crate::IggyPartition;
use crate::state_machine::{
    Applied, COMMAND_BYTES_MAX, CommandEncodeError, CommandKind, MACHINE_COMMAND_HEADER_SIZE,
    MachineCommand, MachineId, MachineRestoreError, PartitionStateMachine,
};

/// The machine's id in snapshots, stable within a storage format.
pub const GEO_POSITIONS_MACHINE: MachineId = MachineId(4);

/// Fold what local readers committed. See [`GeoNote`].
pub const NOTE_GEO_POSITIONS: CommandKind = CommandKind(8);

/// Take positions that other datacenters published.
pub const ADOPT_GEO_POSITIONS: CommandKind = CommandKind(9);

/// The most holders one partition holds positions for.
pub const GEO_POSITIONS_MAX: usize = POSITIONS_PER_PARTITION_MAX;

/// The most holders of one kind one partition holds positions for.
///
/// It is as many as `[partition] consumer_offsets_max` takes offsets of a
/// kind by default. So the plain consumers of a partition leave room for its
/// groups.
pub const GEO_POSITIONS_PER_KIND_MAX: usize = POSITIONS_PER_KIND_MAX;

const SNAPSHOT_VERSION: u8 = 2;
const KIND_DELETED: u8 = 0;
const KIND_UNSET: u8 = 1;
const KIND_OFFSET: u8 = 2;
const HOLDER_GROUP: u8 = 0;
const HOLDER_EXTERNAL_GROUP: u8 = 1;
const HOLDER_CONSUMER: u8 = 2;
const HAS_GROUP: u8 = 1;
const HAS_OFFSET: u8 = 1 << 1;
/// A holder's byte, a name's length byte and its longest bytes, then the
/// widest fields of a note or an adoption.
const ROW_BYTES_MAX: usize = 1 + 1 + MAX_GROUP_NAME_LEN + 1 + 8 + 8 + 4;
const COUNT_BYTES: usize = size_of::<u16>();
const PAYLOAD_BYTES_MAX: usize = COMMAND_BYTES_MAX - MACHINE_COMMAND_HEADER_SIZE;

/// One holder's position, the local reader it is bound to, and the offset
/// that reader had committed when the machine last looked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeoEntry {
    pub position: GeoPosition,
    /// The id of the local reader of the entry's holder: the group of its
    /// name, or the plain consumer of its id.
    pub bound: Option<u32>,
    /// The offset the bound reader had committed at the last fold or bind.
    pub base: Option<u64>,
}

/// What the primary saw of one holder in this cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoNote {
    /// No local group has the holder's name.
    Gone,
    /// The local reader `id` is the holder's, and committed `committed`.
    Local { id: u32, committed: Option<u64> },
}

/// A local consumer group of the partition's topic, as committed metadata
/// names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalGroup {
    pub group_id: u32,
    pub name: String,
    /// Whether the group has members in this cluster. Their reads and
    /// commits make its position here, so a peer's does not move it.
    pub consuming: bool,
}

/// The positions of the consumers of one partition.
#[derive(Debug, Default)]
pub struct GeoPositions {
    entries: BTreeMap<PositionHolder, GeoEntry>,
    /// The holder of the entry bound to each local reader, by the kind of
    /// its offsets and its id.
    bound: HashMap<(ConsumerKind, u32), PositionHolder>,
}

impl GeoPositions {
    #[must_use]
    pub const fn entries(&self) -> &BTreeMap<PositionHolder, GeoEntry> {
        &self.entries
    }

    #[must_use]
    pub fn get(&self, holder: &PositionHolder) -> Option<&GeoEntry> {
        self.entries.get(holder)
    }

    /// The entry bound to the local reader `id` whose offsets are of `kind`.
    #[must_use]
    pub fn bound_to(&self, kind: ConsumerKind, id: u32) -> Option<(&PositionHolder, &GeoEntry)> {
        let holder = self.bound.get(&(kind, id))?;
        self.entries.get(holder).map(|entry| (holder, entry))
    }

    /// Fold what the primary saw of `holder`.
    pub fn note(&mut self, holder: &PositionHolder, note: GeoNote) -> bool {
        match note {
            GeoNote::Gone => {
                let Some(entry) = self.entries.get_mut(holder) else {
                    return false;
                };
                let Some(id) = entry.bound.take() else {
                    return false;
                };
                entry.position = tombstone_after(entry.position);
                entry.base = None;
                self.bound.remove(&(offset_kind(holder), id));
                true
            }
            GeoNote::Local { id, committed } => self.note_local(holder, id, committed),
        }
    }

    fn note_local(&mut self, holder: &PositionHolder, id: u32, committed: Option<u64>) -> bool {
        let local = PositionValue::of_offset(committed);
        let kind = offset_kind(holder);
        if !self.entries.contains_key(holder) && !self.make_room(kind) {
            return false;
        }
        if let Some(other) = self.bound.get(&(kind, id)).filter(|other| *other != holder) {
            // Group ids are never reused, so the id cannot name two groups.
            let other = other.clone();
            if let Some(entry) = self.entries.get_mut(&other) {
                entry.bound = None;
                entry.base = None;
            }
        }
        let entry = self.entries.entry(holder.clone()).or_insert(GeoEntry {
            position: GeoPosition {
                reset: 0,
                value: local,
            },
            bound: None,
            base: committed,
        });
        match entry.bound {
            Some(bound) if bound == id => {
                if entry.base == committed {
                    return false;
                }
                entry.position = if local < entry.position.value {
                    GeoPosition {
                        reset: entry.position.reset.saturating_add(1),
                        value: local,
                    }
                } else {
                    GeoPosition {
                        reset: entry.position.reset,
                        value: local,
                    }
                };
            }
            Some(old) => {
                // The group of the name was deleted and created again.
                self.bound.remove(&(kind, old));
                entry.position = tombstone_after(entry.position);
                entry.position.value = entry.position.value.max(local);
            }
            None => {
                // The reader never saw the entry's position, so a lower
                // offset of its own is no move backwards.
                entry.position.value = entry.position.value.max(local);
            }
        }
        entry.bound = Some(id);
        entry.base = committed;
        self.bound.insert((kind, id), holder.clone());
        true
    }

    /// Take `position` for `holder` when it is higher than the entry's.
    pub fn adopt(&mut self, holder: &PositionHolder, position: GeoPosition) -> bool {
        if let Some(entry) = self.entries.get_mut(holder) {
            if position <= entry.position {
                return false;
            }
            entry.position = position;
            return true;
        }
        if !self.make_room(offset_kind(holder)) {
            return false;
        }
        self.entries.insert(
            holder.clone(),
            GeoEntry {
                position,
                bound: None,
                base: None,
            },
        );
        true
    }

    /// Whether a new holder whose offsets are of `kind` fits, after dropping
    /// a tombstone of that kind that no reader is bound to when the machine
    /// holds the most of the kind.
    fn make_room(&mut self, kind: ConsumerKind) -> bool {
        let of_kind = |holder: &PositionHolder| offset_kind(holder) == kind;
        if self.entries.keys().filter(|holder| of_kind(holder)).count() < GEO_POSITIONS_PER_KIND_MAX
        {
            return true;
        }
        let Some(dropped) = self
            .entries
            .iter()
            .find(|(holder, entry)| {
                of_kind(holder)
                    && entry.bound.is_none()
                    && entry.position.value == PositionValue::Deleted
            })
            .map(|(holder, _)| holder.clone())
        else {
            return false;
        };
        self.entries.remove(&dropped);
        true
    }

    /// The offset that the local reader `id` of `kind`, whose stored offset
    /// is `stored`, reads from. `None` when its own offset applies.
    #[must_use]
    pub fn read_offset(
        &self,
        kind: ConsumerKind,
        id: u32,
        stored: Option<u64>,
    ) -> Option<Option<u64>> {
        let (_, entry) = self.bound_to(kind, id)?;
        if entry.base != stored {
            return None;
        }
        match entry.position.value {
            PositionValue::Offset(offset) => Some(Some(offset)),
            PositionValue::Unset => Some(None),
            PositionValue::Deleted => None,
        }
    }

    fn rebuild_bound(&mut self) {
        self.bound = self
            .entries
            .iter()
            .filter_map(|(holder, entry)| {
                entry
                    .bound
                    .map(|id| ((offset_kind(holder), id), holder.clone()))
            })
            .collect();
    }
}

/// The kind of the local offsets that the entry of `holder` binds to.
#[must_use]
pub const fn offset_kind(holder: &PositionHolder) -> ConsumerKind {
    match holder {
        PositionHolder::Group(_) => ConsumerKind::ConsumerGroup,
        PositionHolder::ExternalGroup(_) => ConsumerKind::ExternalGroup,
        PositionHolder::Consumer(_) => ConsumerKind::Consumer,
    }
}

/// The tombstone that replaces `position`, one reset higher.
const fn tombstone_after(position: GeoPosition) -> GeoPosition {
    GeoPosition {
        reset: position.reset.saturating_add(1),
        value: PositionValue::Deleted,
    }
}

impl PartitionStateMachine for GeoPositions {
    fn id(&self) -> MachineId {
        GEO_POSITIONS_MACHINE
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[NOTE_GEO_POSITIONS, ADOPT_GEO_POSITIONS]
    }

    /// Each note and adoption is idempotent: a second apply finds the base
    /// it set, or a position that is no longer higher.
    fn apply(&mut self, _: u64, command: MachineCommand<'_>) -> Applied {
        let changed = match command.kind {
            NOTE_GEO_POSITIONS => decode_notes(command.payload).map(|notes| {
                notes.into_iter().fold(false, |changed, (holder, note)| {
                    self.note(&holder, note) | changed
                })
            }),
            ADOPT_GEO_POSITIONS => decode_adoptions(command.payload).map(|adoptions| {
                adoptions
                    .into_iter()
                    .fold(false, |changed, (holder, position)| {
                        self.adopt(&holder, position) | changed
                    })
            }),
            _ => None,
        };
        match changed {
            Some(true) => Applied::Changed,
            Some(false) | None => Applied::Refused,
        }
    }

    fn snapshot_into(&self, out: &mut Vec<u8>) {
        out.push(SNAPSHOT_VERSION);
        let count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for (holder, entry) in &self.entries {
            put_holder(out, holder);
            put_position(out, entry.position);
            let flags = (if entry.bound.is_some() { HAS_GROUP } else { 0 })
                | (if entry.base.is_some() { HAS_OFFSET } else { 0 });
            out.push(flags);
            out.extend_from_slice(&entry.bound.unwrap_or(0).to_le_bytes());
            out.extend_from_slice(&entry.base.unwrap_or(0).to_le_bytes());
        }
    }

    fn holds_state(&self) -> bool {
        !self.entries.is_empty()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
        let refused = MachineRestoreError {
            reason: "a geo positions snapshot is a version byte and the entries in holder order",
        };
        let mut reader = Reader(snapshot);
        if reader.u8() != Some(SNAPSHOT_VERSION) {
            return Err(refused);
        }
        let count = reader.u32().ok_or(refused)? as usize;
        if count > GEO_POSITIONS_MAX {
            return Err(refused);
        }
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let holder = reader.holder().ok_or(refused)?;
            let position = reader.position().ok_or(refused)?;
            let flags = reader.u8().ok_or(refused)?;
            let id = reader.u32().ok_or(refused)?;
            let base = reader.u64().ok_or(refused)?;
            if flags & !(HAS_GROUP | HAS_OFFSET) != 0
                || entries
                    .last_key_value()
                    .is_some_and(|(last, _)| *last >= holder)
            {
                return Err(refused);
            }
            entries.insert(
                holder,
                GeoEntry {
                    position,
                    bound: (flags & HAS_GROUP != 0).then_some(id),
                    base: (flags & HAS_OFFSET != 0).then_some(base),
                },
            );
        }
        if !reader.0.is_empty() {
            return Err(refused);
        }
        self.entries = entries;
        self.rebuild_bound();
        Ok(())
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.bound.clear();
    }
}

/// The payloads of the commands that carry `notes`, each within the size of
/// one command.
#[must_use]
pub fn encode_notes(notes: &[(PositionHolder, GeoNote)]) -> Vec<Vec<u8>> {
    pack(notes, |out, (holder, note)| {
        put_holder(out, holder);
        let (flags, id, committed) = match *note {
            GeoNote::Gone => (0, 0, None),
            GeoNote::Local { id, committed } => (HAS_GROUP, id, committed),
        };
        out.push(flags | if committed.is_some() { HAS_OFFSET } else { 0 });
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&committed.unwrap_or(0).to_le_bytes());
    })
}

/// The payloads of the commands that carry `adoptions`, each within the
/// size of one command.
#[must_use]
pub fn encode_adoptions(adoptions: &[(PositionHolder, GeoPosition)]) -> Vec<Vec<u8>> {
    pack(adoptions, |out, (holder, position)| {
        put_holder(out, holder);
        put_position(out, *position);
    })
}

fn pack<T>(rows: &[T], mut put: impl FnMut(&mut Vec<u8>, &T)) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    let mut rows = rows.iter().peekable();
    while rows.peek().is_some() {
        let mut payload = vec![0; COUNT_BYTES];
        let mut count: u16 = 0;
        while payload.len() + ROW_BYTES_MAX <= PAYLOAD_BYTES_MAX && count < u16::MAX {
            let Some(row) = rows.next() else {
                break;
            };
            put(&mut payload, row);
            count += 1;
        }
        payload[..COUNT_BYTES].copy_from_slice(&count.to_le_bytes());
        payloads.push(payload);
    }
    payloads
}

fn decode_notes(payload: &[u8]) -> Option<Vec<(PositionHolder, GeoNote)>> {
    let mut reader = Reader(payload);
    let count = reader.u16()?;
    let mut notes = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let holder = reader.holder()?;
        let flags = reader.u8()?;
        let id = reader.u32()?;
        let committed = reader.u64()?;
        let note = match flags {
            0 if id == 0 && committed == 0 => GeoNote::Gone,
            HAS_GROUP if committed == 0 => GeoNote::Local {
                id,
                committed: None,
            },
            flags if flags == HAS_GROUP | HAS_OFFSET => GeoNote::Local {
                id,
                committed: Some(committed),
            },
            _ => return None,
        };
        // A plain consumer is its own reader, and is never gone.
        if let PositionHolder::Consumer(consumer_id) = &holder
            && !matches!(note, GeoNote::Local { id, .. } if id == *consumer_id)
        {
            return None;
        }
        notes.push((holder, note));
    }
    reader.0.is_empty().then_some(notes)
}

fn decode_adoptions(payload: &[u8]) -> Option<Vec<(PositionHolder, GeoPosition)>> {
    let mut reader = Reader(payload);
    let count = reader.u16()?;
    let mut adoptions = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let holder = reader.holder()?;
        adoptions.push((holder, reader.position()?));
    }
    reader.0.is_empty().then_some(adoptions)
}

/// Callers keep group names within [`MAX_GROUP_NAME_LEN`] bytes.
fn put_holder(out: &mut Vec<u8>, holder: &PositionHolder) {
    match holder {
        PositionHolder::Group(name) => {
            out.push(HOLDER_GROUP);
            put_name(out, name);
        }
        PositionHolder::ExternalGroup(name) => {
            out.push(HOLDER_EXTERNAL_GROUP);
            put_name(out, name);
        }
        PositionHolder::Consumer(consumer_id) => {
            out.push(HOLDER_CONSUMER);
            out.extend_from_slice(&consumer_id.to_le_bytes());
        }
    }
}

/// Callers keep names within [`MAX_GROUP_NAME_LEN`] bytes, which one length
/// byte holds.
fn put_name(out: &mut Vec<u8>, name: &str) {
    let len = u8::try_from(name.len()).unwrap_or(u8::MAX);
    out.push(len);
    out.extend_from_slice(&name.as_bytes()[..usize::from(len)]);
}

fn put_position(out: &mut Vec<u8>, position: GeoPosition) {
    let (kind, offset) = match position.value {
        PositionValue::Deleted => (KIND_DELETED, 0),
        PositionValue::Unset => (KIND_UNSET, 0),
        PositionValue::Offset(offset) => (KIND_OFFSET, offset),
    };
    out.extend_from_slice(&position.reset.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&offset.to_le_bytes());
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (taken, left) = self.0.split_first_chunk::<N>()?;
        self.0 = left;
        Some(*taken)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[byte]| byte)
    }

    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }

    fn name(&mut self) -> Option<String> {
        let len = usize::from(self.u8()?);
        if len == 0 {
            return None;
        }
        let (name, left) = self.0.split_at_checked(len)?;
        self.0 = left;
        std::str::from_utf8(name).ok().map(str::to_owned)
    }

    fn holder(&mut self) -> Option<PositionHolder> {
        match self.u8()? {
            HOLDER_GROUP => self.name().map(PositionHolder::Group),
            HOLDER_EXTERNAL_GROUP => self.name().map(PositionHolder::ExternalGroup),
            HOLDER_CONSUMER => self.u32().map(PositionHolder::Consumer),
            _ => None,
        }
    }

    fn position(&mut self) -> Option<GeoPosition> {
        let reset = self.u64()?;
        let kind = self.u8()?;
        let offset = self.u64()?;
        let value = match (kind, offset) {
            (KIND_DELETED, 0) => PositionValue::Deleted,
            (KIND_UNSET, 0) => PositionValue::Unset,
            (KIND_OFFSET, offset) => PositionValue::Offset(offset),
            _ => return None,
        };
        Some(GeoPosition { reset, value })
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The consumer group positions this partition committed. `None` on a
    /// partition built without the machine.
    #[must_use]
    pub fn geo_positions(&self) -> Option<&GeoPositions> {
        self.state_machine::<GeoPositions>(GEO_POSITIONS_MACHINE)
    }

    /// The offset of the local reader `id` of `kind` as the replicas
    /// committed it.
    #[must_use]
    pub fn committed_offset(&self, kind: ConsumerKind, id: u32) -> Option<u64> {
        self.durable_consumer_offsets
            .get(kind, id)
            .map(|state| state.committed_offset)
    }

    /// The offset that the local group `group_id` reads from: the position
    /// of the entry bound to it while the group's stored offset is still the
    /// entry's base, and the stored offset otherwise.
    #[must_use]
    pub fn geo_group_offset(&self, group_id: usize) -> Option<u64> {
        let stored = self
            .consumer_group_offsets
            .borrow()
            .get(&ConsumerGroupId(group_id))
            .map(|offset| offset.offset.load(Ordering::Relaxed));
        self.geo_read(ConsumerKind::ConsumerGroup, group_id, stored)
    }

    /// [`Self::geo_group_offset`] for the plain consumer `consumer_id`.
    #[must_use]
    pub fn geo_consumer_offset(&self, consumer_id: usize) -> Option<u64> {
        let stored = self
            .consumer_offsets
            .borrow()
            .get(&consumer_id)
            .map(|offset| offset.offset.load(Ordering::Relaxed));
        self.geo_read(ConsumerKind::Consumer, consumer_id, stored)
    }

    /// [`Self::geo_group_offset`] for the external offsets of the group
    /// `group_id`.
    #[must_use]
    pub fn geo_external_group_offset(&self, group_id: u32) -> Option<u64> {
        let stored = self.external_group_offset(group_id);
        self.geo_read(ConsumerKind::ExternalGroup, group_id as usize, stored)
    }

    /// The offset that the position of `holder` names, for a holder with no
    /// local reader: a group that this cluster does not have.
    #[must_use]
    pub fn geo_position_offset(&self, holder: &PositionHolder) -> Option<u64> {
        match self.geo_positions()?.get(holder)?.position.value {
            PositionValue::Offset(offset) => Some(offset),
            PositionValue::Unset | PositionValue::Deleted => None,
        }
    }

    fn geo_read(&self, kind: ConsumerKind, id: usize, stored: Option<u64>) -> Option<u64> {
        let Ok(id) = u32::try_from(id) else {
            return stored;
        };
        self.geo_positions()
            .and_then(|positions| positions.read_offset(kind, id, stored))
            .unwrap_or(stored)
    }

    /// What the primary must fold of the partition's readers: `groups`, the
    /// local groups of its topic, and its plain consumers. It folds every
    /// reader whose committed offset is not its entry's base, every reader
    /// that has an entry not bound to it, and every bound entry whose group
    /// is gone. A group's external offsets get an entry once it stores one
    /// or a peer publishes one, so a group that never stores them takes no
    /// room for them.
    #[must_use]
    pub fn geo_notes(&self, groups: &[LocalGroup]) -> Vec<(PositionHolder, GeoNote)> {
        let Some(positions) = self.geo_positions() else {
            return Vec::new();
        };
        let mut notes = Vec::new();
        let mut note = |holder: PositionHolder, id: u32, committed: Option<u64>| {
            let current = positions
                .get(&holder)
                .is_some_and(|entry| entry.bound == Some(id) && entry.base == committed);
            if !current {
                notes.push((holder, GeoNote::Local { id, committed }));
            }
        };
        for group in groups {
            if group.name.is_empty() || group.name.len() > MAX_GROUP_NAME_LEN {
                continue;
            }
            let own = PositionHolder::Group(group.name.clone());
            note(
                own,
                group.group_id,
                self.committed_offset(ConsumerKind::ConsumerGroup, group.group_id),
            );
            let external = PositionHolder::ExternalGroup(group.name.clone());
            let committed = self.committed_offset(ConsumerKind::ExternalGroup, group.group_id);
            if committed.is_some() || positions.get(&external).is_some() {
                note(external, group.group_id, committed);
            }
        }
        let mut consumers =
            self.durable_consumer_offsets
                .with_entries(ConsumerKind::Consumer, |consumers| {
                    consumers
                        .iter()
                        .map(|(id, state)| (*id, state.committed_offset))
                        .collect::<Vec<_>>()
                });
        consumers.sort_unstable();
        for (consumer_id, committed) in consumers {
            note(
                PositionHolder::Consumer(consumer_id),
                consumer_id,
                Some(committed),
            );
        }
        for holder in positions.entries().keys() {
            if let PositionHolder::Consumer(consumer_id) = holder
                && self
                    .committed_offset(ConsumerKind::Consumer, *consumer_id)
                    .is_none()
            {
                note(holder.clone(), *consumer_id, None);
            }
        }
        for (holder, entry) in positions.entries() {
            let (Some(bound), Some(name)) = (entry.bound, holder.group_name()) else {
                continue;
            };
            if !groups
                .iter()
                .any(|group| group.group_id == bound || group.name == name)
            {
                notes.push((holder.clone(), GeoNote::Gone));
            }
        }
        notes
    }

    /// Whether the entry bound to the local reader `id` of `kind` folded
    /// what the reader committed: the position this partition publishes for
    /// it is what it committed.
    #[must_use]
    pub fn geo_folded(&self, kind: ConsumerKind, id: u32) -> bool {
        self.geo_positions()
            .and_then(|positions| positions.bound_to(kind, id))
            .is_some_and(|(_, entry)| entry.base == self.committed_offset(kind, id))
    }

    /// The positions of `peer` that this partition takes: those higher than
    /// its own, of a name whose local group has no members here, or of any
    /// name while `consuming_groups_too` (no group reads anything while it is
    /// set). A reader with a stored offset its entry has not folded yet
    /// waits for the fold.
    ///
    /// Only a group's own offsets have members to check. A group's external
    /// offsets and a plain consumer take a higher position at any time, so a
    /// log is read by one of them in one datacenter at a time.
    #[must_use]
    pub fn geo_adoptions(
        &self,
        groups: &[LocalGroup],
        peer: &BTreeMap<PositionHolder, GeoPosition>,
        consuming_groups_too: bool,
    ) -> Vec<(PositionHolder, GeoPosition)> {
        let Some(positions) = self.geo_positions() else {
            return Vec::new();
        };
        let mut adoptions = Vec::new();
        for (holder, position) in peer {
            if holder
                .group_name()
                .is_some_and(|name| name.is_empty() || name.len() > MAX_GROUP_NAME_LEN)
            {
                continue;
            }
            let entry = positions.get(holder);
            if entry.is_some_and(|entry| *position <= entry.position) {
                continue;
            }
            let group = |name: &str| groups.iter().find(|group| group.name == name);
            let reader = match holder {
                PositionHolder::Group(name) => match group(name) {
                    Some(group) if group.consuming && !consuming_groups_too => continue,
                    found => found.map(|group| group.group_id),
                },
                PositionHolder::ExternalGroup(name) => group(name).map(|group| group.group_id),
                PositionHolder::Consumer(consumer_id) => Some(*consumer_id),
            };
            if let (Some(entry), Some(id)) = (entry, reader) {
                let kind = offset_kind(holder);
                let folded =
                    entry.bound == Some(id) && entry.base == self.committed_offset(kind, id);
                if !folded {
                    continue;
                }
            }
            adoptions.push((holder.clone(), *position));
        }
        adoptions
    }

    /// The requests that commit `payloads` of `kind`, numbered from
    /// `first_request`.
    ///
    /// # Errors
    /// Returns an error when a payload is too large for a command.
    pub fn geo_position_requests(
        &self,
        kind: CommandKind,
        payloads: &[Vec<u8>],
        first_request: u64,
    ) -> Result<Vec<Message<RoutedRequestHeader>>, CommandEncodeError> {
        payloads
            .iter()
            .zip(first_request..)
            .map(|(payload, request)| self.command_request(kind, payload, request))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offset(reset: u64, offset: u64) -> GeoPosition {
        GeoPosition {
            reset,
            value: PositionValue::Offset(offset),
        }
    }

    fn group(id: u32, committed: Option<u64>) -> GeoNote {
        GeoNote::Local { id, committed }
    }

    fn g(name: &str) -> PositionHolder {
        PositionHolder::Group(name.to_owned())
    }

    const GROUP: ConsumerKind = ConsumerKind::ConsumerGroup;

    #[test]
    fn given_a_higher_offset_from_a_peer_when_adopting_should_take_it_and_never_a_lower_one() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));

        assert!(positions.adopt(&g("billing"), offset(0, 250)));
        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            offset(0, 250)
        );
        assert!(!positions.adopt(&g("billing"), offset(0, 120)));
        assert!(!positions.adopt(&g("billing"), offset(0, 250)));
        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            offset(0, 250)
        );
    }

    #[test]
    fn given_a_seek_backwards_when_folding_should_bump_the_reset_so_it_wins_over_higher_offsets() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));
        positions.adopt(&g("billing"), offset(0, 250));
        positions.note(&g("billing"), group(3, Some(260)));
        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            offset(0, 260)
        );

        positions.note(&g("billing"), group(3, Some(40)));

        let entry = positions.get(&g("billing")).unwrap();
        assert_eq!(entry.position, offset(1, 40));
        assert!(
            !positions.clone_adopt(&g("billing"), offset(0, 900)),
            "a higher offset of an older reset loses"
        );
        assert!(positions.clone_adopt(&g("billing"), offset(1, 41)));
    }

    #[test]
    fn given_a_peer_that_moved_back_when_adopting_should_take_the_lower_offset_of_the_higher_reset()
    {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(500)));

        assert!(positions.adopt(&g("billing"), offset(1, 20)));

        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            offset(1, 20)
        );
        assert_eq!(
            positions.read_offset(GROUP, 3, Some(500)),
            Some(Some(20)),
            "the group reads from the peer's earlier offset"
        );
    }

    #[test]
    fn given_a_position_ahead_of_the_data_when_reading_should_keep_it_unclamped() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, None));
        positions.adopt(&g("billing"), offset(0, u64::MAX - 1));

        assert_eq!(
            positions.read_offset(GROUP, 3, None),
            Some(Some(u64::MAX - 1))
        );
    }

    #[test]
    fn given_a_local_store_after_an_adoption_when_reading_should_use_the_local_offset_until_folded()
    {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));
        positions.adopt(&g("billing"), offset(0, 250));

        assert_eq!(positions.read_offset(GROUP, 3, Some(100)), Some(Some(250)));
        assert_eq!(positions.read_offset(GROUP, 3, Some(255)), None, "progress");
        assert_eq!(positions.read_offset(GROUP, 3, Some(10)), None, "a seek");
        assert_eq!(
            positions.read_offset(GROUP, 4, Some(100)),
            None,
            "another group"
        );
    }

    #[test]
    fn given_a_deleted_group_when_folding_should_leave_a_tombstone_that_stale_positions_do_not_beat()
     {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));

        assert!(positions.note(&g("billing"), GeoNote::Gone));

        let entry = *positions.get(&g("billing")).unwrap();
        assert_eq!(
            entry.position,
            GeoPosition {
                reset: 1,
                value: PositionValue::Deleted
            }
        );
        assert_eq!(entry.bound, None);
        assert!(!positions.adopt(&g("billing"), offset(0, 900)));
        assert!(!positions.note(&g("billing"), GeoNote::Gone), "idempotent");
    }

    #[test]
    fn given_a_tombstone_from_a_peer_when_reading_should_fall_back_to_the_local_offset() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));
        positions.adopt(&g("billing"), offset(0, 250));

        assert!(positions.adopt(
            &g("billing"),
            GeoPosition {
                reset: 1,
                value: PositionValue::Deleted
            }
        ));

        assert_eq!(positions.read_offset(GROUP, 3, Some(100)), None);
    }

    #[test]
    fn given_a_recreated_group_when_binding_should_keep_the_reset_above_the_tombstone() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));

        positions.note(&g("billing"), group(7, Some(5)));

        let entry = *positions.get(&g("billing")).unwrap();
        assert_eq!(entry.position, offset(1, 5));
        assert_eq!(entry.bound, Some(7));
        assert!(positions.bound_to(GROUP, 3).is_none());
        assert!(!positions.adopt(&g("billing"), offset(0, 100)));
    }

    #[test]
    fn given_an_adopted_name_when_a_local_group_binds_should_keep_the_higher_value_without_a_reset()
    {
        let mut positions = GeoPositions::default();
        positions.adopt(&g("billing"), offset(2, 300));

        positions.note(&g("billing"), group(9, Some(120)));

        let entry = *positions.get(&g("billing")).unwrap();
        assert_eq!(entry.position, offset(2, 300));
        assert_eq!(entry.base, Some(120));
        assert_eq!(positions.read_offset(GROUP, 9, Some(120)), Some(Some(300)));
    }

    #[test]
    fn given_a_deleted_offset_when_folding_should_count_it_as_a_move_backwards() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));

        positions.note(&g("billing"), group(3, None));

        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            GeoPosition {
                reset: 1,
                value: PositionValue::Unset
            }
        );
    }

    #[test]
    fn given_notes_and_adoptions_when_applied_twice_should_change_nothing_the_second_time() {
        let mut positions = GeoPositions::default();
        let notes = encode_notes(&[
            (g("billing"), group(3, Some(100))),
            (g("gone"), GeoNote::Gone),
        ]);
        let adoptions = encode_adoptions(&[(g("billing"), offset(0, 300))]);
        let apply = |positions: &mut GeoPositions, kind, payload: &[u8]| {
            positions.apply(1, MachineCommand { kind, payload })
        };

        assert_eq!(
            apply(&mut positions, NOTE_GEO_POSITIONS, &notes[0]),
            Applied::Changed
        );
        assert_eq!(
            apply(&mut positions, NOTE_GEO_POSITIONS, &notes[0]),
            Applied::Refused
        );
        assert_eq!(
            apply(&mut positions, ADOPT_GEO_POSITIONS, &adoptions[0]),
            Applied::Changed
        );
        assert_eq!(
            apply(&mut positions, ADOPT_GEO_POSITIONS, &adoptions[0]),
            Applied::Refused
        );
        assert_eq!(
            apply(&mut positions, ADOPT_GEO_POSITIONS, b"\x01\x00"),
            Applied::Refused,
            "a malformed payload"
        );
    }

    #[test]
    fn given_many_rows_when_packing_should_split_them_into_commands_that_fit() {
        let long_name = "n".repeat(MAX_GROUP_NAME_LEN);
        let adoptions: Vec<_> = (0..1_000)
            .map(|index| (g(&format!("{long_name:.250}{index:05}")), offset(1, index)))
            .collect();

        let payloads = encode_adoptions(&adoptions);

        assert!(payloads.len() > 1);
        let mut decoded = Vec::new();
        for payload in &payloads {
            assert!(payload.len() <= PAYLOAD_BYTES_MAX);
            decoded.extend(decode_adoptions(payload).unwrap());
        }
        assert_eq!(decoded, adoptions);
    }

    #[test]
    fn given_a_snapshot_when_restoring_should_get_back_every_entry_and_binding() {
        let mut positions = GeoPositions::default();
        positions.note(&g("billing"), group(3, Some(100)));
        positions.adopt(&g("audit"), offset(4, 9));
        positions.note(&g("gone"), group(5, None));
        positions.note(&g("gone"), GeoNote::Gone);
        let mut snapshot = Vec::new();
        positions.snapshot_into(&mut snapshot);

        let mut restored = GeoPositions::default();
        restored.restore(&snapshot).unwrap();

        assert_eq!(restored.entries(), positions.entries());
        assert_eq!(
            restored
                .bound_to(GROUP, 3)
                .map(|(holder, _)| holder.clone()),
            Some(g("billing"))
        );
        assert!(restored.restore(&snapshot[..snapshot.len() - 1]).is_err());
        let mut versioned = snapshot.clone();
        versioned[0] = 9;
        assert!(GeoPositions::default().restore(&versioned).is_err());
    }

    #[test]
    fn given_a_full_machine_when_a_new_name_arrives_should_drop_an_unbound_tombstone_first() {
        let mut positions = GeoPositions::default();
        positions.adopt(
            &g("a-tombstone"),
            GeoPosition {
                reset: 1,
                value: PositionValue::Deleted,
            },
        );
        for index in 1..GEO_POSITIONS_PER_KIND_MAX {
            positions.adopt(&g(&format!("group-{index:05}")), offset(0, 1));
        }
        assert_eq!(positions.entries().len(), GEO_POSITIONS_PER_KIND_MAX);

        assert!(positions.adopt(&g("new"), offset(0, 1)));
        assert!(positions.get(&g("a-tombstone")).is_none());
        assert!(!positions.adopt(&g("newer"), offset(0, 1)), "no room left");
        assert!(
            positions.adopt(&PositionHolder::Consumer(7), offset(0, 1)),
            "the groups leave the room of plain consumers"
        );
    }

    #[test]
    fn given_a_group_with_external_offsets_when_folding_should_keep_two_entries_bound_to_its_id() {
        let mut positions = GeoPositions::default();
        let external = PositionHolder::ExternalGroup("billing".to_owned());
        positions.note(&g("billing"), group(3, Some(100)));
        positions.note(&external, group(3, Some(7)));

        assert_eq!(
            positions.get(&g("billing")).unwrap().position,
            offset(0, 100)
        );
        assert_eq!(positions.get(&external).unwrap().position, offset(0, 7));
        assert_eq!(
            positions
                .bound_to(ConsumerKind::ExternalGroup, 3)
                .map(|(holder, _)| holder),
            Some(&external)
        );
        assert!(positions.adopt(&external, offset(0, 50)));
        assert_eq!(
            positions.read_offset(ConsumerKind::ExternalGroup, 3, Some(7)),
            Some(Some(50))
        );
        assert_eq!(
            positions.read_offset(GROUP, 3, Some(100)),
            Some(Some(100)),
            "the group's own offsets keep their entry"
        );

        assert!(positions.note(&external, GeoNote::Gone));
        assert_eq!(positions.bound_to(ConsumerKind::ExternalGroup, 3), None);
        assert!(positions.bound_to(GROUP, 3).is_some());
    }

    #[test]
    fn given_a_plain_consumer_when_a_peer_moves_it_should_read_there_and_count_a_seek_back() {
        let mut positions = GeoPositions::default();
        let consumer = PositionHolder::Consumer(7);
        positions.note(&consumer, group(7, Some(10)));
        assert!(positions.adopt(&consumer, offset(0, 90)));

        assert_eq!(
            positions.read_offset(ConsumerKind::Consumer, 7, Some(10)),
            Some(Some(90))
        );
        assert_eq!(
            positions.read_offset(GROUP, 7, Some(10)),
            None,
            "a group with the consumer's id is another reader"
        );
        positions.note(&consumer, group(7, Some(40)));
        assert_eq!(positions.get(&consumer).unwrap().position, offset(1, 40));
    }

    #[test]
    fn given_a_note_of_a_plain_consumer_that_names_another_reader_when_applying_should_refuse_it() {
        let mut positions = GeoPositions::default();
        let apply = |positions: &mut GeoPositions, note| {
            let payload = encode_notes(&[(PositionHolder::Consumer(7), note)]);
            positions.apply(
                1,
                MachineCommand {
                    kind: NOTE_GEO_POSITIONS,
                    payload: &payload[0],
                },
            )
        };

        assert_eq!(apply(&mut positions, group(8, Some(1))), Applied::Refused);
        assert_eq!(apply(&mut positions, GeoNote::Gone), Applied::Refused);
        assert_eq!(apply(&mut positions, group(7, Some(1))), Applied::Changed);
    }

    #[test]
    fn given_every_kind_of_holder_when_restoring_a_snapshot_should_get_back_each_binding() {
        let mut positions = GeoPositions::default();
        let external = PositionHolder::ExternalGroup("billing".to_owned());
        positions.note(&g("billing"), group(3, Some(100)));
        positions.note(&external, group(3, Some(7)));
        positions.note(&PositionHolder::Consumer(3), group(3, None));
        positions.adopt(&PositionHolder::Consumer(9), offset(2, 4));
        let mut snapshot = Vec::new();
        positions.snapshot_into(&mut snapshot);

        let mut restored = GeoPositions::default();
        restored.restore(&snapshot).unwrap();

        assert_eq!(restored.entries(), positions.entries());
        for (kind, holder) in [
            (GROUP, g("billing")),
            (ConsumerKind::ExternalGroup, external),
            (ConsumerKind::Consumer, PositionHolder::Consumer(3)),
        ] {
            assert_eq!(
                restored.bound_to(kind, 3).map(|(bound, _)| bound.clone()),
                Some(holder)
            );
        }
        assert_eq!(restored.bound_to(ConsumerKind::Consumer, 9), None);
    }

    impl GeoPositions {
        fn clone_adopt(&self, holder: &PositionHolder, position: GeoPosition) -> bool {
            let mut copy = Self {
                entries: self.entries.clone(),
                bound: self.bound.clone(),
            };
            copy.adopt(holder, position)
        }
    }
}
