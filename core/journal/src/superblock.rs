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

//! Durable superblock: a small record that survives a crash intact.
//!
//! Persists the VSR state consensus cannot recover from its own disk
//! (`view`, `log_view`, `commit`). The payload is opaque here: this layer
//! makes N bytes durable and picks the latest good copy, nothing more.
//! Consensus owns their meaning.
//!
//! [`PingPongSuperblock`] writes two files alternately, higher valid
//! `sequence` wins, each replaced by writing a temp, fsyncing it, renaming it
//! over the target, then fsyncing the directory (as `PrepareJournal` does for
//! WAL compaction). Rename is atomic, so a torn write dies in the temp while
//! the other file stays an intact prior generation: an update can never destroy
//! the last good record.
//!
//! By that same atomicity, a slot holding bytes that fail verification is bit-rot
//! of a once-durable record, not an interrupted write. Its `sequence` is
//! unreadable, so it may be the NEWER generation, and reading through it would
//! hand consensus an older `view` / `log_view` than it already acted on. Such a
//! slot yields [`SuperblockContents::Unreadable`] and blocks writes, so the next
//! `write` cannot stamp a fresh sequence over the evidence.
//!
//! The record already carries the `version`, `sequence`, and
//! `parent_checksum` an N-copy in-place quorum variant would need, so that
//! migration stays contained to a second `impl SuperblockStore`.

// Every future here drives compio single-threaded file I/O and is `!Send` by
// construction, like the sibling `FileStorage`/`PrepareJournal`.
#![allow(clippy::future_not_send)]

use std::cell::Cell;
use std::hash::Hasher;
use std::io;
use std::path::{Path, PathBuf};

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};

use crate::prepare_journal::TmpFileGuard;
use twox_hash::XxHash3_64;

/// Identifies a superblock file and rejects a foreign or zeroed one. "SBLK".
const SUPERBLOCK_MAGIC: u32 = 0x5342_4C4B;
/// On-disk record format version. Bump when the framing or payload contract
/// changes; `read` rejects an unknown version rather than misparsing it.
const SUPERBLOCK_VERSION: u16 = 1;

/// Fixed framing ahead of the payload (28 bytes): magic, version, a reserved
/// half-word, sequence, parent checksum, and payload length.
const HEADER_LEN: usize = 28;
/// Trailing `XxHash3_64` over every preceding byte.
const CHECKSUM_LEN: usize = 8;
/// Smallest well-formed record (empty payload).
const MIN_RECORD_LEN: usize = HEADER_LEN + CHECKSUM_LEN;

/// Ceiling on a record's payload, bounding every allocation this module makes from
/// a length it read off disk (`PrepareJournal::MAX_ENTRY_SIZE` bounds the WAL for the
/// same reason). The only payload today is a [`consensus::VsrState`], 74 bytes now
/// that it carries the offset reservation (66 before it, a length its decode still
/// accepts); the headroom is for a payload that grows fields, not for bulk data.
/// `read_slot` treats a longer file as corrupt WITHOUT reading it, and
/// `build_record` refuses to write one, so a length this store could have
/// produced is always in bounds.
const MAX_PAYLOAD_LEN: usize = 4096;
/// Largest record `read_slot` will read into memory.
const MAX_RECORD_LEN: usize = HEADER_LEN + MAX_PAYLOAD_LEN + CHECKSUM_LEN;

/// First retry delay after a failed superblock write, doubling per failure.
///
/// Starts near the 10 ms consensus tick, so a one-off failure costs no
/// latency, and a persistent one stops re-running `atomic_replace` per tick.
pub const SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS: u64 = 10_000;
/// Ceiling on the retry delay. A replica fenced this long is not coming back without
/// operator action, but the retry must stay frequent enough to recover on its own the
/// moment the disk does.
pub const SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS: u64 = 1_000_000;
/// Caps the doubling so the shift cannot overflow before the ceiling clamps it.
pub const SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT: u64 = 8;

const FILE_A: &str = "superblock.a";
const FILE_B: &str = "superblock.b";

/// The two ping-pong slot file names under a superblock directory.
///
/// Newest wins by `sequence`. Exposed so an off-runtime reader (tests, tooling)
/// can read the slots with blocking I/O and decode them via [`decode_slots`].
pub const SLOT_FILE_NAMES: [&str; 2] = [FILE_A, FILE_B];

/// Outcome of reading the latest record from a [`SuperblockStore`].
///
/// The three states stay distinct because "never written" and "written, now
/// unreadable" demand opposite recovery responses. Collapsing them (as an
/// `Option` would) lets a lost or corrupt superblock masquerade as a fresh
/// deployment: the split-brain footgun this type exists to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuperblockContents {
    /// Every slot absent or zero-length. The only state a genuinely fresh
    /// deployment produces.
    Empty,
    /// The newest checksum-clean record of a supported version.
    Present(Vec<u8>),
    /// No slot pair this build can trust: bytes that fail verification on every
    /// copy, or one clean record beside a copy that does not verify, whose lost
    /// `sequence` may have been the newer. `version` names an unrecognized format
    /// version when that was the cause (a downgrade). Never "fresh".
    Unreadable { version: Option<u16> },
}

/// A durable store for a single small record.
///
/// `write` returns only once the record is durable, and a crash during it must
/// never destroy the record a prior `write` made durable.
pub trait SuperblockStore {
    /// Persist `payload` as the newest record. Durable on return.
    ///
    /// # Errors
    /// I/O error if the record cannot be made durable, or `InvalidInput` if
    /// `payload` exceeds `u32::MAX`.
    fn write(&self, payload: &[u8]) -> impl Future<Output = io::Result<()>>;

    /// Read the latest record. See [`SuperblockContents`].
    ///
    /// # Errors
    /// I/O error if a slot exists but cannot be read. A checksum, magic, or
    /// version failure is NOT an I/O error: it surfaces as
    /// [`SuperblockContents::Unreadable`] so the caller decides how to respond.
    fn read_latest(&self) -> impl Future<Output = io::Result<SuperblockContents>>;
}

#[derive(Clone, Copy)]
enum Slot {
    A,
    B,
}

impl Slot {
    const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    const fn file_name(self) -> &'static str {
        match self {
            Self::A => FILE_A,
            Self::B => FILE_B,
        }
    }
}

/// Two-file ping-pong superblock. See the module docs for the durability
/// argument.
pub struct PingPongSuperblock {
    dir: PathBuf,
    /// Sequence to stamp on the next `write`. Monotonic; selects the latest
    /// record on read.
    next_sequence: Cell<u64>,
    /// Slot the next `write` targets: always the one NOT holding the latest
    /// record, so an interrupted write cannot corrupt the newest good copy.
    next_slot: Cell<Slot>,
    /// Set at `open` when a slot held bytes that do not verify, which `write` then
    /// refuses to overwrite (see the `write` impl). Fixed for the store's
    /// lifetime: repair happens out of band.
    degraded: bool,
    /// Debug tripwire for the single-writer contract (see the `write` impl).
    /// Set across the `.await`, so an overlapping second writer trips the assert
    /// instead of silently tearing a slot. Absent in release builds.
    #[cfg(debug_assertions)]
    writing: Cell<bool>,
}

impl PingPongSuperblock {
    /// Open the superblock rooted at `dir`, which must already exist. Classifies
    /// both slots to resume the sequence counter and aim the next write at the
    /// staler slot. Missing files mean a fresh store.
    ///
    /// Succeeds even when a slot is unusable, so the caller reads the typed verdict
    /// from [`SuperblockStore::read_latest`] rather than a bare I/O error. Writes
    /// are refused in that state.
    ///
    /// # Errors
    /// I/O error if a slot file exists but cannot be read.
    pub async fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        Ok(Self::open_with_latest(dir).await?.0)
    }

    /// [`Self::open`] plus the latest recorded contents, from the SAME slot
    /// reads `open` already performs -- boot paths that would otherwise call
    /// `read_latest` right after `open` pay four slot reads per group
    /// instead of two.
    ///
    /// # Errors
    /// Same as [`Self::open`]: any slot read failing at the io layer.
    pub async fn open_with_latest(
        dir: impl Into<PathBuf>,
    ) -> io::Result<(Self, SuperblockContents)> {
        let dir = dir.into();
        let slot_a = read_slot(&dir.join(FILE_A)).await?;
        let slot_b = read_slot(&dir.join(FILE_B)).await?;
        let seq_a = sequence_of(&slot_a);
        let seq_b = sequence_of(&slot_b);

        // Latest sequence across both slots; a slot with no readable sequence counts
        // as 0. Sound only because `degraded` then blocks every write: such a slot
        // may hold a higher sequence, and reusing it would break the monotonicity
        // that selects the latest record.
        let latest = seq_a.unwrap_or(0).max(seq_b.unwrap_or(0));
        // Aim the next write at the slot that does NOT hold the strictly-newest
        // record, so an interrupted write cannot clobber it. A tie or a fresh
        // store targets A.
        let a_is_newest = match (seq_a, seq_b) {
            (Some(a), Some(b)) => a >= b,
            (Some(_), None) => true,
            (None, _) => false,
        };
        let next_slot = if a_is_newest { Slot::B } else { Slot::A };

        let store = Self {
            dir,
            next_sequence: Cell::new(latest + 1),
            next_slot: Cell::new(next_slot),
            degraded: has_unreadable_sequence(&slot_a) || has_unreadable_sequence(&slot_b),
            #[cfg(debug_assertions)]
            writing: Cell::new(false),
        };
        Ok((store, combine_slots(slot_a, slot_b)))
    }
}

impl SuperblockStore for PingPongSuperblock {
    async fn write(&self, payload: &[u8]) -> io::Result<()> {
        // An unverifiable slot looks stale to the aiming logic in `open`, so this
        // write would land on it and stamp `sequence = latest_valid + 1` over the
        // only evidence, leaving two clean slots whose "newest" carries the OLDER
        // payload: the regression `read_latest` refuses, laundered into durable
        // state. Refuse until the slot is repaired out of band.
        if self.degraded {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "superblock slot holds bytes that do not verify; refusing to write over it",
            ));
        }
        // Single-writer contract: callers MUST serialize `write` (the metadata
        // superblock lock does). `sequence`/`slot` are read before the
        // `atomic_replace` await and committed after it, so two overlapping
        // writers would target the same slot and could tear it while both return
        // `Ok`. The ping-pong guarantee, that the non-written slot is always an
        // intact prior generation, holds only with one write in flight.
        let sequence = self.next_sequence.get();
        let slot = self.next_slot.get();
        // Built before the first await so the `payload` borrow does not span it.
        let record = build_record(sequence, payload)?;
        // RAII, not a manual clear: a dropped write future would otherwise leave the
        // flag latched for the store's lifetime and make every later write panic on a
        // contract nobody violated, sending whoever debugs it to audit the lock.
        #[cfg(debug_assertions)]
        let _writing = WritingGuard::acquire(&self.writing);
        atomic_replace(&self.dir, slot.file_name(), record).await?;
        self.next_sequence.set(sequence + 1);
        self.next_slot.set(slot.other());
        Ok(())
    }

    async fn read_latest(&self) -> io::Result<SuperblockContents> {
        let a = read_slot(&self.dir.join(FILE_A)).await?;
        let b = read_slot(&self.dir.join(FILE_B)).await?;
        Ok(combine_slots(a, b))
    }
}

/// Holds the single-writer tripwire for one `write`, clearing it on drop so a
/// cancelled write future cannot latch it. Debug builds only.
#[cfg(debug_assertions)]
struct WritingGuard<'a>(&'a Cell<bool>);

#[cfg(debug_assertions)]
impl<'a> WritingGuard<'a> {
    fn acquire(writing: &'a Cell<bool>) -> Self {
        assert!(
            !writing.replace(true),
            "PingPongSuperblock::write called concurrently; writes must be externally serialized"
        );
        Self(writing)
    }
}

#[cfg(debug_assertions)]
impl Drop for WritingGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(bytes);
    hasher.finish()
}

fn build_record(sequence: u64, payload: &[u8]) -> io::Result<Vec<u8>> {
    // Refuse here rather than at the next boot: a record over the read ceiling would
    // be written durably and then classified corrupt, which blocks writes and refuses
    // boot over a payload the caller could have been told about synchronously.
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "superblock payload is {} bytes, over the {MAX_PAYLOAD_LEN}-byte ceiling",
                payload.len()
            ),
        ));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "superblock payload too large"))?;

    let mut record = Vec::with_capacity(MIN_RECORD_LEN + payload.len());
    record.extend_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    record.extend_from_slice(&SUPERBLOCK_VERSION.to_le_bytes());
    record.extend_from_slice(&0u16.to_le_bytes()); // reserved
    record.extend_from_slice(&sequence.to_le_bytes());
    record.extend_from_slice(&0u64.to_le_bytes()); // parent_checksum, unused by ping-pong
    record.extend_from_slice(&payload_len.to_le_bytes());
    record.extend_from_slice(payload);
    let ck = checksum(&record);
    record.extend_from_slice(&ck.to_le_bytes());
    Ok(record)
}

/// Reach the same verdict as [`PingPongSuperblock::read_latest`] from the two
/// slots' raw bytes, without a runtime.
///
/// `slot_a` / `slot_b` are the file contents read from [`SLOT_FILE_NAMES`], or
/// `None` for a missing slot. Shares the classification and selection rules with
/// the async path, so an off-runtime reader (tests, tooling) cannot conclude
/// `Present` where the server would refuse to boot.
#[must_use]
pub fn decode_slots(slot_a: Option<&[u8]>, slot_b: Option<&[u8]>) -> SuperblockContents {
    combine_slots(classify_bytes(slot_a), classify_bytes(slot_b))
}

/// One slot's contents, classified. Separates "nothing here" from "something
/// here but unusable" so [`combine_slots`] can tell `Empty` from `Unreadable`.
enum SlotClass {
    /// File missing or zero-length.
    Absent,
    /// Bytes present but unusable: too short, foreign magic, or a length/checksum
    /// failure. `version` is `Some` when the magic matched but the format version is
    /// unknown to this build, so nothing past that field can be trusted or
    /// checksum-verified; [`combine_slots`] reports that case over generic corruption,
    /// since it names a downgrade.
    Corrupt { version: Option<u16> },
    /// A checksum-clean record of the current version.
    Valid { sequence: u64, payload: Vec<u8> },
}

/// Classify one slot from raw bytes, `Absent` for a missing or empty slot.
/// Mirrors [`read_slot`]'s treatment of a zero-length file.
fn classify_bytes(bytes: Option<&[u8]>) -> SlotClass {
    match bytes {
        None | Some([]) => SlotClass::Absent,
        Some(bytes) => classify(bytes),
    }
}

/// Classify one slot's raw, non-empty bytes: framing, version, payload length and
/// checksum in one pass, so no slot is parsed twice.
fn classify(bytes: &[u8]) -> SlotClass {
    let corrupt = SlotClass::Corrupt { version: None };
    // Too short to hold even the framing, or a foreign file: unusable bytes.
    if bytes.len() < MIN_RECORD_LEN {
        return corrupt;
    }
    if u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) != SUPERBLOCK_MAGIC {
        return corrupt;
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SUPERBLOCK_VERSION {
        // Magic matched, so a superblock writer produced this, but the framing past
        // the version field may differ, so neither the payload length nor the checksum
        // can validate it.
        return SlotClass::Corrupt {
            version: Some(version),
        };
    }

    let payload_len = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]) as usize;
    // `decode_slots` is public and takes bytes straight from a caller, so bound the
    // length field here too, not only via `read_slot`'s file-size ceiling.
    if payload_len > MAX_PAYLOAD_LEN {
        return corrupt;
    }
    let checksum_start = HEADER_LEN + payload_len;
    if bytes.len() != checksum_start + CHECKSUM_LEN {
        return corrupt;
    }
    let Ok(stored) = bytes[checksum_start..checksum_start + CHECKSUM_LEN].try_into() else {
        return corrupt;
    };
    if checksum(&bytes[..checksum_start]) != u64::from_le_bytes(stored) {
        return corrupt;
    }

    SlotClass::Valid {
        sequence: u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]),
        payload: bytes[HEADER_LEN..checksum_start].to_vec(),
    }
}

/// Combine the two slots into one outcome.
///
/// Two valid records: higher `sequence` wins. One valid record beside an absent
/// slot: that record, what a single write leaves behind. `Empty` only when both
/// slots are absent, the one state a fresh deployment produces.
///
/// Everything else refuses boot with `Unreadable`, including a valid record beside
/// a slot that holds bytes but does not verify. Such a slot is bit-rot, not a torn
/// write (rename is atomic), and its `sequence` is gone, so it cannot be shown to
/// be the older of the two. Falling back would walk `view` / `log_view` backwards
/// and let this replica act twice in one view. An unrecognized version is reported
/// over generic corruption, since it names a downgrade.
///
/// Two copies cannot do better: with no readable sequence the choice is refuse or
/// risk regression. An N-copy quorum could repair in place instead, which the
/// record's `sequence` and `parent_checksum` framing already allows for.
fn combine_slots(a: SlotClass, b: SlotClass) -> SuperblockContents {
    match (a, b) {
        (
            SlotClass::Valid {
                sequence: seq_a,
                payload: payload_a,
            },
            SlotClass::Valid {
                sequence: seq_b,
                payload: payload_b,
            },
        ) => SuperblockContents::Present(if seq_a >= seq_b { payload_a } else { payload_b }),
        (SlotClass::Valid { payload, .. }, SlotClass::Absent)
        | (SlotClass::Absent, SlotClass::Valid { payload, .. }) => {
            SuperblockContents::Present(payload)
        }
        (SlotClass::Absent, SlotClass::Absent) => SuperblockContents::Empty,
        (
            SlotClass::Corrupt {
                version: Some(version),
            },
            _,
        )
        | (
            _,
            SlotClass::Corrupt {
                version: Some(version),
            },
        ) => SuperblockContents::Unreadable {
            version: Some(version),
        },
        _ => SuperblockContents::Unreadable { version: None },
    }
}

/// The slot's `sequence`, or `None` when it holds no record this build can read.
const fn sequence_of(slot: &SlotClass) -> Option<u64> {
    match slot {
        SlotClass::Valid { sequence, .. } => Some(*sequence),
        SlotClass::Absent | SlotClass::Corrupt { .. } => None,
    }
}

/// Whether the slot holds bytes whose generation is unknowable, so it cannot be
/// ruled out as the newer. An absent slot never held a record to lose, so it is
/// not one of these.
const fn has_unreadable_sequence(slot: &SlotClass) -> bool {
    matches!(slot, SlotClass::Corrupt { .. })
}

async fn read_slot(path: &Path) -> io::Result<SlotClass> {
    let file = match compio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(SlotClass::Absent),
        Err(e) => return Err(e),
    };
    let len = file.metadata().await?.len();
    if len == 0 {
        return Ok(SlotClass::Absent);
    }
    // Bound the allocation on `st_size` before making it. A file longer than a
    // well-formed record cannot be one: `classify` demands an exact length, so
    // reading it would spend the allocation only to conclude "corrupt". The realistic
    // producer is a foreign or leftover file, not bit-rot, since the value is the
    // file's size rather than a field inside it.
    let len = match usize::try_from(len) {
        Ok(len) if len <= MAX_RECORD_LEN => len,
        _ => return Ok(SlotClass::Corrupt { version: None }),
    };
    let buf = vec![0u8; len];
    let (result, buf) = file.read_exact_at(buf, 0).await.into();
    result?;
    // A checksum/magic/version failure is not an I/O error: the other slot may
    // still be good, so classify the bytes and let `combine_slots` decide.
    Ok(classify(&buf))
}

/// Replace `dir/file_name` atomically: write a temp, fsync it, rename over the
/// target, then fsync the directory so the rename itself is durable.
async fn atomic_replace(dir: &Path, file_name: &str, bytes: Vec<u8>) -> io::Result<()> {
    let tmp_path = dir.join(format!("{file_name}.tmp"));
    let final_path = dir.join(file_name);

    // Unlink the temp on any failure before the rename. Not a correctness matter,
    // since `File::create` truncates and `next_sequence` / `next_slot` stay
    // un-advanced so a retry re-targets the same slot; it keeps a failing disk from
    // littering `superblock.{a,b}.tmp` next to the slots an operator is inspecting.
    let guard = TmpFileGuard::new(tmp_path.clone());
    let mut tmp = compio::fs::File::create(&tmp_path).await?;
    let (result, _buf) = tmp.write_all_at(bytes, 0).await.into();
    result?;
    tmp.sync_all().await?;

    compio::fs::rename(&tmp_path, &final_path).await?;
    guard.defuse();

    let dir_file = compio::fs::File::open(dir).await?;
    dir_file.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// The payload when present, else `None`, so round-trip assertions can
    /// compare against the written bytes.
    fn present(contents: SuperblockContents) -> Option<Vec<u8>> {
        match contents {
            SuperblockContents::Present(payload) => Some(payload),
            SuperblockContents::Empty | SuperblockContents::Unreadable { .. } => None,
        }
    }

    /// Fresh store, ping-pong alternation, and sequence resume across a reopen, in
    /// one pass: `Empty` before any write, the newer payload winning after each,
    /// and both slot files present once two writes have landed.
    #[compio::test]
    async fn given_reopen_when_write_should_alternate_slots_and_advance() {
        let dir = tempdir().unwrap();
        {
            let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
            assert_eq!(sb.read_latest().await.unwrap(), SuperblockContents::Empty);
            sb.write(b"first").await.unwrap();
        }
        // Reopen: sequence resumes and the next write targets the staler slot.
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        assert_eq!(
            present(sb.read_latest().await.unwrap()).as_deref(),
            Some(&b"first"[..])
        );
        sb.write(b"second").await.unwrap();
        assert_eq!(
            present(sb.read_latest().await.unwrap()).as_deref(),
            Some(&b"second"[..])
        );
        sb.write(b"third").await.unwrap();
        assert_eq!(
            present(sb.read_latest().await.unwrap()).as_deref(),
            Some(&b"third"[..])
        );

        // Writes alternate slots, so both files exist.
        assert!(dir.path().join(FILE_A).exists());
        assert!(dir.path().join(FILE_B).exists());
    }

    #[compio::test]
    async fn given_one_slot_corrupt_when_read_latest_should_refuse_boot() {
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        sb.write(b"first").await.unwrap(); // slot A, seq 1
        sb.write(b"second").await.unwrap(); // slot B, seq 2 (newer)

        // Corrupt the newer slot's payload byte so its checksum fails.
        let path = dir.path().join(FILE_B);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        // Falling back to slot A would serve `first`, a generation consensus already
        // moved past. Nothing proves the corrupt slot was the older one, so refuse.
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable { version: None }
        );
    }

    #[compio::test]
    async fn given_corrupt_slot_when_reopen_should_refuse_write_and_keep_both_slots() {
        // `open` aims the next write at the slot that looks stale, which is the
        // corrupt one. Writing there would stamp `sequence = 2` over it, leaving two
        // clean slots whose newest carries `first` and laundering the regression
        // `read_latest` just refused. Writes stay blocked instead.
        let dir = tempdir().unwrap();
        {
            let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
            sb.write(b"first").await.unwrap(); // slot A, seq 1
            sb.write(b"second").await.unwrap(); // slot B, seq 2
        }
        let path_b = dir.path().join(FILE_B);
        let mut bytes = std::fs::read(&path_b).unwrap();
        bytes[HEADER_LEN] ^= 0xFF;
        std::fs::write(&path_b, &bytes).unwrap();
        let before_a = std::fs::read(dir.path().join(FILE_A)).unwrap();
        let before_b = std::fs::read(&path_b).unwrap();

        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable { version: None }
        );
        let error = sb.write(b"third").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        assert_eq!(
            std::fs::read(dir.path().join(FILE_A)).unwrap(),
            before_a,
            "the surviving record must be left untouched for out-of-band repair"
        );
        assert_eq!(
            std::fs::read(&path_b).unwrap(),
            before_b,
            "the corrupt slot is the only evidence of the lost generation"
        );
    }

    #[compio::test]
    async fn given_both_slots_corrupt_when_read_latest_should_return_unreadable() {
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        sb.write(b"first").await.unwrap();
        sb.write(b"second").await.unwrap();

        for name in [FILE_A, FILE_B] {
            let path = dir.path().join(name);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[HEADER_LEN] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }

        // Bytes on both slots, neither verifies: a torn superblock, NOT a fresh
        // deployment. Recovery must refuse boot.
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable { version: None }
        );
    }

    #[compio::test]
    async fn given_unsupported_version_when_read_latest_should_report_version() {
        // Magic matches but the format version is unknown: a downgrade, or a
        // corrupt version field. Version is checked before the checksum, so
        // patching it alone suffices.
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        sb.write(b"payload").await.unwrap(); // slot A, current version

        let path = dir.path().join(FILE_A);
        let mut bytes = std::fs::read(&path).unwrap();
        let bogus = SUPERBLOCK_VERSION + 1;
        bytes[4..6].copy_from_slice(&bogus.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable {
                version: Some(bogus)
            }
        );

        // A clean record beside it does not rescue the read: this build cannot
        // sequence the unrecognized slot, so that slot may be the newer generation.
        sb.write(b"newer").await.unwrap(); // slot B, current version
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable {
                version: Some(bogus)
            }
        );
    }

    #[compio::test]
    async fn given_two_writes_when_decode_slots_from_disk_should_match_read_latest() {
        // The off-runtime decoder must reach the same verdict the async read path
        // does: newer sequence wins across the two slots.
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        sb.write(b"first").await.unwrap();
        sb.write(b"second").await.unwrap();

        let read_slots = || {
            let slot_a = std::fs::read(dir.path().join(SLOT_FILE_NAMES[0])).ok();
            let slot_b = std::fs::read(dir.path().join(SLOT_FILE_NAMES[1])).ok();
            decode_slots(slot_a.as_deref(), slot_b.as_deref())
        };
        assert_eq!(read_slots(), sb.read_latest().await.unwrap());
        assert_eq!(
            read_slots(),
            SuperblockContents::Present(b"second".to_vec())
        );

        // And it must refuse where the server would, not read through a corrupt slot
        // to the older record.
        let path = dir.path().join(SLOT_FILE_NAMES[1]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        assert_eq!(read_slots(), sb.read_latest().await.unwrap());
        assert_eq!(
            read_slots(),
            SuperblockContents::Unreadable { version: None }
        );
    }

    // Two `write`s interleaving across the `atomic_replace` await read the same
    // `(sequence, slot)` and would tear that slot while both report success.
    // Production serializes writes behind the metadata superblock lock; this
    // proves the debug tripwire catches a bypass instead of corrupting a slot.
    // Compiled out of release builds.
    #[cfg(debug_assertions)]
    #[compio::test]
    #[should_panic(expected = "called concurrently")]
    async fn given_overlapping_writes_when_write_should_trip_single_writer_guard() {
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        // Polling both together parks the first on its `atomic_replace` await
        // holding the writer flag, so the second trips the assert on the same poll.
        let (first, second) = futures::future::join(sb.write(b"first"), sb.write(b"second")).await;
        let _ = (first, second);
    }

    // A dropped write future must not latch the tripwire: the flag is a
    // single-writer contract check, and leaving it set would make every later write
    // panic on a violation that never happened.
    #[cfg(debug_assertions)]
    #[compio::test]
    async fn given_dropped_write_future_when_writing_again_should_not_trip_guard() {
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        {
            let mut write = Box::pin(sb.write(b"cancelled"));
            // One poll parks it inside `atomic_replace` with the flag held, then the
            // future is dropped without ever completing.
            assert!(
                futures::poll!(&mut write).is_pending(),
                "the write must park on file I/O for this to model a cancellation"
            );
        }

        sb.write(b"after").await.unwrap();
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Present(b"after".to_vec())
        );
    }

    #[compio::test]
    async fn given_oversized_payload_when_write_should_refuse() {
        // Refused synchronously rather than written and then classified corrupt on the
        // next boot, which would block writes and refuse boot over a payload the
        // caller could have been told about here.
        let dir = tempdir().unwrap();
        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        let error = sb
            .write(&vec![0u8; MAX_PAYLOAD_LEN + 1])
            .await
            .expect_err("a payload over the ceiling must be refused");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Empty,
            "a refused write must leave the store untouched"
        );
    }

    #[compio::test]
    async fn given_oversized_slot_file_when_read_latest_should_refuse_without_reading_it() {
        // A file longer than a well-formed record cannot be one (`classify` demands
        // an exact length), so it is classified from its size alone. The verdict must
        // still be the fail-closed one: a foreign file in a slot is not a fresh
        // deployment.
        let dir = tempdir().unwrap();
        let path = dir.path().join(SLOT_FILE_NAMES[0]);
        let mut bytes = Vec::with_capacity(MAX_RECORD_LEN + 1);
        bytes.extend_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
        bytes.resize(MAX_RECORD_LEN + 1, 0);
        std::fs::write(&path, &bytes).unwrap();

        let sb = PingPongSuperblock::open(dir.path()).await.unwrap();
        assert_eq!(
            sb.read_latest().await.unwrap(),
            SuperblockContents::Unreadable { version: None }
        );
        assert!(
            sb.write(b"payload").await.is_err(),
            "a slot whose generation is unknowable must block writes"
        );
    }
}
