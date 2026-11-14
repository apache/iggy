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

//! Body entries of the batched `Commit` and `CommitReply` frames.
//!
//! A frame carries `cluster`, the sending node and the entry count once in its
//! header. Its body is `entry_count` fixed-size entries, one per group.

use super::header::HEADER_SIZE;
use bytemuck::{Pod, Zeroable};

/// The largest frame of one batch: 64 MiB.
pub const COMMIT_BATCH_FRAME_MAX: usize = 1 << 26;

/// Most `Commit` entries one frame holds.
pub const COMMIT_BATCH_ENTRIES_MAX: usize =
    (COMMIT_BATCH_FRAME_MAX - HEADER_SIZE) / size_of::<CommitEntry>();

/// Most `CommitReply` entries one frame holds. Not below
/// [`COMMIT_BATCH_ENTRIES_MAX`], so a node that fits its Commits in one frame
/// also fits its replies.
pub const COMMIT_REPLY_BATCH_ENTRIES_MAX: usize =
    (COMMIT_BATCH_FRAME_MAX - HEADER_SIZE) / size_of::<CommitReplyEntry>();

const _: () = assert!(COMMIT_REPLY_BATCH_ENTRIES_MAX >= COMMIT_BATCH_ENTRIES_MAX);

/// Why a primary sent a `Commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HeartbeatState {
    /// A heartbeat of a busy group. The backups wait the busy timeout.
    Busy = 0,
    /// A heartbeat of an idle group. The backups wait the idle timeout.
    Idle = 1,
    /// A `Commit` sent right after the commit point moved.
    Eager = 2,
}

impl TryFrom<u8> for HeartbeatState {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Busy),
            1 => Ok(Self::Idle),
            2 => Ok(Self::Eager),
            unknown => Err(unknown),
        }
    }
}

/// One group's `Commit` in a batch. 40 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct CommitEntry {
    pub group: u64,
    pub partition_incarnation: u64,
    pub commit: u64,
    pub timestamp_monotonic: u64,
    pub view: u32,
    /// A [`HeartbeatState`]. Raw, so the receiver rejects an unknown value in
    /// this entry alone.
    pub heartbeat_state: u8,
    /// Member index of the sender in this group.
    pub replica: u8,
    pub reserved: [u8; 2],
}
const _: () = assert!(size_of::<CommitEntry>() == 40);

impl CommitEntry {
    #[must_use]
    pub const fn new(
        group: u64,
        partition_incarnation: u64,
        view: u32,
        commit: u64,
        timestamp_monotonic: u64,
        heartbeat_state: HeartbeatState,
        replica: u8,
    ) -> Self {
        Self {
            group,
            partition_incarnation,
            commit,
            timestamp_monotonic,
            view,
            heartbeat_state: heartbeat_state as u8,
            replica,
            reserved: [0; 2],
        }
    }

    /// # Errors
    /// The raw byte when it names no [`HeartbeatState`].
    pub fn heartbeat_state(&self) -> Result<HeartbeatState, u8> {
        HeartbeatState::try_from(self.heartbeat_state)
    }
}

/// One group's `CommitReply` in a batch. 32 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct CommitReplyEntry {
    pub group: u64,
    pub partition_incarnation: u64,
    pub timestamp_monotonic: u64,
    pub view: u32,
    /// Member index of the sender in this group.
    pub replica: u8,
    pub reserved: [u8; 3],
}
const _: () = assert!(size_of::<CommitReplyEntry>() == 32);

impl CommitReplyEntry {
    #[must_use]
    pub const fn new(
        group: u64,
        partition_incarnation: u64,
        view: u32,
        timestamp_monotonic: u64,
        replica: u8,
    ) -> Self {
        Self {
            group,
            partition_incarnation,
            timestamp_monotonic,
            view,
            replica,
            reserved: [0; 3],
        }
    }
}

/// The entries of a batch body. A trailing partial entry is ignored, so
/// callers check the size against the header count first.
pub fn batch_entries<E: Pod>(body: &[u8]) -> impl Iterator<Item = E> + '_ {
    body.chunks_exact(size_of::<E>())
        .map(bytemuck::pod_read_unaligned)
}

/// Frame size of a batch of `count` entries of type `E`, `None` above the
/// frame limit.
#[must_use]
pub fn batch_frame_size<E>(count: usize) -> Option<u32> {
    let size = count
        .checked_mul(size_of::<E>())?
        .checked_add(HEADER_SIZE)?;
    if size > COMMIT_BATCH_FRAME_MAX {
        return None;
    }
    u32::try_from(size).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_every_state_byte_when_decoding_should_accept_only_the_three_states() {
        for byte in 0..=u8::MAX {
            let state = HeartbeatState::try_from(byte);
            match byte {
                0..=2 => assert_eq!(state.map(|state| state as u8), Ok(byte)),
                _ => assert_eq!(state, Err(byte)),
            }
        }
    }

    #[test]
    fn given_entries_when_encoded_should_round_trip_through_the_body() {
        let entries = [
            CommitEntry::new(1, 2, 3, 4, 5, HeartbeatState::Idle, 1),
            CommitEntry::new(6, 0, 7, 8, 9, HeartbeatState::Eager, 2),
        ];
        let body: Vec<u8> = bytemuck::cast_slice(&entries).to_vec();
        let decoded: Vec<CommitEntry> = batch_entries(&body).collect();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn given_the_frame_limit_when_sizing_should_refuse_one_entry_past_it() {
        assert!(batch_frame_size::<CommitEntry>(COMMIT_BATCH_ENTRIES_MAX).is_some());
        assert!(batch_frame_size::<CommitEntry>(COMMIT_BATCH_ENTRIES_MAX + 1).is_none());
        assert_eq!(COMMIT_BATCH_ENTRIES_MAX, 1_677_715);
    }
}
