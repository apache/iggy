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

//! Receiver-side state-transfer session math shared by both planes.
//!
//! A transfer pulls the artifacts named by a [`crate::StateArtifact`]
//! manifest in manifest order, lockstep with one chunk in flight; each
//! artifact's `buf.len()` doubles as the next request offset. The functions
//! here are the pure parts of that session -- chunk cursor arithmetic and the
//! frame-acceptance guards -- so the metadata and partition receivers cannot
//! drift on the invariants that were review findings on the metadata plane
//! (sequential offsets, overrun refusal, the zero-byte-payload livelock).

use crate::state_manifest::{StateArtifact, state_artifact_checksum};

/// Stall rounds a receiver spends on ONE peer before abandoning the
/// transfer and falling back to journal repair.
///
/// The retry has no peer re-selection, so this is what keeps a peer that
/// died mid-transfer from wedging the rejoining node; repair then re-picks
/// a target and re-arms a transfer if the gap is still below the new peer's
/// retained floor.
pub const STATE_TRANSFER_MAX_STALL_RETRIES: u32 = 5;

/// Decode-failure rounds a receiver spends on ONE offered generation
/// before refusing to pull it again.
///
/// Read by the METADATA arm only -- it lives here because the chunk cursor it
/// pairs with is plane-agnostic, not because both planes use it.
///
/// Keyed on `snapshot_seq`: a peer whose snapshot
/// generation advances resets the budget (new bytes are worth full
/// retries), while a generation this build cannot decode costs one refused
/// descriptor per repair round instead of a full pull. The partition plane
/// deliberately does NOT use a generation-keyed budget -- a committing
/// origin advances `commit_op` every round, which pinned such a count at
/// 1 forever; it counts consecutive failures on the partition and backs
/// its re-arm off instead.
pub const STATE_TRANSFER_MAX_DECODE_RETRIES: u32 = 5;

/// One artifact of an accepted transfer target: its manifest entry plus
/// the bytes received so far.
///
/// Chunks are sequential, so `buf.len()` doubles as the next request offset. A receiver that spills completed artifacts to
/// disk empties `buf` afterwards and tracks completion out of band.
#[derive(Debug)]
pub struct ArtifactProgress {
    pub entry: StateArtifact,
    pub buf: Vec<u8>,
}

/// What [`next_pending_chunk`] and [`append_chunk`] need from one slot.
///
/// Exists so a plane can track completion in richer shapes -- the
/// partition receiver spills finished segments to disk and replaces the
/// buffer with staged metadata -- while both planes share one chunk cursor:
/// a spilled artifact simply reports itself complete and is skipped.
pub trait ChunkProgress {
    fn declared_len(&self) -> u64;
    fn received_len(&self) -> u64;
    /// Only called after the cursor checks `received + payload <= declared`,
    /// so an impl whose slot cannot grow (already complete) never sees it.
    fn extend_from_chunk(&mut self, payload: &[u8]);
    /// Reserve room for the whole declared length, called once per artifact on
    /// its FIRST chunk (see [`append_chunk`]). Default: nothing, for slots that
    /// do not accumulate in memory. Reserving at accept time instead would
    /// commit address space for every manifest entry at once.
    fn reserve_declared(&mut self) {}
    fn complete(&self) -> bool {
        self.received_len() == self.declared_len()
    }
}

impl ChunkProgress for ArtifactProgress {
    fn declared_len(&self) -> u64 {
        self.entry.len
    }

    fn received_len(&self) -> u64 {
        self.buf.len() as u64
    }

    fn extend_from_chunk(&mut self, payload: &[u8]) {
        self.buf.extend_from_slice(payload);
    }

    fn reserve_declared(&mut self) {
        // Exact rather than geometric: `entry.len` already passed the caller's
        // per-kind caps, and doubling to gigabyte sizes copies roughly twice the
        // bytes at a ~1.5x transient peak.
        #[allow(clippy::cast_possible_truncation)]
        self.buf.reserve_exact(self.entry.len as usize);
    }
}

/// Next `(artifact index, offset, len)` to request.
///
/// The first incomplete artifact in manifest order, asked from its current
/// frontier, clamped to `chunk_len_max`. `None` when every artifact is complete (or the manifest
/// is empty).
#[must_use]
pub fn next_pending_chunk<T: ChunkProgress>(
    artifacts: &[T],
    chunk_len_max: u64,
) -> Option<(u32, u64, u32)> {
    let (index, artifact) = artifacts
        .iter()
        .enumerate()
        .find(|(_, artifact)| !artifact.complete())?;
    let offset = artifact.received_len();
    let remaining = artifact.declared_len() - offset;
    #[allow(clippy::cast_possible_truncation)]
    let len = remaining.min(chunk_len_max) as u32;
    #[allow(clippy::cast_possible_truncation)]
    Some((index as u32, offset, len))
}

/// Append one received chunk; `true` only when bytes actually landed, which
/// is the caller's cue to reset its liveness counters and re-drive progress.
///
/// Everything else is dropped without side effects: an artifact that is not
/// the FIRST incomplete one, a non-sequential offset (chunks are pulled
/// lockstep, so anything else is a duplicate or reorder -- the stall retry
/// re-requests from the current frontier), an overrun past the declared
/// length, and a zero-byte payload. The first-incomplete restriction mirrors
/// what [`next_pending_chunk`] would have requested, and bounds the
/// reservation below to ONE artifact at a time: without it a peer that pushes
/// one byte into every manifest entry would make each slot reserve its whole
/// declared length, committing address space for the sum of the manifest, and
/// a failed `Vec` reservation aborts the process rather than erroring. A
/// zero-byte payload is not progress: it extends nothing and the same offset is re-requested
/// immediately, and resetting liveness counters on one is what turned a
/// short rebuilt offer into an unbounded empty-frame ping-pong on the
/// metadata plane. The serving side refuses to produce these now; the guard
/// stays because a peer running an older build still can.
#[must_use]
pub fn append_chunk<T: ChunkProgress>(
    artifacts: &mut [T],
    artifact_index: u32,
    offset: u64,
    payload: &[u8],
) -> bool {
    let first_incomplete = artifacts.iter().position(|artifact| !artifact.complete());
    if first_incomplete != Some(artifact_index as usize) {
        return false;
    }
    let Some(artifact) = artifacts.get_mut(artifact_index as usize) else {
        return false;
    };
    if offset != artifact.received_len() {
        return false;
    }
    if artifact.received_len() + payload.len() as u64 > artifact.declared_len() {
        tracing::warn!(
            artifact = artifact_index,
            declared_len = artifact.declared_len(),
            "state chunk overruns the declared artifact length; dropping frame"
        );
        return false;
    }
    if payload.is_empty() {
        return false;
    }
    // First chunk of this artifact: give the slot its full declared length in
    // one allocation, so a segment-sized artifact is not grown by doubling.
    if artifact.received_len() == 0 {
        artifact.reserve_declared();
    }
    artifact.extend_from_chunk(payload);
    true
}

/// Whether `bytes` is exactly the artifact the manifest promised:
/// declared length and `XxHash3_64` checksum.
///
/// This proves transit integrity only -- the payload still needs its own
/// format validation, since the checksum does not prove the peer computed
/// it over sane bytes.
#[must_use]
pub fn verify_state_artifact(entry: &StateArtifact, bytes: &[u8]) -> bool {
    bytes.len() as u64 == entry.len && state_artifact_checksum(bytes) == entry.checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(kind: u8, len: u64) -> ArtifactProgress {
        ArtifactProgress {
            entry: StateArtifact {
                kind,
                frontier: 0,
                len,
                checksum: 0,
            },
            buf: Vec::new(),
        }
    }

    #[test]
    fn given_partial_artifacts_when_next_chunk_requested_should_resume_first_incomplete() {
        let mut artifacts = vec![progress(0, 4), progress(1, 10)];
        artifacts[0].buf = vec![0; 4];
        artifacts[1].buf = vec![0; 3];

        assert_eq!(next_pending_chunk(&artifacts, 5), Some((1, 3, 5)));
    }

    #[test]
    fn given_short_tail_when_next_chunk_requested_should_clamp_to_remaining() {
        let mut artifacts = vec![progress(0, 8)];
        artifacts[0].buf = vec![0; 6];

        assert_eq!(next_pending_chunk(&artifacts, 64), Some((0, 6, 2)));
    }

    #[test]
    fn given_complete_or_empty_manifest_when_next_chunk_requested_should_yield_none() {
        assert_eq!(next_pending_chunk::<ArtifactProgress>(&[], 64), None);
        let mut artifacts = vec![progress(0, 2)];
        artifacts[0].buf = vec![0; 2];
        assert_eq!(next_pending_chunk(&artifacts, 64), None);
    }

    #[test]
    fn given_zero_length_artifact_when_scanned_should_read_complete() {
        // A zero-length artifact must never be asked for: `start >= len` is
        // the empty-chunk exchange the serving side refuses.
        let artifacts = vec![progress(0, 0), progress(1, 1)];
        assert_eq!(next_pending_chunk(&artifacts, 64), Some((1, 0, 1)));
    }

    #[test]
    fn given_sequential_chunks_when_appended_should_accumulate() {
        let mut artifacts = vec![progress(0, 4)];
        assert!(append_chunk(&mut artifacts, 0, 0, b"ab"));
        assert!(append_chunk(&mut artifacts, 0, 2, b"cd"));
        assert!(artifacts[0].complete());
    }

    #[test]
    fn given_bad_frames_when_appended_should_drop_without_side_effects() {
        let mut artifacts = vec![progress(0, 4)];
        assert!(append_chunk(&mut artifacts, 0, 0, b"ab"));

        assert!(!append_chunk(&mut artifacts, 1, 0, b"xx"), "index OOB");
        assert!(!append_chunk(&mut artifacts, 0, 0, b"xx"), "stale offset");
        assert!(!append_chunk(&mut artifacts, 0, 3, b"xx"), "future offset");
        assert!(!append_chunk(&mut artifacts, 0, 2, b"xyz"), "overrun");
        assert!(!append_chunk(&mut artifacts, 0, 2, b""), "empty payload");
        assert_eq!(artifacts[0].buf, b"ab", "rejected frames must not mutate");
    }

    #[test]
    fn given_artifact_bytes_when_verified_should_match_len_and_checksum() {
        let bytes = b"state transfer artifact".to_vec();
        let entry = StateArtifact::for_bytes(2, 7, &bytes);
        assert!(verify_state_artifact(&entry, &bytes));

        let mut flipped = bytes.clone();
        flipped[0] ^= 1;
        assert!(!verify_state_artifact(&entry, &flipped));
        assert!(!verify_state_artifact(&entry, &bytes[1..]));
    }
}
