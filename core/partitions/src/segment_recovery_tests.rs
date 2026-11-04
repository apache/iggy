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

use super::*;
use crate::PartitionPathLayout;
use std::fs;
use tempfile::tempdir;

#[path = "segment_recovery_fixtures.rs"]
mod fixtures;
use fixtures::*;

const HEADER_BASE_OFFSET_OFFSET: usize = 8;

fn bait_record(base_offset: u64, claimed_batch_length: u64, sequence: u32) -> Vec<u8> {
    let mut record = zero_padded_record(claimed_batch_length, sequence);
    record[HEADER_BASE_OFFSET_OFFSET..HEADER_BASE_OFFSET_OFFSET + 8]
        .copy_from_slice(&base_offset.to_le_bytes());
    record
}

#[compio::test]
async fn given_overlapping_verify_claims_when_probing_should_refuse_on_verify_budget() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Residue packed with back-to-back plausible headers, each claiming
    // a slice 32x wider than its own 256-byte pitch: claimed slices
    // overlap, so unbounded verification would hash close to residue x
    // claim bytes. The verify budget must give up and refuse -- with a
    // walked prefix this is a refusal, never a truncation -- and its
    // limit must be the residue-derived multiple, which is the guard
    // that keeps the bound from being silently deleted again.
    let mut log = encoded_batch(0, 2);
    for sequence in 0..64u32 {
        log.extend_from_slice(&bait_record(100, 8 * 1024, sequence + 1));
    }
    let residue = u64::from(64u32) * COMMAND_HEADER_SIZE as u64;
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("exhausting the verify budget over a walked prefix must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue {
                    residue_bytes,
                    verified_bytes,
                    verify_budget_bytes,
                    ..
                },
                ..
            } if *residue_bytes == residue
                && *verify_budget_bytes
                    == residue * PROBE_VERIFY_BUDGET_BYTES_PER_RESIDUE_BYTE
                && verified_bytes > verify_budget_bytes
        ),
        "expected a verify-budget refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "a refusal must leave the log byte-identical"
    );
}

#[compio::test]
async fn given_overlapping_verify_claims_with_no_walked_batch_when_probing_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Same bait with a garbage head, so the walk proves not one batch.
    // Exhaustion must still refuse: recover-as-empty would SERVE the
    // segment and re-mint offsets while a real survivor may hide past
    // the budget horizon, which is exactly the destructive verdict the
    // cheapest-to-construct residue must never earn.
    let mut log = GARBAGE.to_vec();
    for sequence in 0..64u32 {
        log.extend_from_slice(&bait_record(100, 8 * 1024, sequence + 1));
    }
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("verify-budget exhaustion must refuse even with no walked batch");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue { .. },
                ..
            }
        ),
        "expected a budget-exhausted refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "a refusal must leave the log byte-identical"
    );
}

#[compio::test]
async fn given_an_index_deeper_than_the_probe_cap_under_fsync_when_recovering_should_refuse_with_the_searched_depth()
 {
    // Entry 0 is the one entry the log does back, and it sits below the
    // cap: the search stops before reaching it and proves nothing. The
    // refusal is still the right verdict -- a step-back this deep is log
    // loss whatever a deeper probe would find -- which is exactly why it
    // must name the depth it searched instead of letting zero provable
    // entries read as "the log backs nothing".
    const OVERSHOOTING_ENTRIES: u64 = MAX_INDEX_ANCHOR_PROBE_ENTRIES + 1;
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let log = encoded_batch(0, 1);
    let mut index = index_entry(0, 0);
    for step in 0..OVERSHOOTING_ENTRIES {
        index.extend_from_slice(&index_entry(step + 1, log.len() as u64 + step));
    }
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("an index outrunning the log past the probe cap must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::FsyncedLogLoss {
                    entry_count,
                    provable_entries: 0,
                    searched_entries,
                    ..
                },
                ..
            } if *entry_count == OVERSHOOTING_ENTRIES + 1
                && *searched_entries == MAX_INDEX_ANCHOR_PROBE_ENTRIES
        ),
        "expected a capped fsynced-log-loss refusal naming its search depth, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[test]
fn probe_budget_charges_verify_bytes_before_the_read() {
    let mut budget = ProbeBudget::default();
    budget.grow_for_residue(1024);
    // 4 KiB of verify allowance: three 1 KiB slices fit, the fifth does
    // not, and the failing charge is already counted (the caller must
    // not read the slice that broke the budget).
    for _ in 0..4 {
        assert!(budget.charge_verify(1024), "in-budget verifies must pass");
    }
    assert!(
        !budget.charge_verify(1024),
        "the fifth 1 KiB slice against a 4 KiB verify budget must exhaust"
    );
    // A later probe widens the shared limit; the failed charge above
    // stays counted, so the widened budget must cover it plus the next
    // slice.
    budget.grow_for_residue(512);
    assert!(budget.charge_verify(1024));
}

#[test]
fn probe_budget_charges_per_candidate_across_probes() {
    let mut budget = ProbeBudget::default();
    budget.grow_for_residue(4);
    for _ in 0..8 {
        assert!(budget.charge_candidate(), "honest scans fit the budget");
    }
    assert!(
        !budget.charge_candidate(),
        "the ninth candidate against a 4-byte residue must exhaust"
    );
    // A later probe in the same load widens the shared limit; spent
    // units carry over rather than resetting per probe.
    budget.grow_for_residue(2);
    assert!(budget.charge_candidate());
}

#[compio::test]
async fn given_exhausted_probe_budget_when_classifying_residue_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let mut log = encoded_batch(0, 2);
    let valid_len = log.len() as u64;
    log.extend_from_slice(&GARBAGE);
    let messages_path = tmp.path().join("00000000000000000000.log");
    fs::write(&messages_path, &log).expect("write log fixture");
    let file = compio::fs::File::open(&messages_path)
        .await
        .expect("open log fixture");
    let partition_path = tmp.path().to_string_lossy().into_owned();
    let identity = PartitionIdentity {
        partition_path: &partition_path,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID,
    };
    // No once-through scan can exhaust a residue-sized budget, so the
    // exhaustion path is a tripwire for probe defects that re-examine
    // candidates. Simulate one by pre-spending the shared budget past
    // anything this residue can grow it by.
    let mut scratch = ScanScratch::default();
    scratch.probe_budget.spent_units = u64::MAX / 2;
    let mut scanner = FileScanner::new(&file, log.len() as u64, &mut scratch);

    let error = refuse_if_survivor_past_damage(
        identity,
        &mut scanner,
        &messages_path.to_string_lossy(),
        valid_len,
        log.len() as u64,
        Some(1),
        0,
    )
    .await
    .expect_err("an exhausted budget must refuse instead of truncating");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue {
                    damage_position,
                    residue_bytes,
                    candidates_examined,
                    budget_units,
                    ..
                },
                ..
            } if *damage_position == valid_len
                && *residue_bytes == GARBAGE.len() as u64
                && candidates_examined > budget_units
        ),
        "expected a budget-exhausted refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path.to_string_lossy()),
        log,
        "a refusal must leave the log byte-identical"
    );
}

#[compio::test]
async fn given_exhausted_probe_budget_with_no_walked_batch_when_classifying_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let log = GARBAGE.to_vec();
    let messages_path = tmp.path().join("00000000000000000000.log");
    fs::write(&messages_path, &log).expect("write log fixture");
    let file = compio::fs::File::open(&messages_path)
        .await
        .expect("open log fixture");
    let partition_path = tmp.path().to_string_lossy().into_owned();
    let identity = PartitionIdentity {
        partition_path: &partition_path,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID,
    };
    let mut scratch = ScanScratch::default();
    scratch.probe_budget.spent_units = u64::MAX / 2;
    let mut scanner = FileScanner::new(&file, log.len() as u64, &mut scratch);

    // A walk that proved nothing exhausts the probe over the whole file.
    // Recover-as-empty here would SERVE the segment and re-mint offsets
    // over bytes the probe never finished scanning; only refusal holds.
    let error = refuse_if_survivor_past_damage(
        identity,
        &mut scanner,
        &messages_path.to_string_lossy(),
        0,
        log.len() as u64,
        None,
        0,
    )
    .await
    .expect_err("an exhausted probe with no walked batch must refuse, not recover empty");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue { .. },
                ..
            }
        ),
        "expected a budget-exhausted refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path.to_string_lossy()), log);
}
