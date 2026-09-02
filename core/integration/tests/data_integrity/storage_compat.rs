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

//! On-disk format backwards compatibility.
//!
//! A server built from the master tip the change merges onto (the BASELINE)
//! writes a rich data directory. The same directory is then restarted under
//! the build under test, and everything must read back unchanged:
//!
//! - A checkpointed metadata snapshot holding topics with every option key,
//!   partitions with a deletion watermark and a purge generation, consumer
//!   groups, users with permissions and personal access tokens, all created
//!   BEFORE the checkpoint fires, plus an uncheckpointed WAL tail holding one
//!   more of each, created after it. The snapshot and the WAL encode the same
//!   types differently, and either can break on its own.
//! - Sealed and active segments in a non-zero partition, holding messages
//!   with explicit ids, typed user headers and payload lengths that vary
//!   inside every batch, and a partial segment deletion.
//! - Individual and group consumer offsets, and a purge generation with
//!   messages appended past it.
//!
//! The swap works because `ServerHandle::start` re-reads
//! `config.executable_path` on every call while the data directory, the
//! ports and the harness paths all survive a `stop`.
//!
//! # Why every assertion here is about DATA
//!
//! Three failure modes reach exit code 0 with a server that boots and serves:
//!
//! - A partition whose durable state cannot be read is TOMBSTONED and boot
//!   continues. Only the log says so.
//! - A topic loses a single storage option, per key, with no log line at
//!   all: `TopicCreateOptions::parse_committed` deliberately discards
//!   per-entry parse errors so one unreadable key cannot drop the rest.
//! - A segment misparse can truncate a torn tail while the messages a test
//!   happens to poll still read back fine, because segment batch headers
//!   carry no magic and no version.
//!
//! So the test asserts the tombstone marker is absent, re-reads every seeded
//! option value AND its provenance flag, compares every stored message field
//! against a read taken from the baseline, and byte-compares the segment
//! files across the swap. It stops producing MID-segment on purpose: boot
//! unseals the last segment, so a chain that ends on a rotation boundary
//! would only ever hand that path an empty file.
//!
//! # Structural false positive to avoid
//!
//! The harness forwards every parent `IGGY_*` variable to the server child,
//! and the server treats an unknown `IGGY_*` name as a `debug_assert`. A pull
//! request that adds a config leaf AND a test that sets it therefore makes
//! the BASELINE binary die on startup, which looks like a compatibility
//! break. Nothing here detects that: `resolve_config_paths` validates the
//! name against the catalog of the build under test, which is the wrong side
//! of the swap, so it only proves the name exists on HEAD. Keeping every
//! override to a name that already exists on the merge base stays a rule the
//! author has to follow by hand.
//!
//! `IGGY_TEST_VERBOSE` makes the harness inherit the server's stdout instead
//! of capturing it, which would make the tombstone check vacuous. The
//! graceful-shutdown assertion below is taken on the same log and fails
//! loudly in that case rather than passing on an empty file.

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{
    TestHarness, TestServerConfig, USER_PASSWORD, disk, resolve_config_paths,
};
use serial_test::parallel;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Absolute path to the baseline `iggy-server` binary. See
/// [`baseline_server_binary`] for why a relative value is refused.
///
/// NOT `IGGY_`-prefixed on purpose: the harness forwards every parent
/// `IGGY_*` variable to the server child, where an unknown name trips a
/// `debug_assert` and kills the debug build this test runs against.
const BASELINE_SERVER_ENV: &str = "COMPAT_BASELINE_SERVER";

/// `metadata.journal_slots` floor for `prepare_queue_depth = 32`
/// (`4 * max(64, 32)`). With the 64-op checkpoint margin a checkpoint fires
/// once the journal holds 192 committed ops.
const JOURNAL_SLOTS: &str = "256";

/// Committed metadata ops between forced checkpoints at [`JOURNAL_SLOTS`].
const CHECKPOINT_EVERY: u32 = 192;

/// Stream creates that drive the metadata plane past its first checkpoint.
/// Everything seeded before them lands in the snapshot.
const SEED_STREAMS: u32 = 250;

/// Stream creates issued after the checkpoint, so recovery must fold a
/// snapshot AND replay a WAL suffix on top of it.
const WAL_TAIL_STREAMS: u32 = 5;

const DATA_STREAM: &str = "compat-data";
const DATA_TOPIC: &str = "data";
/// Created after the checkpoint, so its options only exist as a WAL record.
const WAL_TAIL_TOPIC: &str = "tail";
const PURGE_STREAM: &str = "compat-purge";
const PURGE_TOPIC: &str = "purged";
const OFFSET_CONSUMER: &str = "compat-offset-consumer";
const READBACK_CONSUMER: &str = "compat-readback";
const OFFSET_GROUP: &str = "compat-offset-group";
const TRANSIENT_GROUP: &str = "compat-transient-group";
const COMPAT_USER: &str = "compat-user";
const COMPAT_PAT: &str = "compat-pat";
const WAL_TAIL_USER: &str = "compat-tail-user";
const WAL_TAIL_PAT: &str = "compat-tail-pat";

/// Partitions per seeded topic. Non-default (the default is 1), so the count
/// is itself a recovered value, and more than one so [`SEEDED_PARTITION`]
/// can be non-zero.
const PARTITIONS_COUNT: u32 = 2;

/// The partition every message, offset and purge check targets. Non-zero on
/// purpose: the segment batch header carries the partition id, and a field
/// that decoded as 0 is indistinguishable from a correct one when the only
/// seeded partition is 0. Partition 0 stays empty.
const SEEDED_PARTITION: u32 = 1;

/// Exactly `MIN_TOPIC_SEGMENT_SIZE`, and a 512-byte multiple, so it passes
/// create-time validation while keeping the produced data small.
const SEGMENT_SIZE_BYTES: u64 = 1024 * 1024;

/// Byte flush threshold. Together with `messages_required_to_save = 1` it
/// forces every committed message onto a segment file, instead of leaving it
/// in the in-memory journal until the shutdown flush.
const FLUSH_SIZE_BYTES: u64 = 4096;

/// Seeded `message_expiry`, far enough out that nothing expires mid-run.
///
/// The value matters only in that it is NOT the default (`NeverExpire`): a key
/// lost across the swap is re-derived at its default, so seeding the default
/// would make the assertion pass either way.
const MESSAGE_EXPIRY_SECS: u64 = 30 * 24 * 60 * 60;
const WAL_TAIL_MESSAGE_EXPIRY_SECS: u64 = 7 * 24 * 60 * 60;

/// Seeded `max_topic_size`. Non-default for the same reason (the default is
/// `Unlimited`), and far above the few MiB this test produces so retention
/// never trims a segment.
const MAX_TOPIC_SIZE_BYTES: u64 = 512 * 1024 * 1024;
const WAL_TAIL_MAX_TOPIC_SIZE_BYTES: u64 = 256 * 1024 * 1024;

/// Base payload length; [`payload_for`] adds [`PAYLOAD_STEP`] per position
/// inside a batch.
const PAYLOAD_SIZE: usize = 16 * 1024;
const PAYLOAD_STEP: usize = 64;

/// Messages a segment holds before the append that crosses
/// [`SEGMENT_SIZE_BYTES`] seals it. One [`SEND_BATCH`] costs 272304 bytes on
/// disk: a 256-byte batch header, then per message a 48-byte frame header,
/// the 91 bytes of [`user_headers_for`] and the payload, whose lengths sum
/// to 269824 over the 16 positions of [`payload_for`]. Three batches stay
/// under 1 MiB, the fourth crosses it.
const MESSAGES_PER_SEGMENT: u64 = 4 * SEND_BATCH;

/// Sealed segments the seed leaves behind, before the deletion in step 3.
const SEALED_SEGMENTS: u64 = 5;

/// Messages left in the ACTIVE segment once production stops.
///
/// Non-zero on purpose. Boot unseals the last segment, and a run that stopped
/// on a segment boundary reopens an EMPTY file, so neither the unseal nor a
/// torn-tail truncation of a partially written segment is ever exercised.
/// Not a multiple of [`SEND_BATCH`] either, so the final batch is short.
const ACTIVE_SEGMENT_MESSAGES: u64 = 10;

const PRODUCED_MESSAGES: u64 = SEALED_SEGMENTS * MESSAGES_PER_SEGMENT + ACTIVE_SEGMENT_MESSAGES;

const SEND_BATCH: u64 = 16;

/// Segment logs (the sealed ones plus the active one) the seed waits for
/// before deleting one, so a sealed segment is still left behind.
const MIN_SEGMENTS_BEFORE_DELETE: usize = 4;

/// Messages produced into the topic that is purged afterwards.
const PURGED_MESSAGES: u64 = 8;

/// Messages appended to the purged topic AFTER the purge, at generation 1.
///
/// Without them the purge check is vacuous. An unreadable `purge.gen` reads
/// as 0 on purpose (the absent-or-torn sentinel), the reconciler then
/// re-purges the partition, and an empty topic passes a `messages_count == 0`
/// assertion whether or not the generation decoded. Only messages that must
/// SURVIVE the swap can tell the two apart.
const POST_PURGE_MESSAGES: u64 = 8;

/// Seeded personal access token expiry. Non-default (the default is
/// `NeverExpire`, which reads back as `None`) so a dropped or misread field
/// cannot pass as the default.
const PAT_EXPIRY_SECS: u64 = 7 * 24 * 60 * 60;
const WAL_TAIL_PAT_EXPIRY_SECS: u64 = 3 * 24 * 60 * 60;

/// Contiguous messages re-polled after the swap.
const READBACK_COUNT: u32 = 16;

/// Bound on every [`wait_until`] probe loop.
///
/// Kept small because the whole test has to finish inside nextest's 300s hard
/// kill (`slow-timeout` x `terminate-after` in `.config/nextest.toml`, and the
/// driving script deliberately runs without `--profile ci`). Two boots at 60s
/// plus two stops at 5s plus seven of these waits is 200s. A SIGKILL past
/// that budget would take the named `wait_until` message with it, losing the
/// diagnostic in exactly the run that needed it. At a 200ms
/// [`POLL_INTERVAL`] this is still ~50 probes per wait.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Substring shared by all three tombstone log lines in
/// `core/server/src/bootstrap.rs`. Matching the full sentence of any one of
/// them would miss the others, and a segment layout change lands on the
/// first arm, not the consensus-state one.
///
/// Being shorter than a whole word is what makes it match every arm, and also
/// what makes it collide with [`TOMBSTONE_FIELD`]. Strip that before matching
/// rather than lengthening this.
const TOMBSTONE_MARKER: &str = "tombston";

/// The one tracing FIELD in the server named for the tombstone state
/// (`partition_reconciler.rs`, the parked-frame discard). Fields render as
/// `key=value`, so a shard that is merely not the namespace's owner logs
/// `tombstoned=false`, which contains [`TOMBSTONE_MARKER`] on a HEALTHY run.
///
/// Removed from the log before the absence check. Only this exact field form
/// goes: every real arm reads `tombstoning it`, `tombstoned rather than` or
/// `tombstoning the`, and none of them emits a `tombstoned` field, so their
/// detection is untouched.
///
/// The collision is dormant at the default `info` level, but the line is
/// `debug!`, and two independent variables reach the child: the harness
/// forwards every `IGGY_*` name, and `RUST_LOG` is inherited by any process
/// and OUTRANKS the configured level. Filtering the text keeps the check
/// working under both instead of refusing to run.
const TOMBSTONE_FIELD: &str = "tombstoned=";

/// Logged by `iggy-server`'s `main` once shutdown ran to completion. A stop
/// that escalated to `SIGKILL` leaves crash-recovery state behind, which
/// would make the next boot exercise crash recovery and compatibility at the
/// same time.
const GRACEFUL_SHUTDOWN_MARKER: &str = "server shutdown complete";

#[tokio::test]
#[parallel]
#[ignore = "needs a baseline iggy-server built from master; run via scripts/ci/storage-compat.sh"]
async fn should_read_back_a_data_directory_written_by_the_baseline_server() {
    let baseline = baseline_server_binary();
    let envs = resolve_config_paths(&HashMap::from([(
        "metadata.journal_slots".to_string(),
        JOURNAL_SLOTS.to_string(),
    )]))
    .expect("metadata.journal_slots resolves against the live config catalog");

    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(
            TestServerConfig::builder()
                .executable_path(baseline.clone())
                .extra_envs(envs)
                .build(),
        )
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let data_path = harness.server().data_path();
    let client = harness.tcp_root_client().await.unwrap();

    // 1. The topic carrying the segment chain. The flush thresholds are load
    //    bearing: without them committed messages stay in the journal and the
    //    partition directory looks empty until shutdown.
    let data_stream_details = client.create_stream(DATA_STREAM).await.unwrap();
    let data_stream = Identifier::numeric(data_stream_details.id).unwrap();
    let data_topic_details = client
        .create_topic(&data_stream, DATA_TOPIC, &data_topic_options())
        .await
        .unwrap();
    let data_topic = Identifier::numeric(data_topic_details.id).unwrap();
    let partition = partition_dir(
        &data_path,
        data_stream_details.id,
        data_topic_details.id,
        SEEDED_PARTITION,
    );

    // 2. Produce past [`SEALED_SEGMENTS`] rotations and then stop MID-segment.
    //    A segment rolls on the append that crosses the target, so the
    //    crossing batch lands whole and the tail batch stays in the active one.
    let mut produced = 0u64;
    while produced < PRODUCED_MESSAGES {
        let batch_end = (produced + SEND_BATCH).min(PRODUCED_MESSAGES);
        let mut batch: Vec<IggyMessage> = (produced..batch_end)
            .map(|offset| seeded_message(offset, payload_for(offset)))
            .collect();
        client
            .send_messages(
                &data_stream,
                &data_topic,
                &Partitioning::partition_id(SEEDED_PARTITION),
                &mut batch,
            )
            .await
            .unwrap_or_else(|error| panic!("send messages {produced}..{batch_end}: {error}"));
        produced = batch_end;
    }

    wait_until(
        "the produced messages to seal enough segments",
        async || {
            let logs = segment_logs(&partition);
            if logs.len() >= MIN_SEGMENTS_BEFORE_DELETE {
                Ok(())
            } else {
                Err(format!(
                    "{} holds {} segment log(s) ({logs:?}), need {MIN_SEGMENTS_BEFORE_DELETE}",
                    partition.display(),
                    logs.len()
                ))
            }
        },
    )
    .await;
    let before_delete = segment_logs(&partition);

    // 3. Delete the oldest sealed segment, BEFORE any consumer offset exists:
    //    a committed offset is a retention barrier the removal stops at. The
    //    ack only records the truncation watermark; the reconciler removes the
    //    files on a later commit-driven pass.
    client
        .delete_segments(&data_stream, &data_topic, SEEDED_PARTITION, 1)
        .await
        .unwrap();
    let deleted_segment = before_delete[0].clone();
    wait_until("the deleted segment to leave disk", async || {
        let logs = segment_logs(&partition);
        if logs.contains(&deleted_segment) {
            Err(format!(
                "{deleted_segment} is still present in {} ({logs:?})",
                partition.display()
            ))
        } else {
            Ok(())
        }
    })
    .await;

    let retained = segment_logs(&partition);
    assert!(
        retained.len() >= 2,
        "the seed must keep at least one sealed segment beside the active one, got {retained:?}"
    );
    let first_retained_offset = base_offset_of(&retained[0]);
    assert!(
        first_retained_offset > 0,
        "deleting the oldest segment must move the partition's first offset off zero, \
         segments before={before_delete:?} after={retained:?}"
    );

    // 4. Both offset flavours, so `offsets/consumers/` and `offsets/groups/`
    //    are populated.
    let offset_consumer = Consumer::new(Identifier::named(OFFSET_CONSUMER).unwrap());
    client
        .store_consumer_offset(
            &offset_consumer,
            &data_stream,
            &data_topic,
            Some(SEEDED_PARTITION),
            first_retained_offset,
        )
        .await
        .unwrap();

    client
        .create_consumer_group(&data_stream, &data_topic, OFFSET_GROUP)
        .await
        .unwrap();
    // Storing a GROUP offset is a partition op gated on membership: a caller
    // that only created the group does not own the partition at the current
    // generation, so the namespace resolve fails and the server replies
    // ResourceNotFound with an empty body.
    client
        .join_consumer_group(
            &data_stream,
            &data_topic,
            &Identifier::named(OFFSET_GROUP).unwrap(),
        )
        .await
        .unwrap();
    let offset_group = Consumer::group(Identifier::named(OFFSET_GROUP).unwrap());
    client
        .store_consumer_offset(
            &offset_group,
            &data_stream,
            &data_topic,
            Some(SEEDED_PARTITION),
            first_retained_offset,
        )
        .await
        .unwrap();

    for kind in ["consumers", "groups"] {
        let dir = partition.join("offsets").join(kind);
        wait_until(&format!("the {kind} offset to reach disk"), async || {
            let count = fs::read_dir(&dir)
                .map(|entries| entries.flatten().count())
                .unwrap_or(0);
            if count > 0 {
                Ok(())
            } else {
                Err(format!("{} holds no offset file", dir.display()))
            }
        })
        .await;
    }

    // 5. A purge on a SEPARATE topic: it resets the partition and empties its
    //    segment chain, so it must not touch the one above.
    let purge_stream_details = client.create_stream(PURGE_STREAM).await.unwrap();
    let purge_stream = Identifier::numeric(purge_stream_details.id).unwrap();
    let purge_topic_details = client
        .create_topic(&purge_stream, PURGE_TOPIC, &purge_topic_options())
        .await
        .unwrap();
    let purge_topic = Identifier::numeric(purge_topic_details.id).unwrap();
    let mut purged_batch: Vec<IggyMessage> = (0..PURGED_MESSAGES)
        .map(|index| seeded_message(index, Bytes::from(format!("compat-purged-{index}"))))
        .collect();
    client
        .send_messages(
            &purge_stream,
            &purge_topic,
            &Partitioning::partition_id(SEEDED_PARTITION),
            &mut purged_batch,
        )
        .await
        .unwrap();
    client
        .purge_topic(&purge_stream, &purge_topic)
        .await
        .unwrap();

    let purge_generation = partition_dir(
        &data_path,
        purge_stream_details.id,
        purge_topic_details.id,
        SEEDED_PARTITION,
    )
    .join("purge.gen");
    wait_until("the purge generation to reach disk", async || {
        if purge_generation.is_file() {
            Ok(())
        } else {
            Err(format!("{} does not exist", purge_generation.display()))
        }
    })
    .await;

    // Appended once the generation is durable, so they sit at offset 0 of the
    // reset chain and only survive a boot that decodes `purge.gen`.
    let mut post_purge_batch: Vec<IggyMessage> = (0..POST_PURGE_MESSAGES)
        .map(|index| seeded_message(index, post_purge_payload(index)))
        .collect();
    client
        .send_messages(
            &purge_stream,
            &purge_topic,
            &Partitioning::partition_id(SEEDED_PARTITION),
            &mut post_purge_batch,
        )
        .await
        .unwrap();

    // 6. A user with permissions at every level, and a personal access token.
    let permissions = seeded_permissions(data_stream_details.id, data_topic_details.id);
    client
        .create_user(
            COMPAT_USER,
            USER_PASSWORD,
            UserStatus::Active,
            Some(permissions.clone()),
        )
        .await
        .unwrap();
    let seeded_user = client
        .get_user(&Identifier::named(COMPAT_USER).unwrap())
        .await
        .unwrap()
        .expect("the baseline lists the user it just created");
    assert_eq!(
        seeded_user.permissions,
        Some(permissions.clone()),
        "the baseline must report the permissions exactly as seeded, or a post-swap mismatch \
         could not be attributed to the swap"
    );
    let pat = create_token(&client, COMPAT_PAT, PAT_EXPIRY_SECS).await;

    // 7. Drive the metadata plane past its first forced checkpoint. Everything
    //    above is now encoded in `snapshot.bin`, which the second boot must
    //    fold in as its recovery floor; everything below only exists as WAL
    //    records replayed on top of it.
    for index in 0..SEED_STREAMS {
        client
            .create_stream(&format!("compat-seed-{index}"))
            .await
            .unwrap_or_else(|error| panic!("create seed stream {index}: {error}"));
    }

    let snapshot_path = data_path.join("metadata").join("snapshot.bin");
    wait_until(
        "the metadata checkpoint to persist a snapshot",
        async || match fs::metadata(&snapshot_path).map(|meta| meta.len()) {
            Ok(len) if len > 0 => Ok(()),
            other => Err(format!(
                "{SEED_STREAMS} committed stream creates must cross the {CHECKPOINT_EVERY}-op \
                 checkpoint and persist a non-empty snapshot at {}, got {other:?}",
                snapshot_path.display()
            )),
        },
    )
    .await;

    // 8. One more of each rich type past the checkpoint, so the WAL encodings
    //    are exercised as well: a topic with every option key, a user with
    //    permissions, a token, a group whose membership churned, and streams.
    let tail_topic_details = client
        .create_topic(&data_stream, WAL_TAIL_TOPIC, &wal_tail_topic_options())
        .await
        .unwrap();
    let tail_permissions = seeded_permissions(data_stream_details.id, tail_topic_details.id);
    client
        .create_user(
            WAL_TAIL_USER,
            USER_PASSWORD,
            UserStatus::Active,
            Some(tail_permissions.clone()),
        )
        .await
        .unwrap();
    let tail_pat = create_token(&client, WAL_TAIL_PAT, WAL_TAIL_PAT_EXPIRY_SECS).await;
    client
        .create_consumer_group(&data_stream, &data_topic, TRANSIENT_GROUP)
        .await
        .unwrap();
    let transient_group = Identifier::named(TRANSIENT_GROUP).unwrap();
    client
        .join_consumer_group(&data_stream, &data_topic, &transient_group)
        .await
        .unwrap();
    client
        .leave_consumer_group(&data_stream, &data_topic, &transient_group)
        .await
        .unwrap();

    for index in 0..WAL_TAIL_STREAMS {
        client
            .create_stream(&format!("compat-tail-{index}"))
            .await
            .unwrap_or_else(|error| panic!("create tail stream {index}: {error}"));
    }

    // 9. What the baseline serves, captured for a field-by-field comparison
    //    after the swap. Checked against the seed first, so a post-swap
    //    mismatch can be attributed to the swap.
    let expected_streams = stream_catalog(&client).await;
    assert_eq!(
        expected_streams.len(),
        (SEED_STREAMS + WAL_TAIL_STREAMS + 2) as usize,
        "the seed must have created every stream before the swap"
    );
    let readback = Consumer::new(Identifier::named(READBACK_CONSUMER).unwrap());
    let last_offset = PRODUCED_MESSAGES - 1;
    let retained_before = poll_window(
        &client,
        &data_stream,
        &data_topic,
        &readback,
        first_retained_offset,
        READBACK_COUNT,
    )
    .await;
    assert_seeded_messages(
        "the retained window",
        &retained_before,
        first_retained_offset,
        READBACK_COUNT as usize,
        payload_for,
    );
    let tail_before = poll_window(
        &client,
        &data_stream,
        &data_topic,
        &readback,
        last_offset,
        1,
    )
    .await;
    assert_seeded_messages(
        "the last produced message",
        &tail_before,
        last_offset,
        1,
        payload_for,
    );
    let post_purge_before = poll_window(
        &client,
        &purge_stream,
        &purge_topic,
        &readback,
        0,
        READBACK_COUNT,
    )
    .await;
    assert_seeded_messages(
        "the post-purge messages",
        &post_purge_before,
        0,
        POST_PURGE_MESSAGES as usize,
        post_purge_payload,
    );

    drop(client);
    harness.stop().await.unwrap();
    assert_graceful_shutdown(&harness, "the baseline server");

    let baseline_files = disk::collect_comparable_files(&data_path, false);
    assert!(
        baseline_files
            .keys()
            .any(|rel| rel.starts_with("streams/") && rel.ends_with(".log")),
        "the baseline wrote no segment .log under {}, so the byte comparison would be vacuous \
         (found: {:?})",
        data_path.display(),
        baseline_files.keys().collect::<Vec<_>>()
    );

    // The active segment must be PARTIALLY filled: anything at or above the
    // target is a sealed one. Boot unseals the last segment, so an empty
    // active segment leaves both the reopen and any torn-tail truncation the
    // comparison below would catch unreachable.
    let active_segment = retained.last().expect("the retained chain is non-empty");
    let active_relative = partition
        .strip_prefix(&data_path)
        .expect("the partition directory sits under the data directory")
        .join(active_segment)
        .to_string_lossy()
        .replace('\\', "/");
    let active_bytes = baseline_files
        .get(&active_relative)
        .unwrap_or_else(|| panic!("the baseline wrote no {active_relative}"))
        .len() as u64;
    assert!(
        active_bytes > 0 && active_bytes < SEGMENT_SIZE_BYTES,
        "{active_segment} holds {active_bytes} byte(s), so it is not a partially filled active \
         segment. Production must stop between two rotations: PRODUCED_MESSAGES \
         ({PRODUCED_MESSAGES}) must not be a multiple of MESSAGES_PER_SEGMENT \
         ({MESSAGES_PER_SEGMENT}), and the on-disk batch cost the latter is derived from must \
         still hold. Segments: {retained:?}"
    );

    // The swap: `None` selects the cargo-built binary of the crate under test.
    harness.server_mut().set_executable_path(None);
    harness.restart_server().await.unwrap();

    // `tcp_root_client` hands out a client the harness does not own, so the
    // reconnect loop inside `restart_server` never touched it. Take a fresh one.
    let client = harness.tcp_root_client().await.unwrap();

    // Ids AND names: later reads resolve by numeric id, so a corrupted name
    // would pass a bare count.
    assert_eq!(
        stream_catalog(&client).await,
        expected_streams,
        "streams written by the baseline must all recover with their ids and names, both the \
         checkpointed prefix and the WAL tail"
    );

    assert_topic_recovered(&client, &data_stream, DATA_TOPIC, &data_topic_options()).await;
    assert_topic_recovered(
        &client,
        &data_stream,
        WAL_TAIL_TOPIC,
        &wal_tail_topic_options(),
    )
    .await;
    assert_topic_recovered(&client, &purge_stream, PURGE_TOPIC, &purge_topic_options()).await;

    assert_eq!(
        segment_logs(&partition),
        retained,
        "the segment chain must recover exactly as the baseline left it"
    );

    assert_messages_identical(
        "the retained window",
        &retained_before,
        &poll_window(
            &client,
            &data_stream,
            &data_topic,
            &readback,
            first_retained_offset,
            READBACK_COUNT,
        )
        .await,
    );
    assert_messages_identical(
        "the last produced message",
        &tail_before,
        &poll_window(
            &client,
            &data_stream,
            &data_topic,
            &readback,
            last_offset,
            1,
        )
        .await,
    );

    let stored = client
        .get_consumer_offset(
            &offset_consumer,
            &data_stream,
            &data_topic,
            Some(SEEDED_PARTITION),
        )
        .await
        .unwrap()
        .expect("the individual consumer offset survives the swap");
    assert_eq!(
        stored.stored_offset, first_retained_offset,
        "individual consumer offset"
    );
    let stored_group = client
        .get_consumer_offset(
            &offset_group,
            &data_stream,
            &data_topic,
            Some(SEEDED_PARTITION),
        )
        .await
        .unwrap()
        .expect("the group consumer offset survives the swap");
    assert_eq!(
        stored_group.stored_offset, first_retained_offset,
        "group consumer offset"
    );
    assert_eq!(
        disk::read_replicated_consumer_offset(&data_path),
        Some(first_retained_offset),
        "the on-disk offset record must decode to the offset the baseline stored"
    );

    let purged = client
        .get_topic(&purge_stream, &purge_topic)
        .await
        .unwrap()
        .expect("the purged topic survives the swap");
    assert_eq!(
        purged.messages_count, POST_PURGE_MESSAGES,
        "the purged topic must hold exactly the messages appended after the purge: fewer means \
         `purge.gen` read back as 0 and the partition was purged again, more means the purge \
         itself was lost"
    );
    assert_messages_identical(
        "the post-purge messages",
        &post_purge_before,
        &poll_window(
            &client,
            &purge_stream,
            &purge_topic,
            &readback,
            0,
            READBACK_COUNT,
        )
        .await,
    );

    // A generation that decoded ABOVE the committed one passes both checks
    // above, nothing was re-purged, while parking the partition past every
    // purge the topic will ever commit: the reconciler stages a reset only
    // for `committed > applied`. So a purge issued under the build under test
    // must still take effect.
    client
        .purge_topic(&purge_stream, &purge_topic)
        .await
        .unwrap();
    wait_until("the post-swap purge to empty the topic", async || {
        let topic = client
            .get_topic(&purge_stream, &purge_topic)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "the purged topic is gone".to_string())?;
        if topic.messages_count == 0 {
            Ok(())
        } else {
            Err(format!(
                "messages_count is still {}: the reconciler staged no reset, so the recovered \
                 generation must sit at or above the newly committed one",
                topic.messages_count
            ))
        }
    })
    .await;
    let after_purge = poll_window(
        &client,
        &purge_stream,
        &purge_topic,
        &readback,
        0,
        READBACK_COUNT,
    )
    .await;
    assert!(
        after_purge.is_empty(),
        "offset 0 of the purged topic must be empty after the post-swap purge, got {} message(s)",
        after_purge.len()
    );

    assert_user_recovered(&harness, &client, COMPAT_USER, &permissions).await;
    assert_user_recovered(&harness, &client, WAL_TAIL_USER, &tail_permissions).await;
    assert_token_recovered(&harness, &client, COMPAT_PAT, &pat).await;
    assert_token_recovered(&harness, &client, WAL_TAIL_PAT, &tail_pat).await;

    let groups = client
        .get_consumer_groups(&data_stream, &data_topic)
        .await
        .unwrap();
    let group_names: Vec<&str> = groups.iter().map(|group| group.name.as_str()).collect();
    assert!(
        group_names.contains(&OFFSET_GROUP) && group_names.contains(&TRANSIENT_GROUP),
        "both consumer groups must survive the swap, got {group_names:?}"
    );

    drop(client);
    harness.stop().await.unwrap();

    // Taken after the readback so the non-blocking stdout appender has had the
    // whole run to drain, and after a positive marker so an uncaptured log
    // fails loudly instead of passing the absence check vacuously.
    assert_graceful_shutdown(&harness, "the server under test");
    // `stdout_plain` returns the same ANSI-stripped text `stdout_contains`
    // matches against, so filtering it costs no matching power.
    let stdout = harness.server().stdout_plain();
    assert!(
        !stdout
            .replace(TOMBSTONE_FIELD, "")
            .contains(TOMBSTONE_MARKER),
        "the server under test tombstoned a partition rather than reading the baseline's \
         durable state; boot still exits 0, so only the log says so. Server stdout:\n{stdout}"
    );

    // The purged topic was rewritten by the post-swap purge on purpose, so
    // only the rest of the tree is held to byte identity.
    assert_segments_identical(
        &baseline_files,
        &disk::collect_comparable_files(&data_path, false),
        &topic_prefix(purge_stream_details.id, purge_topic_details.id),
    );
}

/// Resolve the baseline binary to an absolute, existing path.
///
/// The absolute requirement is not cosmetic. `ServerHandle::start` only spawns
/// the configured path directly when it has more than one component or already
/// exists; a bare name that does not exist falls through to
/// `Command::cargo_bin`, which resolves the binary of the crate under test. So
/// a single-component value would boot the HEAD build while the test reported
/// it as the baseline, comparing master against master and passing forever.
/// The same silent-green family as running a stale binary.
fn baseline_server_binary() -> String {
    let raw = std::env::var(BASELINE_SERVER_ENV).unwrap_or_else(|_| {
        panic!(
            "{BASELINE_SERVER_ENV} is unset. It must be an ABSOLUTE path to the iggy-server \
             binary built from master. Without it this test would boot the build under test \
             twice and prove nothing, so it refuses to run. Use scripts/ci/storage-compat.sh, \
             which builds the baseline and exports the variable."
        )
    });

    let path = PathBuf::from(&raw);
    assert!(
        path.is_absolute(),
        "{BASELINE_SERVER_ENV}={raw:?} is relative. It must be an ABSOLUTE path: a bare binary \
         name that does not exist on disk is silently resolved as the build under test, which \
         would compare master against master and report green forever."
    );
    let resolved = fs::canonicalize(&path).unwrap_or_else(|error| {
        panic!(
            "{BASELINE_SERVER_ENV}={raw:?} does not resolve: {error}. It must point at an \
             iggy-server binary built from master."
        )
    });
    assert!(
        resolved.is_file(),
        "{BASELINE_SERVER_ENV}={raw:?} resolves to {}, which is not a file.",
        resolved.display()
    );
    resolved.display().to_string()
}

/// Options of the topic that carries the segment chain. Every key but
/// `compression_algorithm` is sent, so that one must come back derived while
/// the rest come back explicit.
fn data_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(PARTITIONS_COUNT),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::new_from_secs(
            MESSAGE_EXPIRY_SECS,
        ))),
        max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(
            MAX_TOPIC_SIZE_BYTES,
        ))),
        segment_size: Some(IggyByteSize::from(SEGMENT_SIZE_BYTES)),
        enforce_fsync: Some(true),
        messages_required_to_save: Some(1),
        size_of_messages_required_to_save: Some(IggyByteSize::from(FLUSH_SIZE_BYTES)),
        // Left at the default, so only its provenance flag can tell a
        // surviving key from a re-derived one. Turning it on would reserve
        // the whole segment up front, which changes the shape of the active
        // segment the byte comparison certifies.
        preallocate_segments: Some(false),
        ..TopicCreateOptions::default()
    }
}

/// Options of the topic created after the checkpoint. Different values from
/// [`data_topic_options`] where a key has a usable one, so a record attributed
/// to the wrong topic cannot pass, and a different unsent key
/// (`enforce_fsync`) for the provenance check.
fn wal_tail_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(PARTITIONS_COUNT),
        compression_algorithm: Some(CompressionAlgorithm::Gzip),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::new_from_secs(
            WAL_TAIL_MESSAGE_EXPIRY_SECS,
        ))),
        max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(
            WAL_TAIL_MAX_TOPIC_SIZE_BYTES,
        ))),
        segment_size: Some(IggyByteSize::from(2 * SEGMENT_SIZE_BYTES)),
        messages_required_to_save: Some(1),
        size_of_messages_required_to_save: Some(IggyByteSize::from(2 * FLUSH_SIZE_BYTES)),
        preallocate_segments: Some(false),
        ..TopicCreateOptions::default()
    }
}

/// Options of the topic that is purged. `compression_algorithm` is stored
/// topic metadata only (the server compresses nothing), so Gzip is just a
/// non-default value that must round-trip.
fn purge_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(PARTITIONS_COUNT),
        compression_algorithm: Some(CompressionAlgorithm::Gzip),
        messages_required_to_save: Some(1),
        ..TopicCreateOptions::default()
    }
}

/// Deterministic message for `offset`: an explicit id, typed user headers and
/// the given payload, so a readback can name exactly which message diverged.
fn seeded_message(offset: u64, payload: Bytes) -> IggyMessage {
    IggyMessage::builder()
        .id(message_id_for(offset))
        .payload(payload)
        .user_headers(user_headers_for(offset))
        .build()
        .unwrap()
}

/// Both 64-bit halves non-zero and unequal, so a swapped, truncated or zeroed
/// half cannot pass. The builder's default id is 0.
fn message_id_for(offset: u64) -> u128 {
    ((u128::from(offset) + 1) << 64) | u128::from(u64::MAX - offset)
}

/// One value of each of four kinds. Fixed width, so every message costs the
/// same header bytes and the segment math in [`MESSAGES_PER_SEGMENT`] holds.
fn user_headers_for(offset: u64) -> BTreeMap<HeaderKey, HeaderValue> {
    BTreeMap::from([
        (
            HeaderKey::try_from("offset").unwrap(),
            HeaderValue::from(offset),
        ),
        (
            HeaderKey::try_from("origin").unwrap(),
            HeaderValue::try_from(format!("compat-{offset:06}")).unwrap(),
        ),
        (
            HeaderKey::try_from("even").unwrap(),
            HeaderValue::from(offset.is_multiple_of(2)),
        ),
        (
            HeaderKey::try_from("ratio").unwrap(),
            HeaderValue::from(1.0 / (offset as f64 + 1.0)),
        ),
    ])
}

/// Prefixed with its own offset, then padded to a length that varies with
/// the position inside the batch, so a decoder that reused one message's
/// `payload_length` for the next cannot pass. The pattern repeats every
/// [`SEND_BATCH`], so every batch costs the same on disk.
fn payload_for(offset: u64) -> Bytes {
    let mut bytes = format!("compat-message-{offset:06}").into_bytes();
    bytes.resize(
        PAYLOAD_SIZE + (offset % SEND_BATCH) as usize * PAYLOAD_STEP,
        b'.',
    );
    Bytes::from(bytes)
}

fn post_purge_payload(index: u64) -> Bytes {
    Bytes::from(format!("compat-post-purge-{index}"))
}

/// Alternating bits at every level. All-true (the harness default) reads back
/// identical under any field permutation, and a level left `None` cannot tell
/// a dropped map from one never seeded.
fn seeded_permissions(stream_id: u32, topic_id: u32) -> Permissions {
    Permissions {
        global: GlobalPermissions {
            manage_servers: false,
            read_servers: true,
            manage_users: false,
            read_users: true,
            manage_streams: false,
            read_streams: true,
            manage_topics: false,
            read_topics: true,
            poll_messages: false,
            send_messages: true,
        },
        streams: Some(BTreeMap::from([(
            stream_id as usize,
            StreamPermissions {
                manage_stream: true,
                read_stream: false,
                manage_topics: true,
                read_topics: false,
                poll_messages: true,
                send_messages: false,
                topics: Some(BTreeMap::from([(
                    topic_id as usize,
                    TopicPermissions {
                        manage_topic: false,
                        read_topic: true,
                        poll_messages: false,
                        send_messages: true,
                    },
                )])),
            },
        )])),
    }
}

/// A personal access token as the baseline handed it out: the raw token a
/// later login presents, and the stored expiry a listing must reproduce.
struct SeededToken {
    raw: String,
    expiry_at: IggyTimestamp,
}

async fn create_token(client: &IggyClient, name: &str, expiry_secs: u64) -> SeededToken {
    let raw = client
        .create_personal_access_token(
            name,
            PersonalAccessTokenExpiry::ExpireDuration(IggyDuration::new_from_secs(expiry_secs)),
        )
        .await
        .unwrap()
        .token;
    let expiry_at = client
        .get_personal_access_tokens()
        .await
        .unwrap()
        .into_iter()
        .find(|token| token.name == name)
        .and_then(|token| token.expiry_at)
        .unwrap_or_else(|| panic!("the baseline lists {name} with its expiry"));
    SeededToken { raw, expiry_at }
}

/// Stream id to name, the identity the metadata plane must recover.
async fn stream_catalog(client: &impl StreamClient) -> BTreeMap<u32, String> {
    client
        .get_streams()
        .await
        .unwrap()
        .into_iter()
        .map(|stream| (stream.id, stream.name))
        .collect()
}

/// `count` messages from `offset` in [`SEEDED_PARTITION`], without committing
/// an offset: that would add a retention barrier and a stored record of its
/// own.
async fn poll_window(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    offset: u64,
    count: u32,
) -> Vec<IggyMessage> {
    let polled = client
        .poll_messages(
            stream,
            topic,
            Some(SEEDED_PARTITION),
            consumer,
            &PollingStrategy::offset(offset),
            count,
            false,
        )
        .await
        .unwrap_or_else(|error| panic!("poll {count} message(s) from offset {offset}: {error}"));
    assert_eq!(
        polled.partition_id, SEEDED_PARTITION,
        "the poll must be served from the seeded partition"
    );
    polled.messages
}

/// The baseline's own readback of what it was asked to store. Verified before
/// the swap, so the captured messages are a trustworthy oracle for the
/// field-by-field comparison after it.
fn assert_seeded_messages(
    what: &str,
    messages: &[IggyMessage],
    first_offset: u64,
    count: usize,
    payload: fn(u64) -> Bytes,
) {
    assert_eq!(
        messages.len(),
        count,
        "{what}: the baseline must serve every seeded message from offset {first_offset}"
    );
    for (index, message) in messages.iter().enumerate() {
        let offset = first_offset + index as u64;
        assert_eq!(message.header.offset, offset, "{what}: offset");
        assert_eq!(
            message.header.id,
            message_id_for(offset),
            "{what}: id of the message at offset {offset}"
        );
        assert_eq!(
            message.user_headers_map().unwrap(),
            Some(user_headers_for(offset)),
            "{what}: user headers of the message at offset {offset}"
        );
        assert!(
            message.payload == payload(offset),
            "{what}: the payload of the message at offset {offset} ({} bytes) is not the seeded one",
            message.payload.len()
        );
    }
}

/// Every stored field. The header carries the id, both timestamps, the
/// checksum and the lengths, and a payload comparison alone would pass a
/// misread of any of them.
fn assert_messages_identical(what: &str, baseline: &[IggyMessage], current: &[IggyMessage]) {
    assert_eq!(
        baseline.len(),
        current.len(),
        "{what}: message count after the swap"
    );
    for (before, after) in baseline.iter().zip(current) {
        let offset = before.header.offset;
        assert_eq!(
            after.header, before.header,
            "{what}: header of the message at offset {offset} (post-swap left, baseline right)"
        );
        assert_eq!(
            after.user_headers, before.user_headers,
            "{what}: user headers of the message at offset {offset} (post-swap left, baseline \
             right)"
        );
        assert!(
            after.payload == before.payload,
            "{what}: the payload of the message at offset {offset} differs after the swap ({} vs \
             {} bytes)",
            after.payload.len(),
            before.payload.len()
        );
    }
}

/// Per-key option degradation is silent, so every seeded value is re-read,
/// and with it the provenance flag: admission refills a key it cannot read
/// from the built-in default and marks it derived, which the value check
/// alone cannot catch for a key seeded at its default (`preallocate_segments`
/// has no usable non-default value here). The unsent key must come back
/// derived, or provenance stopped round-tripping and the explicit checks
/// prove nothing.
async fn assert_topic_recovered(
    client: &IggyClient,
    stream: &Identifier,
    name: &str,
    seed: &TopicCreateOptions,
) {
    let topic = client
        .get_topic(stream, &Identifier::named(name).unwrap())
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("the topic {name} must survive the swap"));

    // Masked to the keys the seed sent: `from_resource_options` fills the
    // rest from the derived defaults, which the seed leaves `None`.
    let recovered = TopicCreateOptions::from_resource_options(&topic.options);
    let recovered = TopicCreateOptions {
        partitions_count: seed.partitions_count.map(|_| topic.partitions_count),
        compression_algorithm: seed
            .compression_algorithm
            .map(|_| topic.compression_algorithm),
        message_expiry: seed.message_expiry.map(|_| topic.message_expiry),
        max_topic_size: seed.max_topic_size.map(|_| topic.max_topic_size),
        segment_size: seed.segment_size.and(recovered.segment_size),
        enforce_fsync: seed.enforce_fsync.and(recovered.enforce_fsync),
        messages_required_to_save: seed
            .messages_required_to_save
            .and(recovered.messages_required_to_save),
        size_of_messages_required_to_save: seed
            .size_of_messages_required_to_save
            .and(recovered.size_of_messages_required_to_save),
        preallocate_segments: seed
            .preallocate_segments
            .and(recovered.preallocate_segments),
        raw: BTreeMap::new(),
    };
    assert_eq!(
        &recovered, seed,
        "{name}: every seeded value must read back as the baseline stored it (recovered left, \
         seed right); a lost key silently reverts to the shard-wide default with no log line"
    );

    for (key, sent) in [
        (
            topic_option_keys::COMPRESSION_ALGORITHM,
            seed.compression_algorithm.is_some(),
        ),
        (
            topic_option_keys::MESSAGE_EXPIRY,
            seed.message_expiry.is_some(),
        ),
        (
            topic_option_keys::MAX_TOPIC_SIZE,
            seed.max_topic_size.is_some(),
        ),
        (topic_option_keys::SEGMENT_SIZE, seed.segment_size.is_some()),
        (
            topic_option_keys::ENFORCE_FSYNC,
            seed.enforce_fsync.is_some(),
        ),
        (
            topic_option_keys::MESSAGES_REQUIRED_TO_SAVE,
            seed.messages_required_to_save.is_some(),
        ),
        (
            topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
            seed.size_of_messages_required_to_save.is_some(),
        ),
        (
            topic_option_keys::PREALLOCATE_SEGMENTS,
            seed.preallocate_segments.is_some(),
        ),
    ] {
        let option = topic
            .options
            .get(&HeaderKey::from_str(key).unwrap())
            .unwrap_or_else(|| panic!("{name}: {key} is GONE from the topic's options"));
        assert_eq!(
            option.explicit, sent,
            "{name}: {key} provenance. A seeded key that came back DERIVED was dropped and \
             refilled from the built-in default; an unsent key that came back EXPLICIT means \
             provenance no longer distinguishes a surviving option from a re-derived one"
        );
    }
}

/// Listing proves the metadata; the password hash is a separate field, and a
/// user that lists fine but cannot log in is still locked out. The login uses
/// a fresh, unauthenticated connection, so the root session vouches for
/// nothing.
async fn assert_user_recovered(
    harness: &TestHarness,
    client: &IggyClient,
    name: &str,
    permissions: &Permissions,
) {
    let user = client
        .get_user(&Identifier::named(name).unwrap())
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("the user {name} must survive the swap"));
    assert_eq!(user.status, UserStatus::Active, "{name}: status");
    assert_eq!(
        user.permissions.as_ref(),
        Some(permissions),
        "{name}: permissions must survive the swap bit for bit at the global, stream and topic \
         level"
    );
    let by_password = harness.tcp_new_client().await.unwrap();
    by_password
        .login_user(name, USER_PASSWORD)
        .await
        .unwrap_or_else(|error| {
            panic!("{name}: the seeded password must still authenticate after the swap: {error}")
        });
}

/// Same split as [`assert_user_recovered`]: the token hash is not in the
/// listing, so only a login proves it.
async fn assert_token_recovered(
    harness: &TestHarness,
    client: &IggyClient,
    name: &str,
    seeded: &SeededToken,
) {
    let tokens = client.get_personal_access_tokens().await.unwrap();
    let token = tokens
        .iter()
        .find(|token| token.name == name)
        .unwrap_or_else(|| {
            panic!(
                "the personal access token {name} must survive the swap, got {:?}",
                tokens.iter().map(|token| &token.name).collect::<Vec<_>>()
            )
        });
    assert_eq!(token.expiry_at, Some(seeded.expiry_at), "{name}: expiry");
    let by_token = harness.tcp_new_client().await.unwrap();
    by_token
        .login_with_personal_access_token(&seeded.raw)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{name}: the seeded personal access token must still authenticate after the \
                 swap: {error}"
            )
        });
}

/// A topic directory relative to the data directory, in the key form of
/// [`disk::collect_comparable_files`].
fn topic_prefix(stream_id: u32, topic_id: u32) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/")
}

fn partition_dir(data_path: &Path, stream_id: u32, topic_id: u32, partition_id: u32) -> PathBuf {
    data_path
        .join(topic_prefix(stream_id, topic_id))
        .join("partitions")
        .join(partition_id.to_string())
}

/// Segment log file names in a partition directory, in offset order (they are
/// zero-padded base offsets, so lexical order is offset order).
fn segment_logs(partition: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(partition)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| name.ends_with(".log"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn base_offset_of(segment_log: &str) -> u64 {
    segment_log
        .trim_end_matches(".log")
        .parse()
        .unwrap_or_else(|error| {
            panic!("segment {segment_log} is not named for a base offset: {error}")
        })
}

fn assert_graceful_shutdown(harness: &TestHarness, who: &str) {
    assert!(
        harness.server().stdout_contains(GRACEFUL_SHUTDOWN_MARKER),
        "{who} did not log {GRACEFUL_SHUTDOWN_MARKER:?}. Either the stop escalated to SIGKILL, \
         leaving crash-recovery state that confounds the compatibility verdict, or stdout was \
         not captured at all (IGGY_TEST_VERBOSE makes the harness inherit it, which would also \
         make the tombstone check vacuous). Server stdout:\n{}",
        harness.server().stdout_plain()
    );
}

/// Byte-compare the segment files across the binary swap, except under
/// `rewritten`, the one directory the build under test was told to change.
///
/// Segment batch headers carry no magic and no version, and recovery is
/// allowed to truncate a torn tail, so a misparse can shorten a `.log` while
/// every message a test happens to poll still reads back correctly.
fn assert_segments_identical(
    baseline: &BTreeMap<String, Vec<u8>>,
    current: &BTreeMap<String, Vec<u8>>,
    rewritten: &str,
) {
    assert!(
        baseline.keys().any(|rel| rel.starts_with(rewritten)),
        "`{rewritten}` matches nothing the baseline wrote, so the exclusion would hide a typo \
         rather than the post-swap purge"
    );
    let mut problems = Vec::new();
    for (rel, baseline_bytes) in baseline
        .iter()
        .filter(|(rel, _)| !rel.starts_with(rewritten))
    {
        match current.get(rel) {
            None => problems.push(format!(
                "`{rel}` was written by the baseline but is GONE after the swap"
            )),
            Some(bytes) if bytes != baseline_bytes => {
                problems.push(disk::describe_mismatch(rel, 0, baseline_bytes, 1, bytes));
            }
            Some(_) => {}
        }
    }
    for rel in current.keys().filter(|rel| !rel.starts_with(rewritten)) {
        if !baseline.contains_key(rel) {
            problems.push(format!("`{rel}` appeared only after the swap"));
        }
    }
    assert!(
        problems.is_empty(),
        "the build under test did not preserve the baseline's segment bytes \
         ({} issue(s); node 0 = baseline, node 1 = post-swap):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

/// Poll `probe` until it succeeds, panicking with its last message on timeout.
async fn wait_until(what: &str, mut probe: impl AsyncFnMut() -> Result<(), String>) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        match probe().await {
            Ok(()) => return,
            Err(last) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out after {SETTLE_TIMEOUT:?} waiting for {what}: {last}"
                );
                sleep(POLL_INTERVAL).await;
            }
        }
    }
}
