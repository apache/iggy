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

use crate::shell::{ServerShard, ShellBus, ShellShard};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::requests::streams::{RewrapStreamKeyEntry, RewrapStreamKeysRequest};
use iggy_common::{
    HeaderKey, IggyError, MAX_WRAP_SET_ENVELOPES, OptionKey, RewrapPlanError, ServerKeyContext,
    stream_wrap_set,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::{Stream, StreamsInner};
use server_common::Message;
use shard::Receiver;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::Duration;
use tracing::{info, trace, warn};

/// How often the metadata primary checks whether a sweep is due. A tick with
/// an unchanged view and applied op does nothing else.
pub const REWRAP_INTERVAL: Duration = Duration::from_secs(5);
/// Upper bound on streams rewrapped by one replicated operation, so one
/// rotation never monopolizes the metadata pipeline.
const MAX_ENTRIES_PER_OPERATION: usize = 64;

/// What the rewrapper keeps between ticks.
#[derive(Debug, Default)]
struct Rewrapper {
    /// Metadata view and last applied op of the last finished sweep.
    swept: Option<(u32, u64)>,
    /// Stream incarnations already reported as stuck, so each warns once.
    stuck: BTreeSet<(usize, u64)>,
    /// Whether the last finished sweep found every set settled.
    settled: bool,
}

/// Outcome of one tick.
#[derive(Debug, PartialEq, Eq)]
enum Sweep {
    Stopped,
    NotPrimary,
    Unchanged,
    Interrupted,
    Finished,
}

/// A stream that needs a rewrap this node cannot make.
#[derive(Debug)]
struct StuckStream {
    stream_id: usize,
    created_op: u64,
    kek_fingerprints: String,
    error: RewrapPlanError,
}

/// Run the rewrapper until `stop` fires, planning with `context`, this node's
/// keys plus the retired fingerprints. Spawned only when that context is
/// rotating. Only the caught-up metadata primary proposes, so a rotation is
/// replicated once and every replica stores the same wrap sets. A node never
/// rewrites them locally.
pub async fn run_stream_key_rewrapper(
    shard: Rc<ServerShard>,
    context: ServerKeyContext,
    stop: Receiver<()>,
) {
    trace!(shard = shard.id, "stream key rewrapper started");
    let mut rewrapper = Rewrapper::default();
    loop {
        if compio::time::timeout(REWRAP_INTERVAL, stop.recv())
            .await
            .is_ok()
            || rewrapper.sweep(&shard, &context, &stop).await == Sweep::Stopped
        {
            break;
        }
    }
    trace!(shard = shard.id, "stream key rewrapper exited");
}

impl Rewrapper {
    /// Scan every stream once when this node is the caught-up primary and
    /// the metadata moved since the last finished sweep, then propose the
    /// unsettled ones in bounded operations.
    async fn sweep<B, MJ, S, SB>(
        &mut self,
        shard: &Rc<ShellShard<B, MJ, S, SB>>,
        context: &ServerKeyContext,
        stop: &Receiver<()>,
    ) -> Sweep
    where
        B: ShellBus,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        if stop.try_recv().is_ok() {
            return Sweep::Stopped;
        }
        let metadata = shard.plane.metadata();
        let Some(consensus) = metadata
            .consensus
            .as_ref()
            .filter(|_| metadata.is_caught_up_primary())
        else {
            self.swept = None;
            return Sweep::NotPrimary;
        };
        // Read with the scan below and no await between, so the mark names
        // exactly the state the scan saw.
        let mark = (consensus.view(), consensus.commit_min());
        if self.swept == Some(mark) {
            return Sweep::Unchanged;
        }
        let pending = metadata
            .mux_stm
            .streams()
            .read(|streams| unsettled_streams(streams, context));
        let mut rewrapped = 0;
        let mut stuck = BTreeSet::new();
        for ids in pending.chunks(MAX_ENTRIES_PER_OPERATION) {
            if stop.try_recv().is_ok() {
                return Sweep::Stopped;
            }
            let (entries, failures) = metadata
                .mux_stm
                .streams()
                .read(|streams| planned_entries(streams, context, ids));
            for failure in failures {
                let incarnation = (failure.stream_id, failure.created_op);
                if !self.stuck.contains(&incarnation) {
                    match &failure.error {
                        RewrapPlanError::WrapSetFull => warn!(
                            stream_id = failure.stream_id,
                            stored_kek_fingerprints = %failure.kek_fingerprints,
                            node_kek_fingerprints = %context.kek_fingerprints().join(","),
                            "Stream {} wrap set is full with {MAX_WRAP_SET_ENVELOPES} wraps, none under the current server key or a retired one, so add one of its fingerprints to retired_kek_fingerprints",
                            failure.stream_id
                        ),
                        RewrapPlanError::Key(error) => warn!(
                            stream_id = failure.stream_id,
                            stored_kek_fingerprints = %failure.kek_fingerprints,
                            node_kek_fingerprints = %context.kek_fingerprints().join(","),
                            error = %error,
                            "Stream {} needs a key rewrap this node cannot make: {error}",
                            failure.stream_id
                        ),
                    }
                }
                stuck.insert(incarnation);
            }
            if entries.is_empty() {
                continue;
            }
            let planned = entries.len();
            match metadata
                .submit_rewrap_stream_keys_in_process(RewrapStreamKeysRequest { entries })
                .await
            {
                Ok(commit) => {
                    rewrapped += planned;
                    info!(
                        streams = planned,
                        commit,
                        key_context = ?context,
                        "Rewrapped {planned} stream keys: added the current server key's wrap where missing and dropped retired ones"
                    );
                }
                Err(error) => {
                    warn!(
                        ?error,
                        "Stream key rewrap not committed, retrying on the next tick"
                    );
                    self.stuck.extend(stuck);
                    return Sweep::Interrupted;
                }
            }
        }
        let settled = rewrapped == 0 && stuck.is_empty();
        if !settled {
            info!(
                rewrapped,
                stuck = stuck.len(),
                "Stream key sweep rewrapped {rewrapped} streams, {} still need a rewrap this node cannot make",
                stuck.len()
            );
        } else if !self.settled {
            info!(
                kek_fingerprints = %context.kek_fingerprints().join(","),
                "Every encrypted stream holds a wrap under the current server key and none under a retired one"
            );
        }
        self.swept = Some(mark);
        self.stuck = stuck;
        self.settled = settled;
        Sweep::Finished
    }
}

/// Ids of the encrypted streams whose stored set is not what this node's
/// keys call for, in one pass that allocates only the result.
fn unsettled_streams(streams: &StreamsInner, context: &ServerKeyContext) -> Vec<usize> {
    let wrapped_key = HeaderKey::from(OptionKey::EncryptionKeyWrapped);
    streams
        .items
        .iter()
        .filter(|(_, stream)| {
            stream
                .options
                .get(&wrapped_key)
                .is_some_and(|option| !context.is_settled(option.value.as_bytes()))
        })
        .map(|(id, _)| id)
        .collect()
}

/// One operation's entries for the streams `ids` names, planned against
/// their current state, and the streams among them this node cannot rewrap.
/// A stream deleted or settled since the scan is skipped.
fn planned_entries(
    streams: &StreamsInner,
    context: &ServerKeyContext,
    ids: &[usize],
) -> (Vec<RewrapStreamKeyEntry>, Vec<StuckStream>) {
    let mut entries = Vec::with_capacity(ids.len());
    let mut stuck = Vec::new();
    for stream in ids.iter().filter_map(|id| streams.items.get(*id)) {
        match planned_entry(stream, context) {
            Ok(Some(entry)) => entries.push(entry),
            Ok(None) => {}
            Err(error) => stuck.push(StuckStream {
                stream_id: stream.id,
                created_op: stream.created_op,
                kek_fingerprints: stream_wrap_set(&stream.options)
                    .ok()
                    .flatten()
                    .map(|set| set.kek_fingerprints().join(","))
                    .unwrap_or_default(),
                error,
            }),
        }
    }
    (entries, stuck)
}

fn planned_entry(
    stream: &Stream,
    context: &ServerKeyContext,
) -> Result<Option<RewrapStreamKeyEntry>, RewrapPlanError> {
    let Some(stored) = stream_wrap_set(&stream.options)? else {
        return Ok(None);
    };
    let Some(target) = context.target_set(&stored)? else {
        return Ok(None);
    };
    Ok(Some(RewrapStreamKeyEntry {
        stream_id: u32::try_from(stream.id).map_err(|_| IggyError::InvalidStreamId)?,
        created_op: stream.created_op,
        key_fingerprint: stored.key_fingerprint_bytes().try_into().map_err(|_| {
            IggyError::UnsupportedCriticalOption(OptionKey::EncryptionKeyWrapped.to_string())
        })?,
        wrap_set: Bytes::copy_from_slice(target.as_ref()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestShard, prepare_message, test_shard_with_key_context,
    };
    use consensus::{Consensus as _, Plane as _, PlaneKind, Sequencer};
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::{Operation, PrepareOkHeader, WireEncode, WireName};
    use iggy_common::wire_conversions::resource_options_to_wire;
    use iggy_common::{
        HeaderValue, IggyTimestamp, OptionValue, OptionsProvenance, ResourceOptions,
        StreamEncryptionKey, StreamKeyOrigin, WrapSet, critical_keys_marker,
    };
    use journal::prepare_journal::PrepareJournal;
    use metadata::stm::{StateHandler, StateMachine};
    use std::sync::Arc;
    use tempfile::TempDir;

    const OLD_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
    const NEW_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
    const STRANGER_KEY: &str = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";

    #[compio::test]
    async fn given_backup_or_lagging_node_when_ticking_should_not_propose() {
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let (_stop_sender, stop) = shard::channel(1);
        for (replica, replica_count, outcome) in
            [(1, 3, Sweep::NotPrimary), (0, 1, Sweep::Finished)]
        {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard_with_key_context(
                &bus,
                replica,
                replica_count,
                1,
                Arc::clone(&rotated),
            ));
            // A backup holds a stale set and must leave it to the primary.
            // The sole replica is the primary, but its set already matches.
            let stored_under = if replica_count == 1 { NEW_KEY } else { OLD_KEY };
            seed_stream(&shard, "seeded", &context(stored_under, &[]));
            let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
            let sequence = consensus.sequencer().current_sequence();

            assert_eq!(
                Rewrapper::default().sweep(&shard, &rotated, &stop).await,
                outcome
            );

            assert_eq!(
                consensus.sequencer().current_sequence(),
                sequence,
                "replica {replica} of {replica_count} must not propose"
            );
            assert!(bus.replica_sends.borrow().is_empty());
        }
    }

    #[compio::test]
    async fn given_unchanged_metadata_when_sweeping_again_should_skip_the_scan() {
        let bus = SpyBus::default();
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let shard = Rc::new(test_shard_with_key_context(
            &bus,
            0,
            1,
            1,
            Arc::clone(&rotated),
        ));
        seed_stream(&shard, "seeded", &context(STRANGER_KEY, &[]));
        let (_stop_sender, stop) = shard::channel(1);
        let mut rewrapper = Rewrapper::default();

        assert_eq!(
            rewrapper.sweep(&shard, &rotated, &stop).await,
            Sweep::Finished
        );
        assert_eq!(rewrapper.stuck.len(), 1, "the stranger's set is reported");
        assert!(!rewrapper.settled);
        assert_eq!(
            rewrapper.sweep(&shard, &rotated, &stop).await,
            Sweep::Unchanged
        );
        assert_eq!(rewrapper.stuck.len(), 1);
    }

    #[compio::test]
    async fn given_settled_primary_when_a_stale_stream_commits_should_rescan_and_rewrap_it() {
        let directory = TempDir::new().expect("metadata journal directory");
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let shard = journal_backed_primary(&directory, &rotated).await;
        let (_stop_sender, stop) = shard::channel(1);
        let mut rewrapper = Rewrapper::default();
        assert_eq!(
            rewrapper.sweep(&shard, &rotated, &stop).await,
            Sweep::Finished
        );
        assert!(rewrapper.settled);
        assert_eq!(
            rewrapper.sweep(&shard, &rotated, &stop).await,
            Sweep::Unchanged
        );

        // An edge still on the old config wraps a new stream under the old key only.
        commit_stream(&shard, "created-mid-rotation", &context(OLD_KEY, &[])).await;
        let consensus = shard.plane.metadata().consensus.as_ref().expect("primary");
        let before = consensus.commit_min();

        assert_eq!(
            rewrapper.sweep(&shard, &rotated, &stop).await,
            Sweep::Finished
        );
        assert_eq!(consensus.commit_min(), before + 1, "one rewrap commits");
        assert_eq!(unsettled(&shard, &rotated), Vec::<usize>::new());
    }

    #[compio::test]
    async fn given_more_stale_streams_than_one_operation_when_sweeping_should_commit_every_chunk() {
        let directory = TempDir::new().expect("metadata journal directory");
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let shard = journal_backed_primary(&directory, &rotated).await;
        let old = context(OLD_KEY, &[]);
        let stale_count = 2 * MAX_ENTRIES_PER_OPERATION + 8;
        for index in 0..stale_count {
            seed_stream(&shard, &format!("stale-{index}"), &old);
        }
        let (_stop_sender, stop) = shard::channel(1);
        let consensus = shard.plane.metadata().consensus.as_ref().expect("primary");
        let before = consensus.commit_min();

        assert_eq!(
            Rewrapper::default().sweep(&shard, &rotated, &stop).await,
            Sweep::Finished
        );

        assert_eq!(
            consensus.commit_min() - before,
            stale_count.div_ceil(MAX_ENTRIES_PER_OPERATION) as u64
        );
        assert_eq!(unsettled(&shard, &rotated), Vec::<usize>::new());
    }

    #[test]
    fn given_more_stale_streams_than_one_operation_when_sweeping_should_plan_each_from_one_scan() {
        let old = context(OLD_KEY, &[]);
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let mut streams = StreamsInner::new();
        let stale_count = 2 * MAX_ENTRIES_PER_OPERATION + 8;
        for index in 0..stale_count {
            create_stream(&mut streams, &format!("stale-{index}"), &old, 1);
            create_stream(&mut streams, &format!("settled-{index}"), &rotated, 2);
        }

        let pending = unsettled_streams(&streams, &rotated);

        assert_eq!(pending.len(), stale_count);
        let mut planned = Vec::new();
        for ids in pending.chunks(MAX_ENTRIES_PER_OPERATION) {
            let (entries, stuck) = planned_entries(&streams, &rotated, ids);
            assert!(entries.len() <= MAX_ENTRIES_PER_OPERATION);
            assert!(stuck.is_empty());
            planned.extend(entries);
        }
        let mut planned_ids: Vec<u32> = planned.iter().map(|entry| entry.stream_id).collect();
        planned_ids.dedup();
        assert_eq!(planned_ids.len(), stale_count, "each stale stream once");
        for entry in &planned {
            let stream = streams.items.get(entry.stream_id as usize).expect("exists");
            assert!(stream.name.starts_with("stale-"));
            let set = WrapSet::try_from(entry.wrap_set.as_ref()).expect("a valid set");
            assert_eq!(set.envelopes().len(), 2);
            assert_eq!(set.key_fingerprint_bytes(), entry.key_fingerprint);
            assert!(rotated.is_settled(set.as_ref()));
        }
    }

    #[test]
    fn given_old_and_current_sets_when_planning_should_entry_only_the_stale_one() {
        let mut streams = StreamsInner::new();
        create_stream(&mut streams, "old", &context(OLD_KEY, &[]), 1);
        create_stream(&mut streams, "current", &context(NEW_KEY, &[]), 2);
        let rotated = context(NEW_KEY, &[OLD_KEY]);

        let pending = unsettled_streams(&streams, &rotated);
        let (entries, stuck) = planned_entries(&streams, &rotated, &pending);

        assert_eq!(pending, vec![0]);
        assert!(stuck.is_empty());
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.stream_id, 0);
        let set = WrapSet::try_from(entry.wrap_set.as_ref()).expect("a valid set");
        assert_eq!(set.envelopes().len(), 2);
        assert_eq!(set.key_fingerprint_bytes(), entry.key_fingerprint);
    }

    #[test]
    fn given_sets_this_node_cannot_rewrap_when_planning_should_report_them() {
        let mut streams = StreamsInner::new();
        create_stream(&mut streams, "stranger", &context(STRANGER_KEY, &[]), 1);
        create_stream(&mut streams, "old", &context(OLD_KEY, &[]), 2);
        let rotated = context(NEW_KEY, &[OLD_KEY]);

        let pending = unsettled_streams(&streams, &rotated);
        let (entries, stuck) = planned_entries(&streams, &rotated, &pending);

        assert_eq!(pending, vec![0, 1]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].stream_id, 1);
        assert_eq!(stuck.len(), 1);
        assert_eq!(stuck[0].stream_id, 0);
        assert!(matches!(
            stuck[0].error,
            RewrapPlanError::Key(IggyError::CannotDecryptData)
        ));
        assert_eq!(
            stuck[0].kek_fingerprints,
            context(STRANGER_KEY, &[]).kek_fingerprint().expect("a key")
        );
    }

    #[test]
    fn given_retired_kek_when_planning_should_drop_its_wraps_and_keep_unknown_ones() {
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let old_fingerprint = context(OLD_KEY, &[]).kek_fingerprint().expect("a key");
        let phase_two = Arc::new(
            ServerKeyContext::from_config(NEW_KEY, &[], true)
                .and_then(|context| context.with_retired_kek_fingerprints(&[old_fingerprint]))
                .expect("valid keys"),
        );
        let mut streams = StreamsInner::new();
        create_stream(&mut streams, "both", &rotated, 1);
        let stranger_and_new = WrapSet::new(vec![
            context(STRANGER_KEY, &[])
                .wrap(&seeded_key(2), StreamKeyOrigin::Server)
                .expect("wraps"),
            phase_two
                .wrap(&seeded_key(2), StreamKeyOrigin::Server)
                .expect("wraps"),
        ])
        .expect("two wraps");
        insert_stream(&mut streams, "unknown-kept", &stranger_and_new);

        let pending = unsettled_streams(&streams, &phase_two);
        let (entries, stuck) = planned_entries(&streams, &phase_two, &pending);

        assert_eq!(pending, vec![0]);
        assert!(stuck.is_empty());
        let set = WrapSet::try_from(entries[0].wrap_set.as_ref()).expect("a valid set");
        assert_eq!(set.kek_fingerprints(), phase_two.kek_fingerprints());
    }

    fn context(key: &str, previous: &[&str]) -> Arc<ServerKeyContext> {
        let previous: Vec<String> = previous.iter().map(|key| (*key).to_owned()).collect();
        Arc::new(ServerKeyContext::from_config(key, &previous, true).expect("valid keys"))
    }

    fn seeded_key(seed: u8) -> StreamEncryptionKey {
        StreamEncryptionKey::try_from([seed; 32].as_slice()).expect("32 bytes")
    }

    fn create_stream(
        streams: &mut StreamsInner,
        name: &str,
        wrapping: &ServerKeyContext,
        seed: u8,
    ) {
        let set = wrapping
            .wrap_set(&seeded_key(seed), StreamKeyOrigin::Server)
            .expect("wraps");
        insert_stream(streams, name, &set);
    }

    fn insert_stream(streams: &mut StreamsInner, name: &str, set: &WrapSet) {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).expect("fits")),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).expect("marker"),
            ),
        );
        let request = CreateStreamRequest {
            name: WireName::new(name).expect("valid name"),
            options: resource_options_to_wire(&options, OptionsProvenance::All).expect("encodes"),
        };
        let reply = StateHandler::apply(&request, streams, IggyTimestamp::now());
        assert_eq!(reply.code, 0);
    }

    async fn journal_backed_primary(
        directory: &TempDir,
        keys: &Arc<ServerKeyContext>,
    ) -> Rc<TestShard> {
        let mut shard = test_shard_with_key_context(&SpyBus::default(), 0, 1, 1, Arc::clone(keys));
        shard.plane.metadata_mut().journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .expect("opens the metadata journal"),
        );
        Rc::new(shard)
    }

    /// Commits a create through the primary's replicate path, as the create an
    /// edge rewrote reaches it.
    async fn commit_stream(shard: &TestShard, name: &str, wrapping: &ServerKeyContext) {
        let metadata = shard.plane.metadata();
        let consensus = metadata.consensus.as_ref().expect("primary");
        let prepare = consensus::seal_prepare_checksum(
            prepare_message(
                Operation::CreateStream,
                0,
                1,
                &create_stream_body(name, wrapping),
            )
            .transmute_header(|old, header: &mut PrepareHeader| {
                *header = old;
                header.session = 0;
            }),
        );
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        metadata.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        for ack in loopback {
            metadata
                .on_ack(
                    ack.try_into_typed::<PrepareOkHeader>()
                        .expect("loopback holds self PrepareOks"),
                )
                .await;
        }
    }

    fn unsettled(shard: &TestShard, keys: &ServerKeyContext) -> Vec<usize> {
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| unsettled_streams(streams, keys))
    }

    fn seed_stream<B, MJ, S, SB>(
        shard: &Rc<ShellShard<B, MJ, S, SB>>,
        name: &str,
        wrapping: &ServerKeyContext,
    ) where
        B: ShellBus,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::CreateStream,
                1,
                1,
                &create_stream_body(name, wrapping),
            ))
            .expect("applies");
    }

    fn create_stream_body(name: &str, wrapping: &ServerKeyContext) -> Bytes {
        let set = WrapSet::from(
            wrapping
                .wrap(&seeded_key(5), StreamKeyOrigin::Client)
                .expect("wraps"),
        );
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).expect("fits")),
        );
        CreateStreamRequest {
            name: WireName::new(name).expect("valid name"),
            options: resource_options_to_wire(&options, OptionsProvenance::All).expect("encodes"),
        }
        .to_bytes()
    }
}
