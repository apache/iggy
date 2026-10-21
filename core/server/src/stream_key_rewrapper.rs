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
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::requests::streams::{RewrapStreamKeyEntry, RewrapStreamKeysRequest};
use iggy_common::{ServerKeyContext, stream_wrap_set};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::StreamsInner;
use server_common::Message;
use shard::Receiver;
use std::rc::Rc;
use std::time::Duration;
use tracing::{info, trace, warn};

/// How often the metadata primary compares stored wrap sets with this node's
/// configured keys. A pass with nothing to change does no cryptography.
pub const REWRAP_INTERVAL: Duration = Duration::from_secs(5);
/// Upper bound on streams rewrapped by one replicated operation, so one
/// rotation never monopolizes the metadata pipeline. A full batch runs the
/// next pass at once.
const MAX_ENTRIES_PER_OPERATION: usize = 64;

/// Outcome of one rewrap pass.
#[derive(Debug, PartialEq, Eq)]
enum Pass {
    Stopped,
    HitCap,
    Drained,
}

/// Run the rewrapper until `stop` fires. Only the caught-up metadata primary
/// proposes, so a rotation is replicated once and every replica stores the
/// same wrap sets. A node never rewrites them locally.
pub async fn run_stream_key_rewrapper(shard: Rc<ServerShard>, stop: Receiver<()>) {
    trace!(shard = shard.id, "stream key rewrapper started");
    'ticks: loop {
        match compio::time::timeout(REWRAP_INTERVAL, stop.recv()).await {
            Ok(_) => break 'ticks,
            Err(_) => loop {
                match rewrap_pass(&shard, &stop).await {
                    Pass::HitCap => {}
                    Pass::Drained => break,
                    Pass::Stopped => break 'ticks,
                }
            },
        }
    }
    trace!(shard = shard.id, "stream key rewrapper exited");
}

async fn rewrap_pass<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stop: &Receiver<()>,
) -> Pass
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if stop.try_recv().is_ok() {
        return Pass::Stopped;
    }
    let metadata = shard.plane.metadata();
    if !metadata.is_caught_up_primary() {
        return Pass::Drained;
    }
    let context = shard.plane.partitions().key_context();
    let entries = metadata
        .mux_stm
        .streams()
        .read(|streams| planned_entries(streams, &context));
    if entries.is_empty() {
        return Pass::Drained;
    }
    let planned = entries.len();
    match metadata
        .submit_rewrap_stream_keys_in_process(RewrapStreamKeysRequest { entries })
        .await
    {
        Ok(commit) => {
            info!(
                streams = planned,
                commit,
                kek_fingerprints = %context.kek_fingerprints().join(","),
                "Rewrapped {planned} stream keys under the configured server keys"
            );
            if planned == MAX_ENTRIES_PER_OPERATION {
                Pass::HitCap
            } else {
                Pass::Drained
            }
        }
        Err(error) => {
            warn!(
                ?error,
                "Stream key rewrap not committed, retrying on the next pass"
            );
            Pass::Drained
        }
    }
}

/// The streams whose stored wrap set differs from what this node's keys
/// call for, with the set each should store, at most one operation's worth.
fn planned_entries(
    streams: &StreamsInner,
    context: &ServerKeyContext,
) -> Vec<RewrapStreamKeyEntry> {
    streams
        .items
        .iter()
        .filter_map(|(_, stream)| {
            let stored = stream_wrap_set(&stream.options).ok()??;
            let target = context.target_set(&stored).ok()??;
            Some(RewrapStreamKeyEntry {
                stream_id: u32::try_from(stream.id).ok()?,
                created_op: stream.created_op,
                key_fingerprint: stored.key_fingerprint_bytes().try_into().ok()?,
                wrap_set: Bytes::copy_from_slice(target.as_ref()),
            })
        })
        .take(MAX_ENTRIES_PER_OPERATION)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{SpyBus, prepare_message, test_shard_with_key_context};
    use consensus::Sequencer;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::{Operation, WireEncode, WireName};
    use iggy_common::wire_conversions::resource_options_to_wire;
    use iggy_common::{
        HeaderKey, HeaderValue, OptionKey, OptionValue, OptionsProvenance, ResourceOptions,
        StreamEncryptionKey, StreamKeyOrigin, WrapSet, critical_keys_marker,
    };
    use metadata::stm::{StateHandler, StateMachine};
    use std::sync::Arc;

    const OLD_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
    const NEW_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    #[compio::test]
    async fn given_backup_or_lagging_node_when_ticking_then_should_not_propose() {
        let rotated = context(NEW_KEY, &[OLD_KEY]);
        let (_stop_sender, stop) = shard::channel(1);
        for (replica, replica_count) in [(1, 3), (0, 1)] {
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
            seed_stream(&shard, &context(stored_under, &[]));
            let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
            let sequence = consensus.sequencer().current_sequence();

            assert_eq!(rewrap_pass(&shard, &stop).await, Pass::Drained);

            assert_eq!(
                consensus.sequencer().current_sequence(),
                sequence,
                "replica {replica} of {replica_count} must not propose"
            );
            assert!(bus.replica_sends.borrow().is_empty());
        }
    }

    #[test]
    fn given_old_and_current_sets_when_planning_then_should_entry_only_the_stale_one() {
        let mut streams = StreamsInner::new();
        create_stream(&mut streams, "old", &context(OLD_KEY, &[]), 1);
        create_stream(&mut streams, "current", &context(NEW_KEY, &[]), 2);
        let rotated = context(NEW_KEY, &[OLD_KEY]);

        let entries = planned_entries(&streams, &rotated);

        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.stream_id, 0);
        let set = WrapSet::try_from(entry.wrap_set.as_ref()).expect("a valid set");
        assert_eq!(set.envelopes().len(), 2);
        assert_eq!(set.key_fingerprint_bytes(), entry.key_fingerprint);
        // Without the old key the stale set cannot open, so it is left alone,
        // and the current set is already right.
        assert_eq!(
            planned_entries(&streams, &context(NEW_KEY, &[])),
            Vec::<RewrapStreamKeyEntry>::new()
        );
    }

    fn context(key: &str, previous: &[&str]) -> Arc<ServerKeyContext> {
        let previous: Vec<String> = previous.iter().map(|key| (*key).to_owned()).collect();
        Arc::new(ServerKeyContext::from_config(key, &previous, true).expect("valid keys"))
    }

    fn create_stream(
        streams: &mut StreamsInner,
        name: &str,
        wrapping: &ServerKeyContext,
        seed: u8,
    ) {
        let key = StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap();
        let set = WrapSet::from(wrapping.wrap(&key, StreamKeyOrigin::Server).unwrap());
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
            ),
        );
        let request = CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
        };
        let reply = StateHandler::apply(&request, streams, iggy_common::IggyTimestamp::now());
        assert_eq!(reply.code, 0);
    }

    fn seed_stream<B, MJ, S, SB>(shard: &Rc<ShellShard<B, MJ, S, SB>>, wrapping: &ServerKeyContext)
    where
        B: ShellBus,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        let key = StreamEncryptionKey::try_from([5u8; 32].as_slice()).unwrap();
        let set = WrapSet::from(wrapping.wrap(&key, StreamKeyOrigin::Client).unwrap());
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
        );
        let body = CreateStreamRequest {
            name: WireName::new("seeded").unwrap(),
            options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
        }
        .to_bytes();
        shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(Operation::CreateStream, 1, 1, &body))
            .unwrap();
    }
}
