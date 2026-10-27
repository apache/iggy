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

use crate::storage::SimStorage;
use futures::executor::block_on;
use iggy_binary_protocol::Operation;
use journal::Journal;
use journal::durable_storage::DurableStorage;
use partitions::{PartitionPersistence, RetainedPartitionState, WalStorage};
use server_common::send_messages::decode_prepare_slice_trusted;
use server_common::sharding::IggyNamespace;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// Open the WAL of one partition incarnation on a replica's disk, creating its
/// directory on first use, and fit the log a restart carries over to it.
/// Without that log the incarnation starts on an empty WAL.
///
/// Each incarnation has its own directory, so the writer lease of a deleted
/// incarnation never fences the open of its successor.
pub fn reopen(
    storage: &SimStorage,
    namespace: IggyNamespace,
    incarnation: u64,
    retained: Option<&mut RetainedPartitionState>,
) -> Rc<PartitionPersistence> {
    let directory = PathBuf::from(format!("/partition-{}-{incarnation}", namespace.inner()));
    // The harness replays no WAL into an empty partition, so a group that comes
    // back without its log comes back without its WAL too, as a lost disk would.
    let lost = retained.is_none();
    block_on(async {
        if lost {
            storage.remove_tree(&directory).await?;
        }
        storage.create_directories(&directory).await?;
        storage.sync_directory(Path::new("/")).await?;
        storage.sync_directory(&directory).await
    })
    .expect("the simulated disk creates a partition directory");
    let (persistence, _) = block_on(PartitionPersistence::open_with_storage(
        &directory.join(format!("prepares-{incarnation}")),
        namespace.inner(),
        incarnation,
        WalStorage::Injected(Rc::new(storage.clone())),
    ))
    .expect("the simulated disk reopens a partition WAL");
    if let Some(retained) = retained {
        let kept = fit_to_wal(retained, &persistence);
        if kept < persistence.head() {
            persistence.truncate_from(kept + 1);
            if persistence.start() {
                block_on(Rc::clone(&persistence).run());
            }
        }
    }
    persistence
}

/// Remove the directory of a deleted incarnation.
pub fn remove(storage: &SimStorage, namespace: IggyNamespace, incarnation: u64) {
    let directory = PathBuf::from(format!("/partition-{}-{incarnation}", namespace.inner()));
    block_on(storage.remove_tree(&directory)).expect("the simulated disk removes a partition");
}

/// Keep the applied prefix of `retained` and the journal suffix the WAL still
/// holds, and return the head that survives. A crash takes what the WAL had not
/// synced, so an op above the applied prefix that the WAL lost is lost here too.
/// Production recovers that suffix from the WAL itself, so an entry the journal
/// does not hold at the same op is truncated from the WAL by the caller.
fn fit_to_wal(retained: &mut RetainedPartitionState, persistence: &PartitionPersistence) -> u64 {
    let journal = &retained.log.journal().inner;
    let held = |op: u64| journal.repair_header(op).map(|header| header.checksum);
    let first_divergent = (1..=retained.applied_op.min(persistence.head()))
        .find(|&op| held(op).is_some_and(|checksum| persistence.checksum(op) != Some(checksum)));
    let mut kept = retained.applied_op;
    while kept < retained.head_op
        && first_divergent.is_none()
        && held(kept + 1).is_some_and(|checksum| persistence.checksum(kept + 1) == Some(checksum))
    {
        kept += 1;
    }
    if kept < retained.head_op {
        retained.prepare_checksum = held(kept)
            .or_else(|| persistence.checksum(kept))
            .unwrap_or_default();
        let mut write_offset = retained.durable_offset;
        for op in retained.applied_op + 1..=kept {
            if journal
                .repair_header(op)
                .is_some_and(|header| header.operation == Operation::SendMessages)
                && let Some(entry) = journal.repair_entry(op)
                && let Ok(batch) = decode_prepare_slice_trusted(entry.as_slice())
                && batch.header.message_count > 0
            {
                write_offset = write_offset
                    .max(batch.header.base_offset + u64::from(batch.header.message_count) - 1);
            }
        }
        block_on(journal.remove_range(kept + 1..=u64::MAX))
            .expect("the retained journal drops a suffix");
        retained.head_op = kept;
        retained.write_offset = write_offset;
    }
    first_divergent.map_or(kept, |op| op - 1)
}
