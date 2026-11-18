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

//! The proof rows a placing partition keeps of the batches it commits.

use super::placed_batches::{placing_backup_with_wal, placing_primary, replicate_placed};
use super::*;
use iggy_binary_protocol::batch::BatchHeader;
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_object_store::FreshProofRow;

fn rows<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<FreshProofRow> {
    partition
        .fresh_proofs()
        .map_or_else(Vec::new, |proofs| proofs.rows_from(0).copied().collect())
}

fn row_offsets<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<u64> {
    rows(partition).iter().map(|row| row.base_offset).collect()
}

/// The placed batches in the segments under `directory`, as stored.
fn stored_placed(directory: &str) -> Vec<Vec<u8>> {
    segment_file_bytes(Path::new(directory))
        .concat()
        .chunks(PLACED_BATCH_SIZE)
        .map(<[u8]>::to_vec)
        .collect()
}

#[compio::test]
async fn given_a_backup_when_placed_batches_commit_should_keep_a_proof_row_of_each_as_committed() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;

    Box::pin(replicate_placed(&mut partition, &config, 0..=2, 0)).await;

    let stored = stored_placed(&config.get_partition_path(1, 1, 0));
    let rows = rows(&partition);
    assert_eq!(rows.len(), stored.len());
    assert_eq!(rows.len(), 3);
    for (row, bytes) in rows.iter().zip(&stored) {
        let placed = PlacedBatch::decode(bytes).unwrap();
        assert_eq!(row.object_id.get(), placed.placement.object_id);
        assert_eq!(row.position, placed.placement.position);
        assert_eq!(row.base_offset, placed.header.base_offset);
        assert_eq!(row.base_timestamp, placed.header.base_timestamp);
        assert_eq!(row.message_count, placed.header.message_count);
        assert_eq!(row.placed_checksum, placement::checksum_of(bytes));
    }
    assert!(
        !partition.take_proofs_wanted(),
        "a backup gives its shard no rows to seal"
    );
}

/// A prepare as [`placed_segment_prepare`] builds it, whose blob sits at
/// `position` of the object 7.
fn placed_prepare_at(op: u64, parent: u128, offset: u64, position: u64) -> Message<PrepareHeader> {
    let whole = checksummed_segment_prepare(op, parent, offset, b"orphaned");
    let header_size = size_of::<PrepareHeader>();
    let batch = BatchHeader::decode(&whole.as_slice()[header_size..]).unwrap();
    let blob = &whole.as_slice()[header_size + COMMAND_HEADER_SIZE..];
    let placed = PlacedBatch::new(batch, Placement::of(7, position, blob, 0));
    let total = header_size + PLACED_BATCH_SIZE;
    let mut prepare = Message::<PrepareHeader>::new(total);
    placed.encode_into(&mut prepare.as_mut_slice()[header_size..]);
    let template = *whole.header();
    prepare.transmute_header(|_, header: &mut PrepareHeader| {
        *header = template;
        header.size = u32::try_from(total).unwrap();
        header.checksum = header.identity_checksum();
    })
}

/// A primary that lost its view prepared two batches that never commit.
/// The replica that journaled them keeps no row of them, and the batch the
/// new view commits at their offset gets the row.
#[compio::test]
async fn given_prepared_batches_that_never_commit_when_a_view_change_replaces_them_should_keep_rows_of_the_committed_batches_only()
 {
    const ORPHAN_POSITION: u64 = 999_000;
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root.path())).await;
    let committed = Box::pin(replicate_placed(&mut partition, &config, 0..=0, 0)).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let mut parent = committed;
    for offset in 1..=2 {
        let prepare = placed_prepare_at(offset + 1, parent, offset, ORPHAN_POSITION + offset);
        parent = prepare.header().checksum;
        let writer = hold_wal_writer(&persistence);
        Box::pin(partition.on_replicate(prepare)).await;
        writer.await;
    }
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 2);
    assert_eq!(row_offsets(&partition), [0], "prepared is not committed");

    Box::pin(partition.truncate_uncommitted_from(2))
        .await
        .unwrap();
    Box::pin(replicate_placed(&mut partition, &config, 1..=1, committed)).await;

    let rows = rows(&partition);
    assert_eq!(row_offsets(&partition), [0, 1]);
    assert!(
        rows.iter()
            .all(|row| row.position < ORPHAN_POSITION && row.object_id.get() == 7),
        "no row names an orphan: {rows:?}"
    );
    let stored = stored_placed(&config.get_partition_path(1, 1, 0));
    let checksums: Vec<_> = stored
        .iter()
        .map(|bytes| placement::checksum_of(bytes))
        .collect();
    assert_eq!(
        rows.iter()
            .map(|row| row.placed_checksum)
            .collect::<Vec<_>>(),
        checksums
    );
}

/// The request the upload path hands a primary for a send of `count`
/// messages at `position` of the object 7, by a writer that claims the
/// batches below `proven_below` proven.
fn placed_request(
    partition: &IggyPartition<IggyMessageBus>,
    request: u64,
    count: u32,
    proven_below: u64,
) -> Message<RoutedRequestHeader> {
    let admitted = checksumless_send_request(partition.namespace(), count).transmute_header(
        |mut header, next: &mut RoutedRequestHeader| {
            header.request = request;
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        },
    );
    let blob = server_common::send_messages::admitted_blob(&admitted).unwrap();
    let placement = Placement::of(7, request * 4096, blob, proven_below);
    server_common::send_messages::place_request(&admitted, placement).unwrap()
}

#[compio::test]
async fn given_a_primary_whose_placements_claim_proofs_when_they_commit_should_drop_the_rows_below_the_highest_claim()
 {
    let (_directory, mut partition) = Box::pin(placing_primary()).await;
    let config = repair_config();
    assert!(partition.proves_placed_batches());
    assert!(!partition.take_proofs_wanted(), "nothing committed yet");

    for (request, count, proven_below, held) in [
        (1, 2, 0, vec![0]),
        (2, 3, 0, vec![0, 2]),
        (3, 1, 2, vec![2, 5]),
    ] {
        let placed = placed_request(&partition, request, count, proven_below);
        Box::pin(partition.on_placed_request(placed, None)).await;
        partition.consensus().advance_commit_max(request);
        Box::pin(settle_partition_io(&mut partition, &config)).await;
        assert_eq!(row_offsets(&partition), held, "after request {request}");
    }

    assert!(partition.take_proofs_wanted(), "rows wait for the shard");
    assert!(!partition.take_proofs_wanted(), "until more commit");
    assert_eq!(partition.proven_below(u64::MAX), 6);
    assert_eq!(partition.proven_below(5), 5);
    assert_eq!(
        partition.proven_below(0),
        2,
        "never below the highest claim"
    );
}

#[compio::test]
async fn given_a_partition_of_whole_batches_when_batches_commit_should_keep_no_proof_row() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 1)).await;
    let records: Vec<_> = (0..3)
        .map(|_| build_segment_record(IggyNamespace::new(1, 1, 0), 0))
        .collect();

    Box::pin(commit_records(&mut partition, 1, &records)).await;

    assert!(partition.fresh_proofs().is_none());
    assert!(!partition.take_proofs_wanted());
}
