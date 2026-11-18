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

//! A partition that follows another cluster's log, and serves the batches
//! that the origin proved in its fresh objects above the applied floor.
//! The origin's store is a memory store that holds one object of sends and
//! the objects that prove them, built as the origin's shard builds them.

use super::peer_log::{follower, manifest_of, origin_layout, plan, polled};
use super::segment_chain::PREFIX;
use super::*;
use crate::poll_plan::PollTier;
use crate::{PeerFreshRows, ProofTrust};
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, RoutedRequestHeader};
use iggy_common::PollingStrategy;
use iggy_object_store::{
    FreshBatch, FreshIndex, FreshObjectHeader, FreshObjectId, FreshPartition, FreshProofRow,
    MemoryStore, ObjectStore, PutBody, PutOptions, SealedFreshObject,
};
use server_common::send_messages::{
    ChecksumMode, SendMessagesOwned, admitted_blob, calculate_batch_checksum,
    convert_request_message, place_request, stamp_prepare_for_persistence,
};

/// The floor of the manifest the follower applies first.
const FLOOR: u64 = 3;
const SEALED_AT: u64 = 1_700_000_000_000_000;

fn origin_namespace() -> IggyNamespace {
    IggyNamespace::new(
        usize::try_from(PREFIX.stream_id).unwrap(),
        usize::try_from(PREFIX.topic_id).unwrap(),
        usize::try_from(PREFIX.partition_id).unwrap(),
    )
}

fn id(sequence: u64) -> FreshObjectId {
    FreshObjectId::new(1, 2, SEALED_AT, sequence).unwrap()
}

/// A send of `count` messages to the origin, as its shard admits it.
fn admitted(count: u32, first: u32) -> Message<RoutedRequestHeader> {
    let namespace = origin_namespace();
    let mut messages = IggyMessages::with_capacity(count as usize);
    for index in first..first + count {
        let payload = Bytes::from(format!("fresh message {index}"));
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                origin_timestamp: SEALED_AT + u64::from(index),
                payload_length: u32::try_from(payload.len()).unwrap(),
                ..IggyMessageHeader::default()
            },
            payload,
            user_headers: None,
        });
    }
    let mut owned = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
    owned.header.batch_checksum = calculate_batch_checksum(&owned.header, &owned.blob);
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + owned.header.total_size();
    let mut request = Message::<RoutedRequestHeader>::new(total);
    let body = &mut request.as_mut_slice()[header_size..];
    owned.header.encode_into(&mut body[..BATCH_HEADER_SIZE]);
    body[BATCH_HEADER_SIZE..].copy_from_slice(&owned.blob);
    let request = request.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::SendMessages;
        header.client = 1;
        header.session = 1;
        header.request = u64::from(first) + 1;
        header.group = namespace.inner();
        header.size = u32::try_from(total).unwrap();
    });
    convert_request_message(namespace, request, ChecksumMode::Skip).unwrap()
}

/// What the origin stored: one object of sends, and the proof rows of the
/// batches its partition committed from `FLOOR + 1` on.
struct Origin {
    sends: SealedFreshObject,
    proofs: Vec<FreshProofRow>,
}

fn origin() -> Origin {
    let sends = [admitted(2, 0), admitted(3, 2)];
    let batch = |send: &Message<RoutedRequestHeader>| {
        let header =
            BatchHeader::decode(&send.as_slice()[size_of::<RoutedRequestHeader>()..]).unwrap();
        let blob = admitted_blob(send).unwrap();
        FreshBatch {
            message_count: header.message_count,
            origin_timestamp: header.origin_timestamp,
            blob_checksum: placement::blob_checksum(blob),
            producer: None,
            blob: blob.to_vec(),
        }
    };
    let sealed = SealedFreshObject::seal(
        FreshObjectHeader {
            object_id: id(1),
            cluster_uid: 7,
            sealed_at: SEALED_AT,
        },
        &[FreshPartition {
            namespace: origin_namespace().inner(),
            created_revision: PREFIX.created_revision,
            view: 1,
            claimed_first_offset: FLOOR + 1,
            proven_below: FLOOR + 1,
            batches: sends.iter().map(batch).collect(),
            proofs: Vec::new(),
        }],
    )
    .unwrap();
    let mut offset = FLOOR + 1;
    let proofs = sends
        .iter()
        .zip(sealed.index.positions())
        .map(|(send, position)| {
            let blob = admitted_blob(send).unwrap();
            let placed =
                place_request(send, Placement::of(id(1).get(), *position, blob, FLOOR + 1))
                    .unwrap();
            let size = placed.header().size;
            let prepare = placed.transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::SendMessages;
                header.size = size;
            });
            let (stamped, ..) =
                stamp_prepare_for_persistence(prepare, offset, SEALED_AT + offset).unwrap();
            let start = size_of::<PrepareHeader>();
            let committed = &stamped.as_slice()[start..start + PLACED_BATCH_SIZE];
            let batch = PlacedBatch::decode(committed).unwrap();
            offset += u64::from(batch.header.message_count);
            FreshProofRow {
                object_id: id(1),
                position: *position,
                base_offset: batch.header.base_offset,
                base_timestamp: batch.header.base_timestamp,
                message_count: batch.header.message_count,
                placed_checksum: placement::checksum_of(committed),
            }
        })
        .collect();
    Origin {
        sends: sealed,
        proofs,
    }
}

/// The rows of the origin's partition in `index`.
fn rows(index: &FreshIndex, proofs: &[FreshProofRow], trust: ProofTrust) -> PeerFreshRows {
    let row = index.partition(origin_namespace().inner()).unwrap();
    PeerFreshRows {
        proofs: proofs.to_vec(),
        ..PeerFreshRows::of(index, row, trust)
    }
}

/// The follower with the origin's object of sends in its store, which
/// applied a manifest that ends at `FLOOR` and holds the rows of the
/// object of sends.
async fn following(
    store: &Rc<MemoryStore>,
    origin: &Origin,
) -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    store
        .put(
            &origin_layout().fresh_key(id(1).get()),
            PutBody::from_bytes(origin.sends.bytes.clone()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let (directory, mut follower) = follower(Rc::clone(store) as Rc<dyn crate::RemoteObjects>);
    let _ = follower
        .apply_peer_manifest(&manifest_of(None, &[(0, FLOOR)]))
        .unwrap();
    follower.offer_peer_fresh(&rows(&origin.sends.index, &[], ProofTrust::Owner));
    (directory, follower)
}

fn payloads(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    (0..10)
        .map(|index| format!("fresh message {index}"))
        .filter(|payload| text.contains(payload.as_str()))
        .collect()
}

#[compio::test]
async fn given_batches_proven_above_the_floor_when_a_follower_polls_should_serve_them_at_the_origins_offsets()
 {
    let store = Rc::new(MemoryStore::new());
    let origin = origin();
    let (_directory, mut follower) = Box::pin(following(&store, &origin)).await;
    assert!(
        matches!(
            plan(&mut follower, PollingStrategy::offset(FLOOR + 1), 10).tier,
            PollTier::Empty
        ),
        "a batch no proof names is not served"
    );

    let change = follower.offer_peer_fresh(&PeerFreshRows {
        object_id: id(2),
        batches: Vec::new(),
        ..rows(&origin.sends.index, &origin.proofs, ProofTrust::Owner)
    });

    assert!(change.grew);
    assert_eq!(follower.peer_fresh_served_end(), Some(FLOOR + 5));
    assert_eq!(follower.poll_visibility().1.offset, Some(FLOOR + 5));
    let (last, count, bytes) = Box::pin(polled(
        &mut follower,
        PollingStrategy::offset(FLOOR + 1),
        10,
    ))
    .await;
    assert_eq!((last, count), (Some(FLOOR + 5), 5));
    assert_eq!(payloads(&bytes).len(), 5);
    let (last, count, bytes) =
        Box::pin(polled(&mut follower, PollingStrategy::offset(FLOOR + 4), 1)).await;
    assert_eq!((last, count), (Some(FLOOR + 4), 1));
    assert_eq!(payloads(&bytes), ["fresh message 3"]);
    let (last, count, _) = Box::pin(polled(&mut follower, PollingStrategy::last(), 2)).await;
    assert_eq!((last, count), (Some(FLOOR + 5), 2));
    assert!(
        matches!(
            plan(&mut follower, PollingStrategy::offset(FLOOR + 6), 10).tier,
            PollTier::Empty
        ),
        "a consumer ahead of what this node serves waits"
    );
    assert_eq!(
        follower.store_offset_range_error(ConsumerKind::Consumer, FLOOR + 5),
        None
    );
    assert!(
        follower
            .store_offset_range_error(ConsumerKind::Consumer, FLOOR + 6)
            .is_some()
    );
}

#[compio::test]
async fn given_a_fresh_object_the_origin_deleted_when_a_follower_reads_it_should_refuse_the_read_and_plan_from_the_newer_floor()
 {
    let store = Rc::new(MemoryStore::new());
    let origin = origin();
    let (_directory, mut follower) = Box::pin(following(&store, &origin)).await;
    follower.offer_peer_fresh(&rows(
        &origin.sends.index,
        &origin.proofs,
        ProofTrust::Owner,
    ));
    let deleted = store
        .delete(&[origin_layout().fresh_key(id(1).get())])
        .await
        .unwrap();
    assert!(deleted.is_complete());

    let result = plan(&mut follower, PollingStrategy::offset(FLOOR + 1), 10)
        .execute()
        .await;

    assert_eq!(result.read_error, Some(IggyError::TieredReadFailed));
    assert_eq!(
        follower.complete_poll(result).map(|_| ()),
        Err(IggyError::TransientNotAccepted),
        "the read is retried rather than failed"
    );
    let _ = follower
        .apply_peer_manifest(&manifest_of(None, &[(0, FLOOR), (FLOOR + 1, FLOOR + 5)]))
        .unwrap();
    assert_eq!(follower.peer_fresh_served_end(), None);
    assert!(
        matches!(
            plan(&mut follower, PollingStrategy::offset(FLOOR + 1), 10).tier,
            PollTier::Disk { .. }
        ),
        "the retry reads the segment the newer manifest lists"
    );
}
