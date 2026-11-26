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

#[test]
fn eviction_reason_splits_client_and_internal_ops() {
    // Client-allowed op with a bad body evicts InvalidRequestBody; an internal
    // / unknown op (here `CreateTopicWithAssignments`) evicts
    // InvalidRequestOperation.
    assert_eq!(
        eviction_reason_for_invalid(Operation::CreateStream),
        EvictionReason::InvalidRequestBody,
    );
    assert_eq!(
        eviction_reason_for_invalid(Operation::CreateTopicWithAssignments),
        EvictionReason::InvalidRequestOperation,
    );
}

#[test]
fn populated_snapshot_reencode_and_checksum_are_stable() {
    // Two replicas holding identical state must serialize it identically, which
    // holds only while every serialized collection keeps a deterministic order.
    // Fold a multi-slot client table (the envelope's own Vec) and assert the raw
    // re-encode survives the round-trip. The streams sub-tree's map-derived Vecs are
    // covered by
    // `crate::stm::stream::tests::populated_streams_snapshot_reencode_is_byte_stable`.
    let mut snapshot = IggySnapshot::new(7);
    snapshot.snapshot.client_table = Some(two_session_client_table());

    let encoded = snapshot.encode().unwrap();
    let decoded = IggySnapshot::decode(&encoded).unwrap();
    assert_eq!(
        encoded,
        decoded.encode().unwrap(),
        "a populated snapshot must re-encode byte-identically after a decode; an \
         unordered collection in the serialized form would make two replicas with \
         identical state serialize differently"
    );
    assert_ne!(
        checkpoint_checksum(&encoded),
        checkpoint_checksum(&IggySnapshot::new(7).encode().unwrap()),
        "the checksum must track content, else it could not detect a torn snapshot"
    );
}

#[test]
fn client_table_reply_bytes_encode_as_a_msgpack_blob() {
    // A `Vec<u8>` serialized through serde's sequence path spends 2 bytes on every
    // byte >= 0x80, so a checkpoint's reply payload runs up to roughly double.
    // Reply bytes are wire messages, mostly high bytes, so pin the `bin` encoding:
    // the format freezes at release and this is much cheaper to fix now.
    const REPLY_LEN: usize = 512;
    let mut snapshot = IggySnapshot::new(1);
    snapshot.snapshot.client_table = Some(consensus::ClientTableSnapshot {
        capacity: 4,
        slots: vec![(
            0,
            consensus::ClientEntrySnapshot {
                bind_verifier: [0x5a; 32],
                ended_op: None,
                client_id: 1,
                epoch: 1,
                user_id: 1,
                token_id: 0,
                watermark: 0,
                reply: vec![0xFF; REPLY_LEN],
            },
        )],
    });

    let encoded = snapshot.encode().unwrap();
    let baseline = IggySnapshot::new(1).encode().unwrap().len();
    let reply_cost = encoded.len() - baseline;
    assert!(
        reply_cost < REPLY_LEN * 2,
        "a {REPLY_LEN}-byte reply of high bytes cost {reply_cost} bytes, so it is \
         still encoding as an integer array rather than a msgpack blob"
    );

    // And it must decode back to the same bytes.
    let decoded = IggySnapshot::decode(&encoded).unwrap();
    let table = decoded.snapshot().client_table.as_ref().unwrap();
    assert_eq!(table.slots[0].1.reply, vec![0xFF; REPLY_LEN]);
}

#[test]
fn take_prepare_gap_drops_drains_the_count() {
    let md = peer_metadata();
    assert_eq!(md.take_prepare_gap_drops(), 0);

    md.prepare_gap_drops.set(2);
    assert_eq!(md.take_prepare_gap_drops(), 2);
    assert_eq!(
        md.take_prepare_gap_drops(),
        0,
        "a second drain must not re-report drops the metrics already counted"
    );
}

#[test]
fn commit_notifier_fires_with_received_operation() {
    let md = peer_metadata();
    let captured: Rc<RefCell<Vec<Operation>>> = Rc::new(RefCell::new(Vec::new()));

    let observer = Rc::clone(&captured);
    md.set_commit_notifier(Some(Rc::new(move |op| {
        observer.borrow_mut().push(op);
    })));

    md.fire_commit_notifier(Operation::CreateTopicWithAssignments);
    md.fire_commit_notifier(Operation::DeletePartitions);
    md.fire_commit_notifier(Operation::DeleteStream);

    let seen = captured.borrow();
    assert_eq!(
        seen.as_slice(),
        &[
            Operation::CreateTopicWithAssignments,
            Operation::DeletePartitions,
            Operation::DeleteStream,
        ],
        "notifier must observe every fired operation in order"
    );
}

#[test]
fn commit_notifier_is_no_op_when_unset() {
    // No notifier installed: firing must not panic, must not allocate.
    // Mirrors the production-side guarantee that peer shards (no
    // notifier) take the same commit path as shard 0 (with notifier).
    let md = peer_metadata();
    md.fire_commit_notifier(Operation::CreateStream);
}

#[test]
fn commit_notifier_can_be_replaced_and_cleared() {
    let md = peer_metadata();
    let first_count: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
    let second_count: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));

    let first_observer = Rc::clone(&first_count);
    md.set_commit_notifier(Some(Rc::new(move |_op| {
        *first_observer.borrow_mut() += 1;
    })));
    md.fire_commit_notifier(Operation::CreateStream);
    assert_eq!(*first_count.borrow(), 1);

    // Replace: the first closure must no longer run.
    let second_observer = Rc::clone(&second_count);
    md.set_commit_notifier(Some(Rc::new(move |_op| {
        *second_observer.borrow_mut() += 1;
    })));
    md.fire_commit_notifier(Operation::DeleteStream);
    assert_eq!(*first_count.borrow(), 1, "old notifier must be detached");
    assert_eq!(*second_count.borrow(), 1, "new notifier must take over");

    // Clear: subsequent fires must be no-ops.
    md.set_commit_notifier(None);
    md.fire_commit_notifier(Operation::DeleteTopic);
    assert_eq!(
        *second_count.borrow(),
        1,
        "cleared notifier must stay quiet"
    );
}
