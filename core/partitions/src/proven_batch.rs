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

//! A committed placed batch, put back together from the rows of
//! self-describing fresh objects.
//!
//! A proof row binds a committed batch to the offset and the timestamp it
//! committed at, and to the checksum of the placed batch the partition
//! committed. The rest of that placed batch is in the object that holds the
//! blob: its batch row has the fields of the batch header that the producer
//! set, and its partition's directory row has the `proven_below` that the
//! placement carried. Together they give the placed batch back byte for
//! byte, and its checksum says whether they do. A reader in another cluster
//! then fetches the blob and puts the whole batch back together as the
//! partition's own polls do.

use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchProducer};
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_object_store::{
    FreshBatchRow, FreshDirectoryRow, FreshIndex, FreshObjectId, FreshProofRow,
};
use server_common::sharding::IggyNamespace;

/// A proof row with the rows of the object that holds its batch's blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvenBatch<'a> {
    /// The proof row, from any object.
    pub proof: &'a FreshProofRow,
    /// The directory row of the proof's partition in the object that holds
    /// the blob.
    pub source: &'a FreshDirectoryRow,
    /// The row of the batch whose blob starts at the proof's position in
    /// that object.
    pub batch: &'a FreshBatchRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProvenBatchError {
    #[error("the proof names object {named:?}, and the rows are of object {found:?}")]
    WrongSource {
        named: FreshObjectId,
        found: FreshObjectId,
    },
    #[error("no batch of namespace {namespace} starts at {position} of the source object")]
    NoBatch { namespace: u64, position: u64 },
    #[error(
        "the source object holds namespace {namespace} of revision {found}, and the proof is of revision {expected}"
    )]
    OtherIncarnation {
        namespace: u64,
        expected: u64,
        found: u64,
    },
    #[error("the proof counts {proof} messages, and the batch row {batch}")]
    MessageCount { proof: u32, batch: u32 },
    #[error("the batch row's producer is outside what a batch header carries")]
    InvalidProducer,
    #[error("the rebuilt placed batch has checksum {rebuilt:#018x}, and the proof {proven:#018x}")]
    ChecksumMismatch { rebuilt: u64, proven: u64 },
}

impl<'a> ProvenBatch<'a> {
    /// The rows that `proof`, a proof row of the partition of `partition` in
    /// any object, names in `source`, the index of the object that holds its
    /// blob.
    ///
    /// # Errors
    /// [`ProvenBatchError::WrongSource`] when `source` is not the object the
    /// proof names, [`ProvenBatchError::NoBatch`] when no batch of the
    /// partition starts at the proof's position in it, and
    /// [`ProvenBatchError::OtherIncarnation`] when the partition there is
    /// another incarnation under the same namespace.
    pub fn find(
        proof: &'a FreshProofRow,
        partition: &FreshDirectoryRow,
        source: &'a FreshIndex,
    ) -> Result<Self, ProvenBatchError> {
        let found = source.header().object_id;
        if found != proof.object_id {
            return Err(ProvenBatchError::WrongSource {
                named: proof.object_id,
                found,
            });
        }
        let namespace = partition.namespace;
        let (row, batch) =
            source
                .batch_at(namespace, proof.position)
                .ok_or(ProvenBatchError::NoBatch {
                    namespace,
                    position: proof.position,
                })?;
        if row.created_revision != partition.created_revision {
            return Err(ProvenBatchError::OtherIncarnation {
                namespace,
                expected: partition.created_revision,
                found: row.created_revision,
            });
        }
        Ok(Self {
            proof,
            source: row,
            batch,
        })
    }
}

impl TryFrom<ProvenBatch<'_>> for PlacedBatch {
    type Error = ProvenBatchError;

    /// The placed batch the partition committed, once its checksum matches
    /// the one the proof carries.
    fn try_from(proven: ProvenBatch<'_>) -> Result<Self, Self::Error> {
        let ProvenBatch {
            proof,
            source,
            batch,
        } = proven;
        if batch.message_count != proof.message_count {
            return Err(ProvenBatchError::MessageCount {
                proof: proof.message_count,
                batch: batch.message_count,
            });
        }
        let producer = batch
            .producer
            .map(|producer| {
                BatchProducer::new(producer.producer_id, producer.epoch, producer.base_sequence)
                    .ok_or(ProvenBatchError::InvalidProducer)
            })
            .transpose()?;
        let length = u64::from(batch.blob_length);
        let placed = Self::new(
            BatchHeader {
                partition_id: IggyNamespace::from_raw(source.namespace).partition_id() as u64,
                base_offset: proof.base_offset,
                base_timestamp: proof.base_timestamp,
                origin_timestamp: batch.origin_timestamp,
                batch_length: BATCH_HEADER_SIZE as u64 + length,
                batch_checksum: 0,
                message_count: batch.message_count,
                producer,
            },
            Placement {
                object_id: proof.object_id.get(),
                position: proof.position,
                length,
                blob_checksum: batch.blob_checksum,
                proven_below: source.proven_below,
            },
        );
        let mut bytes = [0; PLACED_BATCH_SIZE];
        placed.encode_into(&mut bytes);
        let rebuilt = placement::checksum_of(&bytes);
        if rebuilt != proof.placed_checksum {
            return Err(ProvenBatchError::ChecksumMismatch {
                rebuilt,
                proven: proof.placed_checksum,
            });
        }
        Ok(placed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy_binary_protocol::{Command, Operation, PrepareHeader, RoutedRequestHeader};
    use iggy_object_store::{
        FreshBatch, FreshObjectHeader, FreshPartition, FreshProducer, SealedFreshObject,
    };
    use server_common::Message;
    use server_common::send_messages::{
        ChecksumMode, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
        admitted_blob, calculate_batch_checksum, convert_request_message, place_request,
        stamp_prepare_for_persistence,
    };

    const NOW: u64 = 1_700_000_000_000_000;
    const CLUSTER: u128 = 0xfeed;

    fn first() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    fn second() -> IggyNamespace {
        IggyNamespace::new(1, 1, 3)
    }

    fn id(sequence: u64) -> FreshObjectId {
        FreshObjectId::new(0, 1, NOW, sequence).unwrap()
    }

    /// A send as the shard admits it, in its pipeline form.
    fn admitted(
        namespace: IggyNamespace,
        count: u32,
        producer: Option<BatchProducer>,
    ) -> Message<RoutedRequestHeader> {
        let mut messages = IggyMessages::with_capacity(count as usize);
        for index in 0..count {
            let payload = Bytes::from(format!("message {index} of {count}"));
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    origin_timestamp: NOW + u64::from(index),
                    payload_length: u32::try_from(payload.len()).unwrap(),
                    ..IggyMessageHeader::default()
                },
                payload,
                user_headers: None,
            });
        }
        let mut owned = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
        owned.header.producer = producer;
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
            header.request = u64::from(count);
            header.group = namespace.inner();
            header.size = u32::try_from(total).unwrap();
        });
        convert_request_message(namespace, request, ChecksumMode::Skip).unwrap()
    }

    /// The placed batch a partition commits for `placed`, stamped at
    /// prepare as the partition stamps it.
    fn committed(
        placed: Message<RoutedRequestHeader>,
        base_offset: u64,
        base_timestamp: u64,
    ) -> [u8; PLACED_BATCH_SIZE] {
        let size = placed.header().size;
        let prepare = placed.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::SendMessages;
            header.size = size;
        });
        let (stamped, ..) =
            stamp_prepare_for_persistence(prepare, base_offset, base_timestamp).unwrap();
        let start = size_of::<PrepareHeader>();
        let mut bytes = [0; PLACED_BATCH_SIZE];
        bytes.copy_from_slice(&stamped.as_slice()[start..start + PLACED_BATCH_SIZE]);
        bytes
    }

    fn proof(object: FreshObjectId, position: u64, committed: &[u8]) -> FreshProofRow {
        let batch = PlacedBatch::decode(committed).unwrap();
        FreshProofRow {
            object_id: object,
            position,
            base_offset: batch.header.base_offset,
            base_timestamp: batch.header.base_timestamp,
            message_count: batch.header.message_count,
            placed_checksum: placement::checksum_of(committed),
        }
    }

    fn partition(
        namespace: IggyNamespace,
        proven_below: u64,
        batches: Vec<FreshBatch<Vec<u8>>>,
        proofs: Vec<FreshProofRow>,
    ) -> FreshPartition<Vec<u8>> {
        FreshPartition {
            namespace: namespace.inner(),
            created_revision: 4096,
            view: 2,
            claimed_first_offset: 0,
            proven_below,
            batches,
            proofs,
        }
    }

    /// What an origin stored and committed: an object with three sends of
    /// two partitions, one of an idempotent producer, and a later object
    /// that proves all three, as a reader fetches both.
    struct Origin {
        source: Bytes,
        proving: Bytes,
        /// The committed placed batches, in the order of the proofs.
        committed: Vec<[u8; PLACED_BATCH_SIZE]>,
    }

    fn origin() -> Origin {
        let producer = BatchProducer::new(42, 7, 1_000);
        let sends = [
            admitted(first(), 2, None),
            admitted(second(), 3, producer),
            admitted(first(), 1, None),
        ];
        let batch = |send: &Message<RoutedRequestHeader>| {
            let header =
                BatchHeader::decode(&send.as_slice()[size_of::<RoutedRequestHeader>()..]).unwrap();
            let blob = admitted_blob(send).unwrap();
            FreshBatch {
                message_count: header.message_count,
                origin_timestamp: header.origin_timestamp,
                blob_checksum: placement::blob_checksum(blob),
                producer: header.producer.map(|producer| FreshProducer {
                    producer_id: producer.producer_id(),
                    epoch: producer.epoch(),
                    base_sequence: producer.base_sequence(),
                }),
                blob: blob.to_vec(),
            }
        };
        let source = SealedFreshObject::seal(
            FreshObjectHeader {
                object_id: id(4),
                cluster_uid: CLUSTER,
                sealed_at: NOW,
            },
            &[
                partition(
                    first(),
                    40,
                    vec![batch(&sends[0]), batch(&sends[2])],
                    Vec::new(),
                ),
                partition(second(), 7, vec![batch(&sends[1])], Vec::new()),
            ],
        )
        .unwrap();
        let positions = source.index.positions();
        let placed = |send: &Message<RoutedRequestHeader>, position: u64, proven_below| {
            place_request(send, id(4).get(), position, proven_below).unwrap()
        };
        let committed = vec![
            committed(placed(&sends[0], positions[0], 40), 100, NOW + 10),
            committed(placed(&sends[2], positions[1], 40), 102, NOW + 12),
            committed(placed(&sends[1], positions[2], 7), 7, NOW + 11),
        ];
        let proving = SealedFreshObject::seal(
            FreshObjectHeader {
                object_id: id(5),
                cluster_uid: CLUSTER,
                sealed_at: NOW + 20,
            },
            &[
                partition(
                    first(),
                    100,
                    Vec::new(),
                    vec![
                        proof(id(4), positions[0], &committed[0]),
                        proof(id(4), positions[1], &committed[1]),
                    ],
                ),
                partition(
                    second(),
                    7,
                    Vec::new(),
                    vec![proof(id(4), positions[2], &committed[2])],
                ),
            ],
        )
        .unwrap();
        Origin {
            source: source.bytes,
            proving: proving.bytes,
            committed,
        }
    }

    fn indexes(origin: &Origin) -> (FreshIndex, FreshIndex) {
        (
            FreshIndex::decode(&origin.source, id(4)).unwrap(),
            FreshIndex::decode(&origin.proving, id(5)).unwrap(),
        )
    }

    /// Each proof of `proving` with the directory row it is under.
    fn proofs(proving: &FreshIndex) -> Vec<(&FreshDirectoryRow, &FreshProofRow)> {
        proving
            .directory()
            .iter()
            .flat_map(|row| proving.proofs_of(row).iter().map(move |proof| (row, proof)))
            .collect()
    }

    #[test]
    fn given_batches_an_origin_committed_when_rebuilding_them_from_rows_should_give_each_byte_for_byte()
     {
        let origin = origin();
        let (source, proving) = indexes(&origin);

        let proofs = proofs(&proving);

        assert_eq!(proofs.len(), origin.committed.len());
        for ((row, proof), committed) in proofs.into_iter().zip(&origin.committed) {
            let rebuilt =
                PlacedBatch::try_from(ProvenBatch::find(proof, row, &source).unwrap()).unwrap();
            let mut bytes = [0; PLACED_BATCH_SIZE];
            rebuilt.encode_into(&mut bytes);
            assert_eq!(&bytes, committed);
            assert_eq!(PlacedBatch::decode(committed), Ok(rebuilt));
            let start = usize::try_from(rebuilt.placement.position).unwrap();
            let end = start + usize::try_from(rebuilt.placement.length).unwrap();
            assert!(rebuilt.placement.matches(&origin.source[start..end]));
        }
        let producer = PlacedBatch::decode(&origin.committed[2])
            .unwrap()
            .header
            .producer;
        assert_eq!(producer, BatchProducer::new(42, 7, 1_000));
        assert_eq!(
            PlacedBatch::decode(&origin.committed[0])
                .unwrap()
                .placement
                .proven_below,
            40
        );
    }

    #[test]
    fn given_rows_that_differ_from_what_committed_when_rebuilding_should_refuse_them() {
        let origin = origin();
        let (source, proving) = indexes(&origin);
        let (row, proof) = proofs(&proving)[0];
        let proven = ProvenBatch::find(proof, row, &source).unwrap();
        let shifted = |change: fn(&mut FreshProofRow)| {
            let mut proof = *proof;
            change(&mut proof);
            proof
        };
        let proofs = [
            shifted(|proof| proof.base_offset += 1),
            shifted(|proof| proof.base_timestamp += 1),
            shifted(|proof| proof.placed_checksum ^= 1),
        ];
        let mut rows = [*proven.source; 2];
        rows[0].proven_below += 1;
        rows[1].namespace = second().inner();
        let mut batches = [*proven.batch; 2];
        batches[0].origin_timestamp += 1;
        batches[1].blob_checksum ^= 1;

        let rebuilt = proofs
            .iter()
            .map(|proof| ProvenBatch { proof, ..proven })
            .chain(rows.iter().map(|source| ProvenBatch { source, ..proven }))
            .chain(batches.iter().map(|batch| ProvenBatch { batch, ..proven }))
            .map(PlacedBatch::try_from);

        for (case, rebuilt) in rebuilt.enumerate() {
            assert!(
                matches!(rebuilt, Err(ProvenBatchError::ChecksumMismatch { .. })),
                "case {case}: {rebuilt:?}"
            );
        }
        let mut more = *proof;
        more.message_count += 1;
        assert_eq!(
            PlacedBatch::try_from(ProvenBatch {
                proof: &more,
                ..proven
            }),
            Err(ProvenBatchError::MessageCount {
                proof: proven.batch.message_count + 1,
                batch: proven.batch.message_count,
            })
        );
    }

    #[test]
    fn given_a_producer_no_batch_header_carries_when_rebuilding_should_refuse_it() {
        let origin = origin();
        let (source, proving) = indexes(&origin);
        let (row, proof) = proofs(&proving)[2];
        let proven = ProvenBatch::find(proof, row, &source).unwrap();
        let mut batch = *proven.batch;
        batch.producer = Some(FreshProducer {
            producer_id: 42,
            epoch: u16::MAX,
            base_sequence: 1_000,
        });

        assert_eq!(
            PlacedBatch::try_from(ProvenBatch {
                batch: &batch,
                ..proven
            }),
            Err(ProvenBatchError::InvalidProducer)
        );
    }

    #[test]
    fn given_a_proof_that_does_not_point_at_a_batch_of_its_partition_when_finding_should_refuse_it()
    {
        let origin = origin();
        let (source, proving) = indexes(&origin);
        let (row, proof) = proofs(&proving)[0];
        let mut elsewhere = *proof;
        elsewhere.position += 1;
        let mut recreated = *row;
        recreated.created_revision += 1;
        let mut other_partition = *row;
        other_partition.namespace = second().inner();

        assert_eq!(
            ProvenBatch::find(proof, row, &proving),
            Err(ProvenBatchError::WrongSource {
                named: id(4),
                found: id(5),
            })
        );
        assert_eq!(
            ProvenBatch::find(&elsewhere, row, &source),
            Err(ProvenBatchError::NoBatch {
                namespace: first().inner(),
                position: proof.position + 1,
            })
        );
        assert_eq!(
            ProvenBatch::find(proof, &other_partition, &source),
            Err(ProvenBatchError::NoBatch {
                namespace: second().inner(),
                position: proof.position,
            })
        );
        assert_eq!(
            ProvenBatch::find(proof, &recreated, &source),
            Err(ProvenBatchError::OtherIncarnation {
                namespace: first().inner(),
                expected: 4097,
                found: 4096,
            })
        );
    }
}
