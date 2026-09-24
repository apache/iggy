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

use iggy_connector_sdk::{ConnectorState, ProducedMessages, Schema};

use crate::framing::FramedRecord;

pub(crate) fn build_produced_messages(
    records: Vec<FramedRecord>,
    endpoint: &str,
    bucket: &str,
    key: &str,
    etag: &str,
    state: ConnectorState,
) -> ProducedMessages {
    let identity = MessageIdentity::new(endpoint, bucket, key, etag);
    let messages = records
        .into_iter()
        .map(|record| {
            let message_id = identity.id_at(record.start_offset);
            record.into_produced_message(message_id)
        })
        .collect();

    ProducedMessages {
        schema: Schema::Raw,
        messages,
        state: Some(state),
    }
}

struct MessageIdentity(blake3::Hasher);

impl MessageIdentity {
    fn new(endpoint: &str, bucket: &str, key: &str, etag: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        for part in [endpoint, bucket, key, etag] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        Self(hasher)
    }

    fn id_at(&self, record_start_offset: u64) -> u128 {
        let mut hasher = self.0.clone();
        hasher.update(&record_start_offset.to_le_bytes());
        let digest = hasher.finalize();
        let mut id_bytes = [0_u8; 16];
        id_bytes.copy_from_slice(&digest.as_bytes()[..16]);
        u128::from_le_bytes(id_bytes)
    }
}

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::{ConnectorState, Schema};

    use super::MessageIdentity;
    use crate::framing::FramedRecord;

    fn stable_message_id(
        endpoint: &str,
        bucket: &str,
        key: &str,
        etag: &str,
        record_start_offset: u64,
    ) -> u128 {
        MessageIdentity::new(endpoint, bucket, key, etag).id_at(record_start_offset)
    }

    #[test]
    fn given_cached_identity_when_hashing_offsets_should_preserve_original_ids() {
        for key in ["logs/a".to_string(), "x".repeat(1_024)] {
            let identity = MessageIdentity::new("aws", "events", &key, "etag");
            for offset in [0, 1, 63, 64, 1_024, u64::MAX] {
                assert_eq!(
                    identity.id_at(offset),
                    original_message_id("aws", "events", &key, "etag", offset)
                );
            }
        }
    }

    // Keep the original algorithm independent to catch changes to replay IDs.
    fn original_message_id(
        endpoint: &str,
        bucket: &str,
        key: &str,
        etag: &str,
        record_start_offset: u64,
    ) -> u128 {
        let mut hasher = blake3::Hasher::new();
        for part in [endpoint, bucket, key, etag] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        hasher.update(&record_start_offset.to_le_bytes());
        let digest = hasher.finalize();
        let mut id_bytes = [0_u8; 16];
        id_bytes.copy_from_slice(&digest.as_bytes()[..16]);
        u128::from_le_bytes(id_bytes)
    }

    #[test]
    fn given_framed_record_when_converted_should_preserve_payload_and_set_message_fields() {
        let record = FramedRecord {
            payload: b"\xFFevent".to_vec(),
            start_offset: 20,
        };

        let message = record.into_produced_message(42);

        assert_eq!(message.id, Some(42));
        assert_eq!(message.payload, b"\xFFevent");
        assert_eq!(message.headers, None);
        assert_eq!(message.checksum, None);
        assert_eq!(message.timestamp, None);
        assert_eq!(message.origin_timestamp, None);
    }

    #[test]
    fn given_same_object_and_record_when_id_generated_should_return_same_id() {
        let first = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            20,
        );
        let second = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            20,
        );

        assert_eq!(first, second);
    }

    #[test]
    fn given_different_record_offsets_when_ids_generated_should_return_different_ids() {
        let first = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            20,
        );
        let second = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            27,
        );

        assert_ne!(first, second);
    }

    #[test]
    fn given_different_object_versions_when_ids_generated_should_return_different_ids() {
        let first = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            20,
        );
        let second = stable_message_id(
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "def456",
            20,
        );

        assert_ne!(first, second);
    }

    #[test]
    fn given_ambiguous_identity_parts_when_ids_generated_should_return_different_ids() {
        let first = stable_message_id("ab", "c", "key", "etag", 20);
        let second = stable_message_id("a", "bc", "key", "etag", 20);

        assert_ne!(first, second);
    }

    #[test]
    fn given_framed_records_when_batch_built_should_preserve_order_ids_and_raw_schema() {
        let records = vec![
            FramedRecord {
                payload: b"first".to_vec(),
                start_offset: 0,
            },
            FramedRecord {
                payload: b"second".to_vec(),
                start_offset: 6,
            },
        ];

        let batch = super::build_produced_messages(
            records,
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            ConnectorState(vec![1, 2, 3]),
        );

        assert_eq!(batch.schema, Schema::Raw);
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].payload, b"first");
        assert_eq!(batch.messages[1].payload, b"second");
        assert_eq!(
            batch.messages[0].id,
            Some(stable_message_id(
                "https://s3.example.com",
                "events",
                "logs/events.jsonl",
                "abc123",
                0,
            ))
        );
        assert_eq!(
            batch.messages[1].id,
            Some(stable_message_id(
                "https://s3.example.com",
                "events",
                "logs/events.jsonl",
                "abc123",
                6,
            ))
        );
        assert_eq!(
            batch.state.as_ref().map(|state| state.0.as_slice()),
            Some([1, 2, 3].as_slice())
        );
    }

    #[test]
    fn given_same_record_in_different_batches_when_built_should_keep_same_id() {
        let single_record_batch = super::build_produced_messages(
            vec![FramedRecord {
                payload: b"second".to_vec(),
                start_offset: 6,
            }],
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            ConnectorState(vec![1]),
        );
        let multi_record_batch = super::build_produced_messages(
            vec![
                FramedRecord {
                    payload: b"first".to_vec(),
                    start_offset: 0,
                },
                FramedRecord {
                    payload: b"second".to_vec(),
                    start_offset: 6,
                },
            ],
            "https://s3.example.com",
            "events",
            "logs/events.jsonl",
            "abc123",
            ConnectorState(vec![2]),
        );

        assert_eq!(
            single_record_batch.messages[0].id,
            multi_record_batch.messages[1].id
        );
    }
}
