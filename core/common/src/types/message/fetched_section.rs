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

use super::polled_messages::messages_from_batches;
use crate::{EncryptorKind, IggyError, PolledMessages, PollingStrategy};
use bytes::Bytes;
use iggy_binary_protocol::STATUS_OK;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::responses::messages::{
    FetchMessagesSectionHeader, FetchMessagesSectionIterator,
};
use std::sync::Arc;

/// The outcome of one target of a `FetchMessages` request: the served
/// messages, still encoded, or the refusal of its partition. The table fields
/// are known before [`Self::decode`] walks the records.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct FetchedSection {
    pub partition_id: u32,
    /// The refusal of this partition, such as `NotPrimary` after a move. A
    /// refused section has no records.
    pub status: Result<(), IggyError>,
    /// The current offset of the partition.
    pub current_offset: u64,
    pub messages_count: u32,
    /// The partition history that served the section. A later fetch fences
    /// the partition with it through the [continuation](PolledMessages::continuation)
    /// of the decoded section.
    pub context: PartitionContext,
    /// Where the next fetch of the partition starts, also after an empty
    /// section.
    pub next_offset: u64,
    /// The batch records of the served messages.
    pub records: Bytes,
    encryptor: Option<Arc<EncryptorKind>>,
}

impl FetchedSection {
    /// The sections of a fetch reply body, in reply order. Their records
    /// share the buffer of `body`.
    ///
    /// # Errors
    /// [`IggyError::InvalidFormat`] for a body that is not a fetch reply.
    pub fn from_reply(body: &Bytes) -> Result<Vec<Self>, IggyError> {
        let sections =
            FetchMessagesSectionIterator::new(body).map_err(|_| IggyError::InvalidFormat)?;
        Ok(sections
            .map(|section| Self::new(section.header, body.slice_ref(section.records)))
            .collect())
    }

    /// [`Self::decode`] then decrypts the messages with `encryptor`.
    pub fn with_encryptor(mut self, encryptor: Option<Arc<EncryptorKind>>) -> Self {
        self.encryptor = encryptor;
        self
    }

    /// The messages of a served section. Its continuation starts at
    /// `next_offset`, which is also the resolved start of an empty section.
    ///
    /// # Errors
    /// The refusal of a refused section, a decode error of
    /// [`PolledMessages::from_bytes`], or a decryption error.
    pub fn decode(&self) -> Result<PolledMessages, IggyError> {
        self.status.clone()?;
        let mut messages = messages_from_batches(self.records.clone(), self.messages_count)?;
        if let Some(encryptor) = &self.encryptor {
            for message in &mut messages {
                encryptor.decrypt_message(message)?;
            }
        }
        let mut polled = PolledMessages::new(
            self.context,
            self.partition_id,
            self.current_offset,
            self.messages_count,
            messages,
        );
        polled.set_request_start(&PollingStrategy::offset(self.next_offset));
        Ok(polled)
    }

    fn new(header: FetchMessagesSectionHeader, records: Bytes) -> Self {
        Self {
            partition_id: header.partition_id,
            status: match header.status {
                STATUS_OK => Ok(()),
                status => Err(IggyError::from_code(status)),
            },
            current_offset: header.current_offset,
            messages_count: header.messages_count,
            context: header.context,
            next_offset: header.next_offset,
            records,
            encryptor: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Aes256GcmEncryptor, PollingKind};
    use bytes::BytesMut;
    use iggy_binary_protocol::batch::BatchHeader;
    use iggy_binary_protocol::requests::messages::{RawMessage, SendMessagesEncoder};
    use iggy_binary_protocol::responses::messages::{FetchMessagesResponse, FetchMessagesSection};
    use iggy_binary_protocol::{WireIdentifier, WirePartitioning};

    const SERVED_PARTITION: u32 = 1;
    const MOVED_PARTITION: u32 = 2;
    const BASE_OFFSET: u64 = 41;
    const NEXT_OFFSET: u64 = 43;
    const CONTEXT: PartitionContext = PartitionContext {
        incarnation: 3,
        owner_generation: 9,
        metadata_op: 12,
    };
    const PAYLOADS: [&[u8]; 2] = [b"first", b"second"];
    const KEY: [u8; 32] = [7; 32];

    /// One batch of `payloads` from `BASE_OFFSET`, as a section carries it.
    fn records(payloads: &[&[u8]]) -> Bytes {
        let messages: Vec<_> = (1..)
            .zip(payloads.iter().copied())
            .map(|(id, payload)| RawMessage {
                id,
                origin_timestamp: 0,
                headers: None,
                payload,
            })
            .collect();
        let mut send = BytesMut::new();
        SendMessagesEncoder::encode(
            &mut send,
            &WireIdentifier::numeric(1),
            &WireIdentifier::numeric(1),
            &WirePartitioning::PartitionId(SERVED_PARTITION),
            &messages,
        )
        .unwrap();
        let metadata_len = u32::from_le_bytes(send[..size_of::<u32>()].try_into().unwrap());
        let mut record = send.split_off(size_of::<u32>() + metadata_len as usize);
        let mut header = BatchHeader::decode(&record).unwrap();
        header.base_offset = BASE_OFFSET;
        header.encode_into(&mut record);
        record.freeze()
    }

    fn reply(records: &[u8], messages_count: u32) -> Bytes {
        FetchMessagesResponse {
            sections: vec![
                FetchMessagesSection {
                    header: FetchMessagesSectionHeader {
                        partition_id: SERVED_PARTITION,
                        status: STATUS_OK,
                        current_offset: NEXT_OFFSET - 1,
                        messages_count,
                        context: CONTEXT,
                        next_offset: NEXT_OFFSET,
                        records_len: u32::try_from(records.len()).unwrap(),
                    },
                    records,
                },
                FetchMessagesSection {
                    header: FetchMessagesSectionHeader {
                        partition_id: MOVED_PARTITION,
                        status: IggyError::NotPrimary.as_code(),
                        current_offset: 0,
                        messages_count: 0,
                        context: PartitionContext::default(),
                        next_offset: 0,
                        records_len: 0,
                    },
                    records: &[],
                },
            ],
        }
        .to_bytes()
        .unwrap()
    }

    fn payloads(polled: &PolledMessages) -> Vec<&[u8]> {
        polled
            .messages
            .iter()
            .map(|message| &message.payload[..])
            .collect()
    }

    #[test]
    fn given_a_fetch_reply_when_read_should_keep_the_table_and_decode_each_section() {
        let records = records(&PAYLOADS);
        let sections = FetchedSection::from_reply(&reply(&records, 2)).unwrap();

        let [served, moved] = sections.as_slice() else {
            panic!("expected two sections: {sections:?}");
        };
        assert_eq!(
            (
                served.partition_id,
                &served.status,
                served.next_offset,
                served.messages_count
            ),
            (SERVED_PARTITION, &Ok(()), NEXT_OFFSET, 2)
        );
        assert_eq!(served.records, records);
        let polled = served.decode().unwrap();
        assert_eq!(
            (polled.partition_id, polled.current_offset, polled.context()),
            (SERVED_PARTITION, NEXT_OFFSET - 1, CONTEXT)
        );
        assert_eq!(payloads(&polled), PAYLOADS);
        assert_eq!(
            polled
                .messages
                .iter()
                .map(|message| message.header.offset)
                .collect::<Vec<_>>(),
            [BASE_OFFSET, BASE_OFFSET + 1]
        );

        assert_eq!(moved.partition_id, MOVED_PARTITION);
        assert!(matches!(moved.status, Err(IggyError::NotPrimary)));
        assert!(moved.records.is_empty());
        assert!(matches!(moved.decode(), Err(IggyError::NotPrimary)));
    }

    #[test]
    fn given_a_served_section_when_decoded_should_continue_at_its_next_offset() {
        let expected = PollingStrategy::continuation(
            SERVED_PARTITION,
            PollingKind::Offset,
            NEXT_OFFSET,
            CONTEXT,
        );
        for (records, messages_count) in [(records(&PAYLOADS), 2), (Bytes::new(), 0)] {
            let sections = FetchedSection::from_reply(&reply(&records, messages_count)).unwrap();

            let polled = sections[0].decode().unwrap();

            assert_eq!(polled.continuation(), expected, "{messages_count} messages");
        }
    }

    #[test]
    fn given_a_section_whose_count_differs_from_its_records_when_decoded_should_fail() {
        let records = records(&PAYLOADS);
        let sections = FetchedSection::from_reply(&reply(&records, 1)).unwrap();

        assert!(matches!(
            sections[0].decode(),
            Err(IggyError::InvalidMessagesCount)
        ));
    }

    #[test]
    fn given_a_truncated_reply_when_read_should_fail() {
        let reply = reply(&records(&PAYLOADS), 2);

        for end in 0..reply.len() {
            assert!(
                matches!(
                    FetchedSection::from_reply(&reply.slice(..end)),
                    Err(IggyError::InvalidFormat)
                ),
                "accepted truncation at {end}"
            );
        }
    }

    #[test]
    fn given_an_encryptor_when_decoding_should_return_the_plaintext() {
        let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&KEY).unwrap(),
        ));
        let encrypted: Vec<_> = PAYLOADS
            .iter()
            .map(|payload| encryptor.encrypt(payload).unwrap())
            .collect();
        let encrypted: Vec<&[u8]> = encrypted.iter().map(Vec::as_slice).collect();
        let records = records(&encrypted);
        let section = FetchedSection::from_reply(&reply(&records, 2))
            .unwrap()
            .remove(0);

        let undecrypted = section.decode().unwrap();
        let decrypted = section.with_encryptor(Some(encryptor)).decode().unwrap();

        assert_eq!(payloads(&undecrypted), encrypted);
        assert_eq!(payloads(&decrypted), PAYLOADS);
        assert_eq!(
            decrypted.messages[1].header.payload_length as usize,
            PAYLOADS[1].len()
        );
    }
}
