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

use iggy::prelude::*;
use iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE;
use integration::iggy_harness;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

const STREAM_NAME: &str = "decode-diagnostics-stream";
const TOPIC_NAME: &str = "decode-diagnostics-topic";
const CONSUMER_NAME: &str = "decode-diagnostics-consumer";
const PARTITION_ID: u32 = 0;
const PAYLOADS: [&str; 3] = ["first", "second", "third"];
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// A reader with another client-side key than the producer cannot decrypt the messages. Its
/// error names the first message and carries its record as served, and a seek to the skip
/// passes it.
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_a_reader_with_another_key_when_reading_should_report_the_record_as_served(
    harness: &TestHarness,
) {
    let transport = harness.transport().unwrap();
    let producer_key = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&[42; 32]).unwrap(),
    ));
    let reader_key = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&[43; 32]).unwrap(),
    ));
    let producer = harness
        .client_builder_for(transport)
        .unwrap()
        .with_encryptor(producer_key)
        .with_root_login()
        .connect()
        .await
        .unwrap();
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    producer
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    producer
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut messages: Vec<IggyMessage> = PAYLOADS
        .iter()
        .map(|payload| IggyMessage::from_str(payload).unwrap())
        .collect();
    producer
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // The ciphertext records as the server serves them, read without any key.
    let plain = harness.root_client().await.unwrap();
    let served = plain
        .poll_messages(
            &stream_id,
            &topic_id,
            PARTITION_ID,
            &Consumer::new(Identifier::named(CONSUMER_NAME).unwrap()),
            &PollingStrategy::offset(0),
            PollOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(served.messages.len(), PAYLOADS.len());

    let reader_client = harness
        .client_builder_for(transport)
        .unwrap()
        .with_encryptor(reader_key)
        .with_root_login()
        .connect()
        .await
        .unwrap();
    let mut reader = reader_client
        .reader(
            &stream_id,
            &topic_id,
            &Consumer::new(Identifier::named(CONSUMER_NAME).unwrap()),
        )
        .partitions([PARTITION_ID])
        .start(PollingStrategy::first())
        .build()
        .await
        .unwrap();

    let error = timeout(READ_TIMEOUT, reader.next())
        .await
        .expect("the reader answers")
        .expect_err("a message that does not decrypt is an error");
    assert_eq!(error.partition_id, Some(PARTITION_ID));
    assert_eq!(error.error, IggyError::CannotDecryptData);
    assert_eq!(error.offset, Some(0), "the first message does not decrypt");
    let record = error.record.expect("the record as served");
    let ciphertext = &served.messages[0];
    assert_eq!(
        record.len(),
        BATCH_MESSAGE_HEADER_SIZE + ciphertext.payload.len(),
        "the record is the frame header and the ciphertext"
    );
    assert_eq!(
        &record[BATCH_MESSAGE_HEADER_SIZE..],
        &ciphertext.payload[..],
        "the record carries the ciphertext, not a reserialized message"
    );
    let skip = error
        .skip
        .expect("a message that does not decode has a skip");
    assert_eq!(skip.value, 1);

    reader.seek(PARTITION_ID, skip).await.unwrap();
    let error = timeout(READ_TIMEOUT, reader.next())
        .await
        .expect("the reader answers")
        .expect_err("the second message does not decrypt either");
    assert_eq!(error.offset, Some(1), "the seek passed the first message");
    reader.close().await.unwrap();
}
