/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, assert_clean_system, login_root};
use std::collections::HashMap;
use std::str::FromStr;

const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const PARTITION_ID: u32 = 1;
const MAX_SINGLE_HEADER_SIZE: usize = 200;

enum MessageToSend {
    NoMessage,
    OfSize(usize),
    OfSizeWithHeaders(usize, usize),
}

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    login_root(&client).await;
    init_system(&client).await;

    // 3. Send message and check the result
    send_message_and_check_result(
        &client,
        MessageToSend::NoMessage,
        Err(IggyError::InvalidMessagesCount),
    )
    .await;
    send_message_and_check_result(&client, MessageToSend::OfSize(1), Ok(())).await;
    send_message_and_check_result(&client, MessageToSend::OfSize(1_000_000), Ok(())).await;
    send_message_and_check_result(&client, MessageToSend::OfSize(10_000_000), Ok(())).await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(1, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(1_000, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_000, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_001, 10_000_000),
        Err(IggyError::TooBigUserHeaders),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_000, 64_000_001),
        Err(IggyError::TooBigMessagePayload),
    )
    .await;

    assert_message_count(&client, 6).await;
    cleanup_system(&client).await;
    assert_clean_system(&client).await;
}

async fn assert_message_count(client: &IggyClient, expected_count: u32) {
    // 4. Poll messages and validate the count
    let polled_messages = client
        .poll_messages(
            &STREAM_NAME.try_into().unwrap(),
            &TOPIC_NAME.try_into().unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            expected_count * 2,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len() as u32, expected_count);
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic
    client
        .create_topic(
            &STREAM_NAME.try_into().unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

async fn cleanup_system(client: &IggyClient) {
    client
        .delete_stream(&STREAM_NAME.try_into().unwrap())
        .await
        .unwrap();
}

async fn send_message_and_check_result(
    client: &IggyClient,
    message_params: MessageToSend,
    expected_result: Result<(), IggyError>,
) {
    let message_result = match message_params {
        MessageToSend::NoMessage => {
            println!("Sending message without messages inside");
            Ok(Vec::new())
        }
        MessageToSend::OfSize(size) => {
            println!("Sending message with payload size = {size}");
            match create_message(None, size) {
                Ok(msg) => Ok(vec![msg]),
                Err(e) => Err(e),
            }
        }
        MessageToSend::OfSizeWithHeaders(header_size, payload_size) => {
            println!(
                "Sending message with header size = {header_size} and payload size = {payload_size}"
            );
            match create_message(Some(header_size), payload_size) {
                Ok(msg) => Ok(vec![msg]),
                Err(e) => Err(e),
            }
        }
    };

    if let Err(creation_error) = &message_result {
        println!("Message creation failed: {creation_error:?}, expected: {expected_result:?}");
        match expected_result {
            Err(expected_error) if expected_error.as_code() == creation_error.as_code() => {
                return;
            }
            _ => {
                panic!(
                    "Expected {expected_result:?} but message creation failed with {creation_error:?}"
                );
            }
        }
    }

    let mut messages = message_result.unwrap();

    let send_result = client
        .send_messages(
            &STREAM_NAME.try_into().unwrap(),
            &TOPIC_NAME.try_into().unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await;

    println!("Received = {send_result:?}, expected = {expected_result:?}");
    match expected_result {
        Ok(()) => assert!(send_result.is_ok()),
        Err(error) => {
            assert!(send_result.is_err());
            let send_result = send_result.err().unwrap();
            assert_eq!(error.as_code(), send_result.as_code());
            assert_eq!(error.to_string(), send_result.to_string());
        }
    }
}

fn create_string_of_size(size: usize) -> String {
    "x".repeat(size)
}

fn create_message_header_of_size(target_size: usize) -> HashMap<HeaderKey, HeaderValue> {
    let mut headers = HashMap::new();
    let mut header_id = 1;
    let mut current_size = 0;

    while current_size < target_size {
        let remaining_size = target_size - current_size;

        let key_str = format!("header-{header_id}");
        let key_overhead = 4; // 4 bytes for key length
        let value_overhead = 5; // 1 byte for type + 4 bytes for value length
        let total_overhead = key_overhead + key_str.len() + value_overhead;

        let value_size = if remaining_size <= total_overhead {
            break;
        } else if remaining_size - total_overhead > MAX_SINGLE_HEADER_SIZE {
            MAX_SINGLE_HEADER_SIZE
        } else {
            remaining_size - total_overhead
        };

        let key = HeaderKey::new(key_str.as_str()).unwrap();
        let value = HeaderValue::from_str(create_string_of_size(value_size).as_str()).unwrap();

        let actual_header_size = 4 + key_str.len() + 1 + 4 + value_size;
        current_size += actual_header_size;

        headers.insert(key, value);
        header_id += 1;
    }

    headers
}

fn create_message(
    header_size: Option<usize>,
    payload_size: usize,
) -> Result<IggyMessage, IggyError> {
    let headers = match header_size {
        Some(header_size) => {
            if header_size > 0 {
                Some(create_message_header_of_size(header_size))
            } else {
                None
            }
        }
        None => None,
    };

    let payload = create_string_of_size(payload_size);

    if let Some(headers) = headers {
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload))
            .user_headers(headers)
            .build()
    } else {
        IggyMessage::builder()
            .id(1u128)
            .payload(Bytes::from(payload))
            .build()
    }
}
