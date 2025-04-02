use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{assert_clean_system, login_root, ClientFactory};
use std::collections::HashMap;
use std::str::FromStr;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
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
        MessageToSend::OfSizeWithHeaders(100_000, 10_000_001),
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
            &STREAM_ID.try_into().unwrap(),
            &TOPIC_ID.try_into().unwrap(),
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
    client
        .create_stream(STREAM_NAME, Some(STREAM_ID))
        .await
        .unwrap();

    // 2. Create the topic
    client
        .create_topic(
            &STREAM_ID.try_into().unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Default::default(),
            None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

async fn cleanup_system(client: &IggyClient) {
    client
        .delete_stream(&STREAM_ID.try_into().unwrap())
        .await
        .unwrap();
}

async fn send_message_and_check_result(
    client: &IggyClient,
    message_params: MessageToSend,
    expected_result: Result<(), IggyError>,
) {
    let mut messages = Vec::new();

    match message_params {
        MessageToSend::NoMessage => {
            println!("Sending message without messages inside");
        }
        MessageToSend::OfSize(size) => {
            println!("Sending message with payload size = {size}");
            messages.push(create_message(None, size));
        }
        MessageToSend::OfSizeWithHeaders(header_size, payload_size) => {
            println!("Sending message with header size = {header_size} and payload size = {payload_size}");
            messages.push(create_message(Some(header_size), payload_size));
        }
    };

    let send_result = client
        .send_messages(
            &STREAM_ID.try_into().unwrap(),
            &TOPIC_ID.try_into().unwrap(),
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

fn create_message(header_size: Option<usize>, payload_size: usize) -> IggyMessage {
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
        IggyMessage::with_id_and_headers(1u128, Bytes::from(payload), headers)
    } else {
        IggyMessage::with_id(1u128, Bytes::from(payload))
    }
}
