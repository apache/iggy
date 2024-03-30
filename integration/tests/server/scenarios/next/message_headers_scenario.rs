use crate::server::scenarios::next::{
    cleanup, create_client, MESSAGES_COUNT, PARTITIONS_COUNT, PARTITION_ID, STREAM_ID, STREAM_NAME,
    TOPIC_ID, TOPIC_NAME,
};
use bytes::Bytes;
use iggy::clients::next_client::IggyClientNext;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::next_client::{MessageClientNext, StreamClientNext, TopicClientNext};
use iggy::utils::expiry::IggyExpiry;
use integration::test_server::{assert_clean_system_next, login_root_next, ClientFactoryNext};
use std::collections::HashMap;
use std::str::FromStr;

pub async fn run(client_factory: &dyn ClientFactoryNext) {
    let client = create_client(client_factory).await;
    login_root_next(&client).await;
    init_system(&client).await;

    // 1. Send messages with the included headers
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        let id = (offset + 1) as u128;
        let payload = create_message_payload(offset as u64);
        let headers = create_message_headers();
        messages.push(Message {
            id,
            length: payload.len() as u32,
            payload,
            headers: Some(headers),
        });
    }

    client
        .send_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // 2. Poll messages and validate the headers
    let consumer = Consumer::default();
    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len() as u32, MESSAGES_COUNT);
    for i in 0..MESSAGES_COUNT {
        let message = polled_messages.messages.get(i as usize).unwrap();
        assert!(message.headers.is_some());
        let headers = message.headers.as_ref().unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(
            headers
                .get(&HeaderKey::new("key_1").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "Value 1"
        );
        assert!(headers
            .get(&HeaderKey::new("key 2").unwrap())
            .unwrap()
            .as_bool()
            .unwrap(),);
        assert_eq!(
            headers
                .get(&HeaderKey::new("key-3").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            123456
        );
    }
    cleanup(&client, false).await;
    assert_clean_system_next(&client).await;
}

async fn init_system(client: &IggyClientNext) {
    // 1. Create the stream
    client
        .create_stream(STREAM_NAME, Some(STREAM_ID))
        .await
        .unwrap();

    // 2. Create the topic
    client
        .create_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            Some(1),
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            None,
        )
        .await
        .unwrap();
}

fn create_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {}", offset))
}

fn create_message_headers() -> HashMap<HeaderKey, HeaderValue> {
    let mut headers = HashMap::new();
    headers.insert(
        HeaderKey::new("key_1").unwrap(),
        HeaderValue::from_str("Value 1").unwrap(),
    );
    headers.insert(
        HeaderKey::new("key 2").unwrap(),
        HeaderValue::from_bool(true).unwrap(),
    );
    headers.insert(
        HeaderKey::new("key-3").unwrap(),
        HeaderValue::from_uint64(123456).unwrap(),
    );
    headers
}
