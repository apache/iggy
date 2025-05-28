// temporary file, do not forget to delete!

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use iggy::clients::send_mode::{BackgroundConfig, BackpressureMode, SendMode};
use iggy::prelude::defaults::*;
use iggy::prelude::*;
use iggy::{clients::client::IggyClient, prelude::TcpClient};
use iggy_common::TcpClientConfig;
use integration::test_server::{IpAddrKind, TestServer};

#[tokio::test]
async fn test_new_publisher() {
    let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
    server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = Box::new(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    client.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await.unwrap();
    client.create_stream("sample-stream", Some(1)).await.unwrap();
    client.create_topic(
        &1.try_into().unwrap(),
        "sample-topic",
        1,
        CompressionAlgorithm::default(),
        None,
        None,
        IggyExpiry::NeverExpire,
        MaxTopicSize::ServerDefault,
    ).await.unwrap();

    let producer = client
        .producer("1", "1")
        .unwrap()
        .batch_length(10)
        .send_mode(SendMode::Background)
        .build();

    producer.init().await.unwrap();

    let mut t = Vec::new();
    let batches_to_send = 10_000;
    let messages_per_batch = 10;
    let total_expected = batches_to_send * messages_per_batch;

    for _ in 0..batches_to_send {
        let start = Instant::now();

        let messages: Vec<_> = (0..messages_per_batch)
            .map(|_| {
                IggyMessage::builder()
                    .payload(Bytes::from(vec![0u8; 1024]))
                    .build()
                    .unwrap()
            })
            .collect();

        producer.send(messages).await.unwrap();

        t.push(start.elapsed().as_millis());
    }

    let mut consumer = client
        .consumer_group("some-consumer", "1", "1")
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from_str("1ms").unwrap())
        .batch_length(10)
        .build();

    consumer.init().await.unwrap();

    let mut received = 0;
    while let Some(msg) = consumer.next().await {
        match msg {
            Ok(_) => {
                received += 1;
                if received >= total_expected {
                    break;
                }
            }
            Err(e) => panic!("Consumer error: {}", e),
        }
    }

    assert_eq!(
        received, total_expected,
        "Not all messages received: got {}, expected {}",
        received, total_expected
    );

    let total: u128 = t.iter().sum();
    let avg = total as f64 / t.len() as f64;
    println!("Среднее время отправки одного батча: {:.3} мс", avg);
}


// sync: avg send: 1.561ms; overall: 46.71s
// async: avg send: 0.356ms; overall: 28.67s
