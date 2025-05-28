// use std::sync::Arc;

// use bytes::Bytes;
// use criterion::{criterion_group, criterion_main, Criterion};
// use iggy::clients::send_mode::{BackgroundConfig, BackpressureMode, SendMode};
// use iggy::prelude::*;
// use iggy::{clients::client::IggyClient, prelude::TcpClient};
// use iggy_common::TcpClientConfig;
// use integration::test_server::{IpAddrKind, TestServer};
// use tokio::runtime::Runtime;

// fn bench_send(c: &mut Criterion) {
//     let rt = Runtime::new().unwrap();

//     // SETUP вне замера
//     let (mut producer, batch): (IggyProducer, Vec<IggyMessage>) = rt.block_on(async {
//         // старт сервера
//         let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
//         server.start();

//         let tcp_client_config = TcpClientConfig {
//             server_address: server.get_raw_tcp_addr().unwrap(),
//             ..TcpClientConfig::default()
//         };

//         let client = Box::new(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
//         let client = IggyClient::create(client, None, None);

//         client.connect().await.unwrap();
//         client
//             .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
//             .await
//             .unwrap();
//         client
//             .create_stream("sample-stream", Some(1))
//             .await
//             .unwrap();
//         client
//             .create_topic(
//                 &1.try_into().unwrap(),
//                 "sample-topic",
//                 1,
//                 CompressionAlgorithm::default(),
//                 None,
//                 None,
//                 IggyExpiry::NeverExpire,
//                 MaxTopicSize::ServerDefault,
//             )
//             .await
//             .unwrap();

//         let mut producer = client
//             .producer("1", "1")
//             .unwrap()
//             .batch_length(10)
//             .send_mode(SendMode::Background(BackgroundConfig {
//                 max_in_flight: 10,
//                 in_flight_timeout: None,
//                 batch_size: None,
//                 failure_mode: BackpressureMode::Block,
//             }))
//             .build();

//         producer.init().await.unwrap();

//         // заранее создаём батч
//         let mut messages = Vec::new();
//         for _ in 0..10 {
//             let payload = Bytes::from(vec![0u8; 1024]);
//             let msg = IggyMessage::builder()
//                 .payload(payload)
//                 .build()
//                 .unwrap();
//             messages.push(msg);
//         }

//         (producer, messages)
//     });

//     c.bench_function("producer.send(batch)", |b| {
//         b.to_async(&rt).iter(|| {
//             let batch = create_batch();
//             producer.send(batch)
//         });
//     });
// }

// fn create_batch() -> Vec<IggyMessage> {
//     let mut messages = Vec::new();
//     for _ in 0..1 {
//         let payload = Bytes::from(vec![0u8; 1024]);
//         let msg = IggyMessage::builder()
//             .payload(payload)
//             .build()
//             .unwrap();
//         messages.push(msg);
//     }
//     messages
// }

// criterion_group!(benches, bench_send);
// criterion_main!(benches);
