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
use iggy_examples::shared::stream::PrintEventConsumer;
use tokio::sync::oneshot;

const IGGY_URL: &str = "iggy://iggy:iggy@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let stream = "test_stream";
    let topic = "test_topic";

    let config = IggyConsumerConfig::builder()
        // Set the stream name. The consumer binds to the stream by this name.
        .stream_name(stream)
        // Set the topic name. The consumer binds to the topic by this name.
        .topic_name(topic)
        // Create the stream if it doesn't exist.
        .create_stream_if_not_exists(true)
        // Create the topic if it doesn't exist.
        .create_topic_if_not_exists(true)
        // Members of the same consumer group use the same name.
        .consumer_name("test_consumer".to_string())
        // The type of consumer. It can be either `Consumer` or `ConsumerGroup`. ConsumerGroup is default.
        .consumer_kind(ConsumerKind::ConsumerGroup)
        // Topic creation count. Ordinary consumers select their partitions with `partitions`.
        .partitions_count(1)
        // A poll returns up to `count` messages when `min_count` messages are ready, `max_bytes` is reached or `max_wait` expires.
        // The greater the `count`, the higher the throughput for bulk data.
        // Note, there is a tradeoff between batch size and latency, so you want to benchmark your setup.
        // `commit` decides who stores offsets:
        // * `CommitPolicy::Manual`, the default: the application commits checkpoints itself.
        // * `CommitPolicy::OnRead`: the server stores the offset of each reply as part of it.
        // * `CommitPolicy::Auto`: the consumer stores the latest counted message at an interval or on an event.
        //   `AutoCommit::default()` stores every 10,000 messages or 5 s, after the application processed them.
        .poll_options(PollOptions {
            count: 100,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
        // `PollingStrategy` specifies from where to start polling messages.
        // It has the following kinds:
        // - `Offset` - start polling from the specified offset.
        // - `Timestamp` - start at or after the broker append timestamp.
        // - `First` - start polling from the first message in the partition. This enables messages replay in order of arrival.
        // - `Last` - read up to `count` messages ending at the committed offset.
        // - `Next` - start polling from the next message after the last polled message based on the stored consumer offset.
        .polling_strategy(PollingStrategy::last())
        // Sets the polling retry interval in case of server disconnection.
        .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
        // Sets the number of retries and the interval when initializing the consumer if the stream or topic is not found.
        // Might be useful when the stream or topic is created dynamically by the producer.
        // The default configuration uses five retries at three-second intervals.
        // When you want to retry at most 5 times with an interval of 1 second,
        // you set `init_retries` to 5 and `init_interval` to 1 second.
        .init_retries(5)
        .init_interval(NonZeroIggyDuration::ONE_SECOND)
        // Optionally, configure Aes256Gcm payload and user-header decryption.
        // Set `commit` to `Manual` or `Auto` first, because the consumer refuses an encryptor with `OnRead`.
        // Key must be identical to the one used by the producer; thus ensure secure key exchange i.e. K8s secret etc.
        // Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
        // .encryptor(std::sync::Arc::new(EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[1; 32])?)))
        .build();

    let (client, mut consumer) =
        IggyStreamConsumer::with_client_from_url(IGGY_URL, &config).await?;

    println!("Start message stream");
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        match consumer
            // PrintEventConsumer is imported from examples/src/shared/stream.rs
            .consume_messages(&PrintEventConsumer {}, rx)
            .await
        {
            Ok(_) => {}
            Err(err) => eprintln!("Failed to consume messages: {err}"),
        }
    });

    // Wait a bit for all messages to arrive.
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    println!("Stop the message stream and shutdown iggy client");
    tx.send(()).expect("Failed to send shutdown signal");
    client.shutdown().await?;
    Ok(())
}
