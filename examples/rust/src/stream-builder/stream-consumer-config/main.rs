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

use iggy::prelude::*;
use iggy_examples::shared::stream::PrintEventConsumer;
use std::str::FromStr;
use tokio::sync::oneshot;

const IGGY_URL: &str = "iggy://iggy:iggy@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let stream = "test_stream";
    let topic = "test_topic";

    let config = IggyConsumerConfig::builder()
        // Set the stream identifier and name.
        .stream_id(Identifier::from_str_value(stream)?)
        .stream_name(stream)
        // Set the topic identifier and name
        .topic_id(Identifier::from_str_value(topic)?)
        .topic_name(topic)
        // The auto-commit configuration for storing the message offset on the server.
        // * Disabled:  The auto-commit is disabled and the offset must be stored manually by the consumer.
        // * Interval: The auto-commit is enabled and the offset is stored on the server after a certain interval.
        // * IntervalOrWhen: The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode when consuming the messages.
        // * IntervalOrAfter: [This requires the `IggyConsumerMessageExt` trait when using `consume_messages()`.]
        // The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode after consuming the messages.
        // * When: The auto-commit is enabled and the offset is stored on the server depending on the mode when consuming the messages.
        // * After: [This requires the `IggyConsumerMessageExt` trait when using `consume_messages()`.]
        // The auto-commit is enabled and the offset is stored on the server depending on the mode after consuming the messages.
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        // The max number of messages to send in a batch. The greater the batch size, the higher the throughput for bulk data.
        // Note, there is a tradeoff between batch size and latency, so you want to benchmark your setup.
        // Note, this only applies to batch send messages. Single messages are sent immediately.
        .batch_length(100)
        // Create the stream if it doesn't exist.
        .create_stream_if_not_exists(true)
        // Create the topic if it doesn't exist.
        .create_topic_if_not_exists(true)
        // The name of the consumer. Must be unique.
        .consumer_name("test_consumer".to_string())
        // The type of consumer. It can be either `Consumer` or `ConsumerGroup`. ConsumerGroup is default.
        .consumer_kind(ConsumerKind::ConsumerGroup)
        // Sets the number of partitions for ConsumerKind `Consumer`. Does not apply to `ConsumerGroup`.
        .partitions_count(1)
        // The polling interval for messages.
        .polling_interval(IggyDuration::from_str("5ms").unwrap())
        // `PollingStrategy` specifies from where to start polling messages.
        // It has the following kinds:
        // - `Offset` - start polling from the specified offset.
        // - `Timestamp` - start polling from the specified timestamp.
        // - `First` - start polling from the first message in the partition. This enables messages replay in order of arrival.
        // - `Last` - start polling from the last message in the partition. This disables messages replay since only the latest message is pulled.
        // - `Next` - start polling from the next message after the last polled message based on the stored consumer offset.
        .polling_strategy(PollingStrategy::last())
        // Sets the polling retry interval in case of server disconnection.
        .polling_retry_interval(IggyDuration::new_from_secs(1))
        // Sets the number of retries and the interval when initializing the consumer if the stream or topic is not found.
        // Might be useful when the stream or topic is created dynamically by the producer.
        // The retry only occurs when configured and is disabled by default.
        // When you want to retry at most 5 times with an interval of 1 second,
        // you set `init_retries` to 5 and `init_interval` to 1 second.
        .init_retries(5)
        .init_interval(IggyDuration::new_from_secs(1))
        // Optionally, set a custom client side encryptor for encrypting the messages' payloads. Currently only Aes256Gcm is supported.
        // Key must be identical to the one used by the producer; thus ensure secure key exchange i.e. K8s secret etc.
        // Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
        // .encryptor(Arc::new(EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[1; 32])?)))
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
