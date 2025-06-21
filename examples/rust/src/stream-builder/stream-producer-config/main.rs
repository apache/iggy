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
use std::str::FromStr;

const IGGY_URL: &str = "iggy://iggy:iggy@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let stream = "test_stream";
    let topic = "test_topic";

    // The builder simplifies the IggyProducer configuration.
    let config = IggyProducerConfig::builder()
        // Set the stream identifier and name.
        .stream_id(Identifier::from_str_value(stream)?)
        .stream_name(stream)
        // Set the topic identifier and name
        .topic_id(Identifier::from_str_value(topic)?)
        .topic_name(topic)
        // Sets the number of partitions to create for the topic.
        // The more clients are reading concurrently, the more partitions you should create.
        // i.e. if you have 10 clients, you should create 10 partitions
        .topic_partitions_count(10)
        // Optionally, you can set the replication factor for topic redundancy.
        // There is a tradeoff between replication factor and performance, so you want to benchmark your setup.
        .topic_replication_factor(2)
        // The max number of messages to send in a batch. The greater the batch size, the higher the throughput for bulk data.
        // Note, there is a tradeoff between batch size and latency, so you want to benchmark your setup.
        // Note, this only applies to batch send messages. Single messages are sent immediately.
        .batch_length(100)
        // Sets the interval between sending the messages. Affects latency so you want to benchmark this value.
        .linger_time(IggyDuration::from_str("5ms").unwrap())
        // `Partitioning` is used to specify to which partition the messages should be sent.
        // It has the following kinds:
        // - `Balanced` - the partition ID is calculated by the server using the round-robin algorithm.
        // - `PartitionId` - the partition ID is provided by the client.
        // - `MessagesKey` - the partition ID is calculated by the server using the hash of the provided messages key.
        .partitioning(Partitioning::balanced())
        // Sets the retry policy (maximum number of retries and interval between them) in case of messages sending failure.
        // The error can be related either to disconnecting from the server or to the server rejecting the messages.
        // Default is 3 retries with 1 second interval between them. Customize to your requirements.
        .send_retries_count(3)
        .send_retries_interval(IggyDuration::new_from_secs(1))
        // Optionally, set a custom client side encryptor for encrypting the messages' payloads. Currently only Aes256Gcm is supported.
        // Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
        // .encryptor( Arc::new(EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[1; 32])?)))
        .build();

    let (client, producer) = IggyStreamProducer::with_client_from_url(IGGY_URL, &config).await?;

    println!("Send 3 test messages...");
    producer
        .send_one(IggyMessage::from_str("Hello World")?)
        .await?;

    // Wait a bit for all messages to arrive.
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    println!("Stop the message stream and shutdown iggy client");
    client.shutdown().await?;

    Ok(())
}
