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

//! This scenario fixes the bug that occured when concurrent message sends race with segment rotation:
//! 1. Task A commits journal, ensures indexes for segment N, starts async save
//! 2. Task B's send triggers segment rotation (handle_full_segment)
//! 3. Task B clears segment N's indexes or creates segment N+1 with None indexes
//! 4. Task A calls active_indexes().unwrap() - panics because indexes are None
//!
//! This test uses:
//! - Very small segment size (512B) to trigger frequent rotations
//! - 8 concurrent producers (2 per protocol: TCP, HTTP, QUIC, WebSocket)
//! - All producers write to the same partition for maximum lock contention
//! - Short message_saver interval to add more concurrent persist operations

use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinSet;

const STREAM_NAME: &str = "race-test-stream";
const TOPIC_NAME: &str = "race-test-topic";
const PRODUCERS_PER_PROTOCOL: usize = 2;
const PARTITION_ID: u32 = 0;
const TEST_DURATION_SECS: u64 = 10;
const MESSAGES_PER_BATCH: usize = 5;

/// Runs the segment rotation race condition test with multiple protocols.
/// Each client factory represents a different protocol (TCP, HTTP, QUIC, WebSocket).
/// 2 producers are spawned per protocol, all writing to the same partition.
pub async fn run(client_factories: &[&dyn ClientFactory]) {
    assert!(
        !client_factories.is_empty(),
        "At least one client factory required"
    );

    let admin_client = create_client(client_factories[0]).await;
    login_root(&admin_client).await;

    let total_producers = client_factories.len() * PRODUCERS_PER_PROTOCOL;
    init_system(&admin_client, total_producers).await;

    let stop_flag = Arc::new(AtomicBool::new(false));
    let total_messages = Arc::new(AtomicU64::new(0));
    let mut join_set = JoinSet::new();

    let mut global_producer_id = 0usize;
    for factory in client_factories {
        let protocol = factory.transport();
        for local_id in 0..PRODUCERS_PER_PROTOCOL {
            let client = create_client(*factory).await;
            login_root(&client).await;

            let stop = stop_flag.clone();
            let counter = total_messages.clone();
            let producer_name = format!("{:?}-{}", protocol, local_id);
            let producer_id = global_producer_id;

            join_set.spawn(async move {
                run_producer(
                    client,
                    producer_id,
                    &producer_name,
                    PARTITION_ID,
                    stop,
                    counter,
                )
                .await;
            });

            global_producer_id += 1;
        }
    }

    tokio::time::sleep(Duration::from_secs(TEST_DURATION_SECS)).await;
    stop_flag.store(true, Ordering::SeqCst);

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result
            && e.is_panic()
        {
            let panic_info = e.into_panic();
            let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            panic!("Producer task panicked: {}", panic_msg);
        }
    }

    let sent = total_messages.load(Ordering::SeqCst);
    println!("Test completed successfully. Total messages sent: {}", sent);

    cleanup(&admin_client).await;
}

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::create(client, None, None)
}

async fn init_system(client: &IggyClient, total_producers: usize) {
    client.create_stream(STREAM_NAME).await.unwrap();

    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    println!(
        "Created stream and topic with 1 partition, {} producers will contend for it",
        total_producers
    );
}

async fn run_producer(
    client: IggyClient,
    producer_id: usize,
    producer_name: &str,
    partition_id: u32,
    stop: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
) {
    let mut batch_num = 0u64;

    while !stop.load(Ordering::SeqCst) {
        let mut messages = Vec::with_capacity(MESSAGES_PER_BATCH);

        for i in 0..MESSAGES_PER_BATCH {
            let payload = format!("p{}:b{}:m{}", producer_id, batch_num, i);
            let message = IggyMessage::builder()
                .payload(payload.into_bytes().into())
                .build()
                .unwrap();
            messages.push(message);
        }

        match client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
        {
            Ok(_) => {
                counter.fetch_add(MESSAGES_PER_BATCH as u64, Ordering::SeqCst);
                batch_num += 1;
            }
            Err(e) => {
                panic!("Producer {} send error: {}", producer_name, e);
            }
        }
    }

    println!(
        "Producer {} (partition {}) stopped after {} batches",
        producer_name, partition_id, batch_num
    );
}

async fn cleanup(client: &IggyClient) {
    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
