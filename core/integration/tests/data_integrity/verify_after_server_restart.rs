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

use iggy::clients::client::IggyClient;
use iggy::prelude::{Identifier, IggyByteSize, MessageClient, SystemClient};
use iggy_common::TransportProtocol;
use integration::bench_utils::run_bench_and_wait_for_finish;
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{ClientFactory, IpAddrKind, SYSTEM_PATH_ENV_VAR, TestServer, login_root},
};
use serial_test::parallel;
use std::{collections::HashMap, str::FromStr};
use test_case::test_matrix;

fn cache_open_segment() -> &'static str {
    "open_segment"
}

fn cache_all() -> &'static str {
    "all"
}

fn cache_none() -> &'static str {
    "none"
}

// TODO(numminex) - Move the message generation method from benchmark run to a special method.
#[test_matrix(
    [cache_all(), cache_open_segment(), cache_none()]
)]
#[tokio::test]
#[parallel]
async fn should_fill_data_and_verify_after_restart(cache_setting: &'static str) {
    // 1. Start server with cache configuration
    let env_vars = HashMap::from([
        (
            SYSTEM_PATH_ENV_VAR.to_owned(),
            TestServer::get_random_path(),
        ),
        (
            "IGGY_SEGMENT_CACHE_INDEXES".to_string(),
            cache_setting.to_string(),
        ),
    ]);

    let mut test_server = TestServer::new(Some(env_vars.clone()), false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let local_data_path = test_server.get_local_data_path().to_owned();

    // 2. Run send bench to fill 5 MB of data
    let amount_of_data_to_process = IggyByteSize::from_str("5 MB").unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-producer",
        amount_of_data_to_process,
    );

    // 3. Run poll bench to check if everything's OK
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        amount_of_data_to_process,
    );

    // 4. Connect and login to newly started server
    let client = TcpClientFactory {
        server_addr,
        ..Default::default()
    }
    .create_client()
    .await;

    let client = IggyClient::create(client, None, None);
    login_root(&client).await;

    let topic_id = Identifier::numeric(0).unwrap();
    for i in 0..7 {
        let stream_id = Identifier::numeric(i).unwrap();
        client
            .flush_unsaved_buffer(&stream_id, &topic_id, 0, true)
            .await
            .unwrap();
    }

    // 5. Save stats from the first server
    let stats = client.get_stats().await.unwrap();
    let expected_messages_size_bytes = stats.messages_size_bytes;
    let expected_streams_count = stats.streams_count;
    let expected_topics_count = stats.topics_count;
    let expected_partitions_count = stats.partitions_count;
    let expected_segments_count = stats.segments_count;
    let expected_messages_count = stats.messages_count;
    let expected_clients_count = stats.clients_count;
    let expected_consumer_groups_count = stats.consumer_groups_count;

    // 6. Stop server
    test_server.stop();
    drop(test_server);

    // 7. Restart server with same settings
    let mut test_server = TestServer::new(Some(env_vars.clone()), false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    // 8. Verify stats are preserved after restart (before adding more data)
    let client_after_restart = IggyClient::create(
        TcpClientFactory {
            server_addr: server_addr.clone(),
            ..Default::default()
        }
        .create_client()
        .await,
        None,
        None,
    );
    login_root(&client_after_restart).await;

    let stats_after_restart = client_after_restart.get_stats().await.unwrap();
    assert_eq!(
        expected_messages_count, stats_after_restart.messages_count,
        "Messages count should be preserved after restart (before: {}, after: {})",
        expected_messages_count, stats_after_restart.messages_count
    );
    assert_eq!(
        expected_messages_size_bytes.as_bytes_usize(),
        stats_after_restart.messages_size_bytes.as_bytes_usize(),
        "Messages size bytes should be preserved after restart (before: {}, after: {})",
        expected_messages_size_bytes.as_bytes_usize(),
        stats_after_restart.messages_size_bytes.as_bytes_usize()
    );
    assert_eq!(
        expected_streams_count, stats_after_restart.streams_count,
        "Streams count should be preserved after restart"
    );
    assert_eq!(
        expected_topics_count, stats_after_restart.topics_count,
        "Topics count should be preserved after restart"
    );
    assert_eq!(
        expected_partitions_count, stats_after_restart.partitions_count,
        "Partitions count should be preserved after restart"
    );
    assert_eq!(
        expected_segments_count, stats_after_restart.segments_count,
        "Segments count should be preserved after restart"
    );

    // 9. Run send bench again to add more data
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-producer",
        amount_of_data_to_process,
    );

    // 10. Run poll bench again to check if all data is still there
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        IggyByteSize::from(amount_of_data_to_process.as_bytes_u64() * 2),
    );
    drop(client_after_restart);

    // 11. Connect and login to newly started server
    let client = IggyClient::create(
        TcpClientFactory {
            server_addr: server_addr.clone(),
            ..Default::default()
        }
        .create_client()
        .await,
        None,
        None,
    );
    login_root(&client).await;

    // 12. Flush unsaved buffer
    let topic_id = Identifier::numeric(0).unwrap();
    for i in 0..7 {
        let stream_id = Identifier::numeric(i).unwrap();
        client
            .flush_unsaved_buffer(&stream_id, &topic_id, 0, true)
            .await
            .unwrap();
    }

    // 13. Save stats from the second server (should have double the data)
    let stats = client.get_stats().await.unwrap();
    let actual_messages_size_bytes = stats.messages_size_bytes;
    let actual_streams_count = stats.streams_count;
    let actual_topics_count = stats.topics_count;
    let actual_partitions_count = stats.partitions_count;
    let actual_segments_count = stats.segments_count;
    let actual_messages_count = stats.messages_count;
    let actual_clients_count = stats.clients_count;
    let actual_consumer_groups_count = stats.consumer_groups_count;

    // 14. Compare stats (expecting double the messages/size after second bench run)
    assert_eq!(
        expected_messages_size_bytes.as_bytes_usize() * 2,
        actual_messages_size_bytes.as_bytes_usize(),
        "Messages size bytes should be doubled"
    );
    assert_eq!(
        expected_streams_count, actual_streams_count,
        "Streams count"
    );
    assert_eq!(expected_topics_count, actual_topics_count, "Topics count");
    assert_eq!(
        expected_partitions_count, actual_partitions_count,
        "Partitions count"
    );
    assert!(
        actual_segments_count >= expected_segments_count,
        "Segments count should be at least the same or more"
    );
    assert_eq!(
        expected_messages_count * 2,
        actual_messages_count,
        "Messages count should be doubled"
    );
    assert_eq!(
        expected_clients_count, actual_clients_count,
        "Clients count"
    );
    assert_eq!(
        expected_consumer_groups_count, actual_consumer_groups_count,
        "Consumer groups count"
    );

    // 15. Run poll bench to check if all data (10MB total) is still there
    run_bench_and_wait_for_finish(
        &server_addr,
        &TransportProtocol::Tcp,
        "pinned-consumer",
        IggyByteSize::from(amount_of_data_to_process.as_bytes_u64() * 2),
    );

    // 16. Manual cleanup
    std::fs::remove_dir_all(local_data_path).unwrap();
}
