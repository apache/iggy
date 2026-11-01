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

//! The reader of a sink stops a partition after six refusals in a row and reads it again only
//! after a seek, and the sink seeks such a partition to `Next`. A sink reads with a committing
//! fetch, so this holds only because the server fails a committing fetch with `Unauthorized` while
//! no read stored, which the SDK takes as a refusal that stored nothing: a read permission revoked
//! on the server counts towards the stop, and the partition reads again once the permission
//! returns, without a restart.

use iggy::prelude::{
    Identifier, IggyClient, IggyMessage, MessageClient, Partitioning, Permissions, UserClient,
};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats};
use integration::harness::{seeds, wait_for_consumer_group_assignment};
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const SINK_KEY: &str = "stdout_refusal";
const SINK_GROUP: &str = "iggy-connect-sink-stdout_refusal";
const PARTITION: u32 = 0;
const MEMBERSHIP_BUDGET: Duration = Duration::from_secs(30);
const STATS_BUDGET: Duration = Duration::from_secs(30);
/// Six refusals with a wait that doubles from one second take about 31 s, and the seek that
/// follows the sixth starts a new cycle, whose first refusal is the sixth warning.
const REFUSALS_BUDGET: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// The reader warns at refusals one to five of a cycle and stops silently at the sixth.
const REFUSED: &str = "Partition: 0 was refused";
const WARNINGS_PAST_THE_STOP: usize = 6;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/sink_refusal.toml")),
    seed = seeds::connector_stream_with_read_user
)]
async fn given_a_sink_partition_stopped_by_a_revoked_permission_when_the_permission_returns_should_consume_again(
    harness: &TestHarness,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    let stream_id = Identifier::named(seeds::names::STREAM).expect("valid stream name");
    let topic_id = Identifier::named(seeds::names::TOPIC).expect("valid topic name");
    let user = Identifier::named(seeds::names::USER).expect("valid user name");
    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime should be available");
    let stats_url = format!("{}/stats", runtime.http_url());
    wait_for_consumer_group_assignment(
        &client,
        &stream_id,
        &topic_id,
        &Identifier::named(SINK_GROUP).expect("valid group name"),
        1,
        MEMBERSHIP_BUDGET,
    )
    .await;
    send(&client, &stream_id, &topic_id, 1).await;
    wait_for_processed(&stats_url, 1).await;

    client
        .update_permissions(&user, Some(Permissions::default()))
        .await
        .expect("the read permission should be revoked");
    send(&client, &stream_id, &topic_id, 2).await;
    let warnings = timeout(REFUSALS_BUDGET, async {
        loop {
            let (stdout, _) = runtime.collect_logs();
            let warnings = stdout.matches(REFUSED).count();
            if warnings >= WARNINGS_PAST_THE_STOP {
                break warnings;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect(
        "the sink never read the stopped partition again: a sixth refusal warning exists only after a seek while the permission is missing",
    );
    client
        .update_permissions(&user, Some(seeds::read_streams_permissions()))
        .await
        .expect("the read permission should return");
    send(&client, &stream_id, &topic_id, 3).await;

    let sink = wait_for_processed(&stats_url, 3).await;
    assert!(warnings >= WARNINGS_PAST_THE_STOP);
    assert_eq!(
        sink.messages_processed,
        Some(3),
        "the message sent while refused and the one sent after both arrive once"
    );
}

async fn send(client: &IggyClient, stream_id: &Identifier, topic_id: &Identifier, number: u32) {
    let mut messages = vec![IggyMessage::from(format!(r#"{{"message":{number}}}"#))];
    client
        .send_messages(
            stream_id,
            topic_id,
            &Partitioning::partition_id(PARTITION),
            &mut messages,
        )
        .await
        .expect("message should reach Iggy");
}

/// Polls the runtime stats until the sink has processed `count` messages, and returns its entry.
async fn wait_for_processed(stats_url: &str, count: u64) -> ConnectorStats {
    let http_client = Client::new();
    timeout(STATS_BUDGET, async {
        loop {
            let snapshot: ConnectorRuntimeStats = http_client
                .get(stats_url)
                .send()
                .await
                .expect("stats request should complete")
                .error_for_status()
                .expect("stats endpoint should succeed")
                .json()
                .await
                .expect("stats response should decode");
            let sink = snapshot
                .connectors
                .into_iter()
                .find(|connector| connector.key == SINK_KEY)
                .expect("sink should be reported");
            if sink.messages_processed == Some(count) {
                break sink;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the sink should process {count} messages"))
}
