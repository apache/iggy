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

use crate::connectors::setup;
use iggy_binary_protocol::{StreamClient, TopicClient};
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use std::collections::HashMap;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

#[tokio::test]
async fn given_valid_configuration_postgres_sink_should_start() {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("Failed to start Postgres");
    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get Postgres port");

    let mut envs = HashMap::new();
    envs.insert(
        "IGGY_CONNECTORS_SINKS_POSTGRES_CONFIG_CONNECTION_STRING".to_owned(),
        format!("postgres://postgres:postgres@localhost:{host_port}"),
    );
    let mut infra = setup();
    let client = infra.create_client().await;
    let stream_name = "test";
    let topic_name = "test";
    client
        .create_stream("test", None)
        .await
        .expect("Failed to create stream");
    let stream_id: Identifier = stream_name.try_into().expect("Invalid stream name");
    client
        .create_topic(
            &stream_id,
            topic_name,
            1,
            CompressionAlgorithm::None,
            None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Failed to create topic");
    infra
        .start_connectors_runtime(Some("sinks/postgres/postgres.toml"), Some(envs))
        .await;
}
