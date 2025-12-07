/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::connectors::{ConnectorsRuntime, IggySetup, setup_runtime};
use std::collections::HashMap;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

mod postgres_sink;

async fn setup() -> ConnectorsRuntime {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("Failed to start Postgres");
    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get Postgres port");

    let mut envs = HashMap::new();
    let iggy_setup = IggySetup::default();
    let connection_string = format!("postgres://postgres:postgres@localhost:{host_port}");
    envs.insert(
        "IGGY_CONNECTORS_SINK_POSTGRES_CONFIG_CONNECTION_STRING".to_owned(),
        connection_string,
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_STREAM".to_owned(),
        iggy_setup.stream.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_TOPICS_0".to_owned(),
        iggy_setup.topic.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_SCHEMA".to_owned(),
        "json".to_owned(),
    );
    let mut runtime = setup_runtime();
    runtime
        .init("postgres/config.toml", Some(envs), iggy_setup)
        .await;
    runtime
}
