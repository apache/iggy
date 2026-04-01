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
use testcontainers_modules::{
    localstack::LocalStack,
    postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};

mod redshift_sink;

/// Holds the test containers to keep them alive during tests.
struct RedshiftTestContainers {
    _postgres: ContainerAsync<postgres::Postgres>,
    _localstack: ContainerAsync<LocalStack>,
}

/// Setup result containing both runtime and containers.
#[allow(dead_code)]
struct RedshiftTestSetup {
    runtime: ConnectorsRuntime,
    _containers: RedshiftTestContainers,
}

async fn setup() -> RedshiftTestSetup {
    // Start PostgreSQL container (simulating Redshift as they share the same wire protocol)
    let postgres_container = postgres::Postgres::default()
        .start()
        .await
        .expect("Failed to start Postgres (Redshift simulator)");
    let postgres_port = postgres_container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get Postgres port");

    // Start LocalStack for S3
    let localstack_container = LocalStack::default()
        .start()
        .await
        .expect("Failed to start LocalStack");
    let localstack_port = localstack_container
        .get_host_port_ipv4(4566)
        .await
        .expect("Failed to get LocalStack port");

    // Create S3 bucket using LocalStack S3 API
    let s3_endpoint = format!("http://localhost:{localstack_port}");
    let bucket_name = "iggy-redshift-staging";

    // Create the bucket via LocalStack S3 API using path-style URL
    let client = reqwest::Client::new();
    let create_bucket_url = format!("{s3_endpoint}/{bucket_name}");
    client
        .put(&create_bucket_url)
        .send()
        .await
        .expect("Failed to create S3 bucket in LocalStack");

    let mut envs = HashMap::new();
    let iggy_setup = IggySetup::default();

    // Redshift connection (using PostgreSQL as simulator)
    let connection_string = format!("postgres://postgres:postgres@localhost:{postgres_port}");
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_CONNECTION_STRING".to_owned(),
        connection_string,
    );

    // S3 configuration for staging
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_BUCKET".to_owned(),
        bucket_name.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_REGION".to_owned(),
        "us-east-1".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_S3_ENDPOINT".to_owned(),
        s3_endpoint,
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_AWS_ACCESS_KEY_ID".to_owned(),
        "test".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_PLUGIN_CONFIG_AWS_SECRET_ACCESS_KEY".to_owned(),
        "test".to_owned(),
    );

    // Stream configuration
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_STREAM".to_owned(),
        iggy_setup.stream.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_TOPICS".to_owned(),
        format!("[{}]", iggy_setup.topic),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_SCHEMA".to_owned(),
        "json".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SINK_REDSHIFT_STREAMS_0_CONSUMER_GROUP".to_owned(),
        "test".to_owned(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init("redshift/config.toml", Some(envs), iggy_setup)
        .await;

    RedshiftTestSetup {
        runtime,
        _containers: RedshiftTestContainers {
            _postgres: postgres_container,
            _localstack: localstack_container,
        },
    }
}
