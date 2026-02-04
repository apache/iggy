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

use crate::server::scenarios::message_cleanup_scenario;
use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use test_case::test_matrix;

type CleanupScenarioFn =
    for<'a> fn(&'a IggyClient, &'a Path) -> Pin<Box<dyn Future<Output = ()> + 'a>>;

fn expiry_after_rotation() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_expiry_after_rotation(
            client, path,
        ))
    }
}

fn active_segment_protection() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_active_segment_protection(
            client, path,
        ))
    }
}

fn size_based_retention() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_size_based_retention(
            client, path,
        ))
    }
}

fn combined_retention() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_combined_retention(
            client, path,
        ))
    }
}

fn expiry_multipartition() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_expiry_with_multiple_partitions(client, path))
    }
}

fn fair_size_cleanup_multipartition() -> CleanupScenarioFn {
    |client, path| {
        Box::pin(message_cleanup_scenario::run_fair_size_based_cleanup_multipartition(client, path))
    }
}

async fn run_cleanup_scenario(scenario: CleanupScenarioFn) {
    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(HashMap::from([
                    ("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "100KiB".to_string()),
                    (
                        "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_string(),
                        "100ms".to_string(),
                    ),
                    (
                        "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_string(),
                        "1".to_string(),
                    ),
                    (
                        "IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC".to_string(),
                        "true".to_string(),
                    ),
                ]))
                .build(),
        )
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();
    let data_path = harness.server().data_path();

    scenario(&client, &data_path).await;
}

#[test_matrix([
    expiry_after_rotation(),
    active_segment_protection(),
    size_based_retention(),
    combined_retention(),
    expiry_multipartition(),
    fair_size_cleanup_multipartition(),
])]
#[tokio::test]
#[parallel]
async fn message_cleanup(scenario: CleanupScenarioFn) {
    run_cleanup_scenario(scenario).await;
}
