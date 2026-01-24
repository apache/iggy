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

use crate::server::scenarios::{PARTITIONS_COUNT, STREAM_NAME, TOPIC_NAME};
use iggy::prelude::*;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyByteSize, IggyDuration, IggyExpiry, MaxTopicSize,
};
use integration::test_server::{ClientFactory, login_root};
use std::path::Path;
use std::time::Duration;
use tokio::fs;
use tokio::time::{sleep, timeout};

const OPERATION_TIMEOUT_SECS: u64 = 10;
const OPERATION_LOOP_COUNT: usize = 500;
const FROM_BYTES_TO_KB: u64 = 1000;
const IGGY_LOG_BASE_NAME: &str = "iggy-server.log";

#[derive(Debug)]
pub struct LogRotationTestConfig {
    pub name: String,
    pub max_single_log_size: IggyByteSize,
    pub max_total_log_size: IggyByteSize,
    pub rotation_check_interval: IggyDuration,
    pub retention: IggyDuration,
}

pub async fn run(
    client_factory: &dyn ClientFactory,
    log_dir: &str,
    present_log_config: LogRotationTestConfig,
) {
    let present_log_test_title = present_log_config.name.clone();
    let log_path = Path::new(log_dir);
    assert!(
        log_path.exists() && log_path.is_dir(),
        "failed::no_such_directory => {log_dir}",
    );

    let client = init_valid_client(client_factory).await;
    assert!(
        client.is_ok(),
        "failed::client_initialize => {:?}",
        client.as_ref().err(),
    );

    let generator_result = generate_enough_logs(client.as_ref().unwrap()).await;
    assert!(
        generator_result.is_ok(),
        "failed::generate_logs => {:?}",
        generator_result.as_ref().err(),
    );

    nocapture_observer(log_path, &present_log_test_title).await;
    sleep(present_log_config.rotation_check_interval.get_duration()).await;

    let rotation_result = validate_log_rotation_rules(log_path, present_log_config).await;
    assert!(
        rotation_result.is_ok(),
        "failed::rotation_check => {:?}",
        rotation_result.as_ref().err(),
    );

    nocapture_observer(log_path, &present_log_test_title).await;
    eprintln!(
        "\n [Passed] <-> {:<25} <{:->45}>\n",
        present_log_test_title, "",
    );
}

pub fn get_configurations() -> Vec<LogRotationTestConfig> {
    vec![
        LogRotationTestConfig {
            name: "log_regular_rotation".to_string(),
            max_single_log_size: IggyByteSize::new(200_000),
            max_total_log_size: IggyByteSize::new(800_000),
            rotation_check_interval: IggyDuration::ONE_SECOND,
            retention: IggyDuration::new_from_secs(30),
        },
        LogRotationTestConfig {
            name: "log_unlimited_size".to_string(),
            max_single_log_size: IggyByteSize::new(0),
            max_total_log_size: IggyByteSize::new(800_000),
            rotation_check_interval: IggyDuration::ONE_SECOND,
            retention: IggyDuration::new_from_secs(30),
        },
        LogRotationTestConfig {
            name: "log_unlimited_archives".to_string(),
            max_single_log_size: IggyByteSize::new(200_000),
            max_total_log_size: IggyByteSize::new(0),
            rotation_check_interval: IggyDuration::ONE_SECOND,
            retention: IggyDuration::new_from_secs(30),
        },
        LogRotationTestConfig {
            name: "log_special_scenario".to_string(),
            max_single_log_size: IggyByteSize::new(0),
            max_total_log_size: IggyByteSize::new(0),
            rotation_check_interval: IggyDuration::ONE_SECOND,
            retention: IggyDuration::new_from_secs(30),
        },
    ]
}

async fn init_valid_client(client_factory: &dyn ClientFactory) -> Result<IggyClient, String> {
    let operation_timeout = IggyDuration::new(Duration::from_secs(OPERATION_TIMEOUT_SECS));
    let client_wrapper = timeout(
        operation_timeout.get_duration(),
        client_factory.create_client(),
    )
    .await
    .map_err(|_| "ClientWrapper creation timed out")?;

    timeout(operation_timeout.get_duration(), client_wrapper.connect())
        .await
        .map_err(|_| "Client connection timed out")?
        .map_err(|e| format!("Client connection failed: {e:?}"))?;

    let client = IggyClient::create(client_wrapper, None, None);
    timeout(operation_timeout.get_duration(), login_root(&client))
        .await
        .map_err(|e| format!("Root user login timed out: {e:?}"))?;

    Ok(client)
}

async fn generate_enough_logs(client: &IggyClient) -> Result<(), String> {
    for i in 0..OPERATION_LOOP_COUNT {
        let stream_name = format!("{STREAM_NAME}-{i}");
        let topic_name = format!("{TOPIC_NAME}-{i}");

        client
            .create_stream(&stream_name)
            .await
            .map_err(|e| format!("Failed to create {stream_name}: {e}"))?;

        let stream_identifier = Identifier::named(&stream_name)
            .map_err(|e| format!("Failed to create stream label {e}"))?;

        client
            .create_topic(
                &stream_identifier,
                &topic_name,
                PARTITIONS_COUNT,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::Unlimited,
            )
            .await
            .map_err(|e| format!("Failed to create topic {topic_name}: {e}"))?;

        client
            .delete_stream(&stream_identifier)
            .await
            .map_err(|e| format!("Failed to remove stream {stream_name}: {e}"))?;
    }

    Ok(())
}

async fn validate_log_rotation_rules(
    log_dir: &Path,
    present_log_config: LogRotationTestConfig,
) -> Result<(), String> {
    let mut dir_entries = fs::read_dir(log_dir).await.map_err(|e| {
        format!(
            "Failed to read log directory '{log_dir}': {e}",
            log_dir = log_dir.display()
        )
    })?;

    let mut valid_log_files = Vec::new();
    while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
        format!(
            "Failed to read next entry in log directory '{log_dir}': {e}",
            log_dir = log_dir.display()
        )
    })? {
        let file_path = entry.path();

        if !file_path.is_file() {
            continue;
        }

        let file_name = match file_path.file_name().and_then(|name| name.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if is_valid_iggy_log_file(file_name) {
            valid_log_files.push(file_path);
        }
    }

    if valid_log_files.is_empty() {
        return Err(format!(
            "No valid Iggy log files found in directory '{}'. Expected files matching '{}' (original) or '{}.<numeric>' (archived).",
            log_dir.display(),
            IGGY_LOG_BASE_NAME,
            IGGY_LOG_BASE_NAME
        ));
    }

    // logger.rs => tracing_appender::non_blocking(file_appender);
    // The delay in log writing in Iggy mainly depends on the processing speed
    // of background threads and the operating system's I/O scheduling,  which
    // means that the actual size of written logs may be slightly larger  than
    // expected. So there ignores tiny minor overflow by comparing integer  KB
    // values instead of exact bytes.

    let mut total_log_size = IggyByteSize::new(0);
    let max_single_kb = present_log_config.max_single_log_size.as_bytes_u64() / FROM_BYTES_TO_KB;
    let max_total_kb = present_log_config.max_total_log_size.as_bytes_u64() / FROM_BYTES_TO_KB;

    if max_single_kb == 0 && valid_log_files.len() > 1 {
        return Err(format!(
            "Log size should be unlimited if `max_file_size` is set to 0, found {} files.",
            valid_log_files.len()
        ));
    } else if max_total_kb == 0 && max_single_kb != 0 {
        if valid_log_files.len() as u64 <= 1 {
            return Err(format!(
                "Archives should be unlimited if `max_total_size` is set to 0, found {} files.",
                valid_log_files.len(),
            ));
        }
    } else {
        for log_file in valid_log_files {
            let file_metadata = fs::metadata(&log_file).await.map_err(|e| {
                format!(
                    "Failed to get metadata for file '{}': {}",
                    log_file.display(),
                    e
                )
            })?;

            let file_size_bytes = file_metadata.len();
            if max_single_kb != 0 {
                let current_single_kb = file_size_bytes / FROM_BYTES_TO_KB;
                if current_single_kb > max_single_kb {
                    return Err(format!(
                        "Single log file exceeds maximum allowed size: '{}'",
                        log_file.display()
                    ));
                }
            }

            total_log_size += IggyByteSize::new(file_size_bytes);
        }
    }

    let current_total_kb = total_log_size.as_bytes_u64() / FROM_BYTES_TO_KB;
    if max_total_kb != 0 && max_single_kb != 0 && current_total_kb > max_total_kb {
        return Err(format!(
            "Total log size exceeds maximum allowed size: '{}'",
            log_dir.display()
        ));
    }

    Ok(())
}

fn is_valid_iggy_log_file(file_name: &str) -> bool {
    if file_name == IGGY_LOG_BASE_NAME {
        return true;
    }

    let archive_log_prefix = format!("{}.", IGGY_LOG_BASE_NAME);
    if file_name.starts_with(&archive_log_prefix) {
        let numeric_suffix = &file_name[archive_log_prefix.len()..];
        return !numeric_suffix.is_empty() && numeric_suffix.chars().all(|c| c.is_ascii_digit());
    }
    false
}

/// Solely for manual && direct observation of file status to reduce debugging overhead.
async fn nocapture_observer(log_path: &Path, title: &str) -> () {
    eprintln!(
        "\n{:>4} Size <-> Path && server::specific::log_rotation_be_valid::{}",
        "", title,
    );
    let mut dir_entries = fs::read_dir(log_path).await.unwrap();
    while let Some(entry) = dir_entries.next_entry().await.unwrap() {
        let file_path = entry.path();
        if file_path.is_file() {
            let meta = fs::metadata(&file_path).await.unwrap();
            eprintln!(
                "{:>6} KB <-> {:<50}",
                meta.len() / FROM_BYTES_TO_KB,
                file_path.display()
            );
        }
    }
}
