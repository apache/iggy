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

use iggy::prelude::*;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyByteSize, IggyDuration, IggyExpiry, MaxTopicSize,
};
use integration::test_server::{ClientFactory, login_root};
use std::path::Path;
use std::time::Duration;
use tokio::fs;
use tokio::time::{sleep, timeout};

pub const MAX_SINGLE_LOG_SIZE: IggyByteSize = IggyByteSize::new(2 * 1000 * 1000);
pub const MAX_TOTAL_LOG_SIZE: IggyByteSize = IggyByteSize::new(10 * 1000 * 1000);
pub const LOG_ROTATION_CHECK_INTERVAL: IggyDuration = IggyDuration::ONE_SECOND;
const OPERATION_TIMEOUT_SECS: u64 = 10;
const OPERATION_LOOP_COUNT: usize = 3000;
const OPERATION_BATCH_SIZE: usize = 500;
const IGGY_LOG_BASE_NAME: &str = "iggy-server.log";

pub async fn run(client_factory: &dyn ClientFactory, log_dir: &str) {
    let log_path = Path::new(log_dir);
    assert!(
        log_path.exists() && log_path.is_dir(),
        "Log directory does not exist or is not a valid directory: {log_dir}"
    );
    println!("Log directory verified: {log_dir}");

    let client = match init_valid_client(client_factory).await {
        Ok(c) => c,
        Err(e) => {
            panic!("Client initialization failed, cannot continue generating logs: {e}");
        }
    };

    if let Err(e) = generate_enough_logs(&client).await {
        eprintln!("Partial errors occurred during log generation: {e}");
    } else {
        sleep(LOG_ROTATION_CHECK_INTERVAL.get_duration()).await;
        let rotation_result = validate_log_rotation_rules(log_path).await;
        match rotation_result {
            Ok(()) => println!("Succeeded Verified Log Rotation"),
            Err(e) => {
                eprintln!("Failed: {e}");
            }
        }
    }
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
    let stream_name = "log_rotation_stream";
    let topic_name = "log_rotation_topic";

    client
        .create_stream(stream_name)
        .await
        .map_err(|e| format!("Failed to create {stream_name}: {e}"))?;

    let stream_identifier = Identifier::named(stream_name)
        .map_err(|e| format!("Failed to create stream identifier: {e}"))?;

    client
        .create_topic(
            &stream_identifier,
            topic_name,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
        .await
        .map_err(|e| format!("Failed to create topic {topic_name}: {e}"))?;

    let topic_identifier = Identifier::named(topic_name)
        .map_err(|e| format!("Failed to create topic label for {topic_name}: {e}"))?;

    for batch in 0..(OPERATION_LOOP_COUNT / OPERATION_BATCH_SIZE) {
        let mut messages = Vec::new();
        for msg_idx in 0..OPERATION_BATCH_SIZE {
            let message_id = ((batch * OPERATION_BATCH_SIZE) + msg_idx) as u128;
            let payload = bytes::Bytes::from(format!(
                "Log message #{} - generating logs for rotation test",
                message_id
            ));

            let message = IggyMessage::builder()
                .id(message_id)
                .payload(payload)
                .build()
                .map_err(|e| format!("Failed to build message: {e}"))?;

            messages.push(message);
        }

        client
            .send_messages(
                &stream_identifier,
                &topic_identifier,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .map_err(|e| format!("Failed to send messages: {e}"))?;
    }

    // Wait for server to flush log buffer to disk
    sleep(Duration::from_millis(200)).await;

    client
        .delete_stream(&stream_identifier)
        .await
        .map_err(|e| format!("Failed to remove stream {stream_name}: {e}"))?;

    Ok(())
}

async fn validate_log_rotation_rules(log_dir: &Path) -> Result<(), String> {
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

    let from_mb_to_bytes: u64 = 1000 * 1000;
    let mut total_log_size = IggyByteSize::new(0);
    let max_single_mb = MAX_SINGLE_LOG_SIZE.as_bytes_u64() / from_mb_to_bytes;
    let max_total_mb = MAX_TOTAL_LOG_SIZE.as_bytes_u64() / from_mb_to_bytes;

    for log_file in valid_log_files {
        let file_metadata = fs::metadata(&log_file).await.map_err(|e| {
            format!(
                "Failed to get metadata for file '{}': {}",
                log_file.display(),
                e
            )
        })?;

        let file_size_bytes = file_metadata.len();

        // In fact,  due to the write cache mechanism,  the actual size  of  the  logs
        // written to the file will be slightly larger than expected, so there ignores
        // tiny minor overflow by comparing integer  MB  values instead of exact bytes
        let current_single_mb = file_size_bytes / from_mb_to_bytes;
        if current_single_mb > max_single_mb {
            return Err(format!(
                "Single log file exceeds maximum allowed size: '{}'",
                log_file.display()
            ));
        }

        total_log_size += IggyByteSize::new(file_size_bytes);
    }

    let current_total_mb = total_log_size.as_bytes_u64() / from_mb_to_bytes;
    if current_total_mb > max_total_mb {
        return Err(format!(
            "Total size of all log files exceeds maximum allowed size: '{}'",
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
