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

use crate::harness::get_test_directory;
use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::*;
use iggy_common::TransportProtocol;
use std::{
    fs::{self, File},
    process::{Command, Stdio},
    thread::{self, panicking},
    time::{Duration, Instant},
};
use uuid::Uuid;

const BENCH_FILES_PREFIX: &str = "bench_";
const MESSAGE_BATCHES: u64 = 100;
const MESSAGES_PER_BATCH: u64 = 100;
const DEFAULT_NUMBER_OF_STREAMS: u64 = 8;
// Generous for a few MB of traffic even in debug builds. Exists because a
// protocol mismatch (an SDK framing the server does not speak, e.g. a
// default-features iggy-bench against a vsr cluster) hangs both sides
// silently instead of erroring.
const BENCH_WAIT_TIMEOUT: Duration = Duration::from_secs(600);

pub fn run_bench_and_wait_for_finish(
    server_addr: &str,
    transport: &TransportProtocol,
    bench: &str,
    amount_of_data_to_process: IggyByteSize,
) {
    #[allow(deprecated)]
    let mut command = Command::cargo_bin("iggy-bench").unwrap();

    let mut stderr_file_path = None;
    let mut stdout_file_path = None;

    let test_verbosity_env_var = "IGGY_TEST_VERBOSE";
    if std::env::var(test_verbosity_env_var).is_err() {
        let stderr_file = get_random_path();
        let stdout_file = get_random_path();
        stderr_file_path = Some(stderr_file);
        stdout_file_path = Some(stdout_file);
    }

    // Calculate message size based on input
    let total_bytes_to_process_per_stream =
        amount_of_data_to_process.as_bytes_u64() / DEFAULT_NUMBER_OF_STREAMS;
    let messages_total: u64 = MESSAGES_PER_BATCH * MESSAGE_BATCHES;
    let message_size = total_bytes_to_process_per_stream / messages_total;

    let messages_per_batch_str = MESSAGES_PER_BATCH.to_string();
    let message_batches_str = MESSAGE_BATCHES.to_string();
    let message_size_str = message_size.to_string();
    let transport_str = transport.to_string();

    command.args([
        "--messages-per-batch",
        messages_per_batch_str.as_str(),
        "--message-batches",
        message_batches_str.as_str(),
        "--message-size",
        message_size_str.as_str(),
        "--reuse-streams",
        bench,
        transport_str.as_str(),
        "--server-address",
        server_addr,
    ]);

    // By default, all iggy-bench logs are redirected to files,
    // and dumped to stderr when test fails. With IGGY_TEST_VERBOSE=1
    // logs are dumped to stdout during test execution.
    if std::env::var(test_verbosity_env_var).is_ok() {
        command.stdout(Stdio::inherit());
        command.stderr(Stdio::inherit());
    } else {
        command.stdout(File::create(stdout_file_path.as_ref().unwrap()).unwrap());
        stdout_file_path = Some(
            fs::canonicalize(stdout_file_path.unwrap())
                .unwrap()
                .display()
                .to_string(),
        );

        command.stderr(File::create(stderr_file_path.as_ref().unwrap()).unwrap());
        stderr_file_path = Some(
            fs::canonicalize(stderr_file_path.unwrap())
                .unwrap()
                .display()
                .to_string(),
        );
    }

    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + BENCH_WAIT_TIMEOUT;
    // A timeout does NOT panic here: doing so jumped over the capture dump and
    // the temp-file cleanup below, so every timed-out run leaked both files and
    // printed only stderr -- while iggy-bench writes its progress to stdout,
    // the one capture that explains a hang. The verdict is the assert at the end.
    let mut timed_out = false;
    let status = loop {
        match child.try_wait().unwrap() {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                timed_out = true;
                break None;
            }
            None => thread::sleep(Duration::from_millis(200)),
        }
    };

    // Nothing to drain, by construction: both branches above redirect the
    // child's stdout and stderr -- to files, or inherited under
    // `IGGY_TEST_VERBOSE` -- so no pipe exists for the poll loop to deadlock
    // against. The old `wait_with_output` capture here could only ever return
    // empty buffers for the same reason; the captures the failure path prints
    // are the redirect FILES.
    let failed = timed_out || status.is_none_or(|status| !status.success());
    if failed || panicking() {
        for (stream, path) in [("stdout", &stdout_file_path), ("stderr", &stderr_file_path)] {
            if let Some(path) = path {
                eprintln!(
                    "Iggy bench {stream}:\n{}",
                    fs::read_to_string(path).unwrap_or_default()
                );
            }
        }
    }

    if let Some(stdout_file_path) = &stdout_file_path {
        fs::remove_file(stdout_file_path).unwrap();
    }
    if let Some(stderr_file_path) = &stderr_file_path {
        fs::remove_file(stderr_file_path).unwrap();
    }

    assert!(
        !timed_out,
        "iggy-bench did not finish within {BENCH_WAIT_TIMEOUT:?}; if the server \
         runs in vsr mode, make sure iggy-bench was built with --features vsr \
         (the SDK framing is chosen at compile time)"
    );
    assert!(status.is_some_and(|status| status.success()));
}

pub fn get_random_path() -> String {
    let test_name = thread::current()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_default();

    let dir = get_test_directory(&test_name)
        .unwrap_or_else(|| std::env::current_dir().expect("Failed to get current directory"));

    let _ = fs::create_dir_all(&dir);
    dir.join(format!(
        "{}{}",
        BENCH_FILES_PREFIX,
        Uuid::now_v7().to_u128_le()
    ))
    .display()
    .to_string()
}
