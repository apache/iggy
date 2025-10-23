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

use crate::cli::common::CLAP_INDENT;
use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use predicates::str::starts_with;
use serial_test::parallel;
use std::{
    fs::{self, File},
    io::Read,
};
use tempfile::tempdir;
use zip::ZipArchive;

struct TestSnapshotCmd {
    temp_out_dir: String,
}

impl TestSnapshotCmd {
    fn new(temp_out_dir: String) -> Self {
        TestSnapshotCmd { temp_out_dir }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestSnapshotCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("snapshot")
            .arg("--compression")
            .arg("deflated")
            .arg("--snapshot-types")
            .arg("test")
            .arg("server_logs")
            .arg("--out-dir")
            .arg(self.temp_out_dir.as_str())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing snapshot command\n"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    let temp_out_dir = tempdir().unwrap();
    iggy_cmd_test
        .execute_test(TestSnapshotCmd::new(
            temp_out_dir.path().to_str().unwrap().to_string(),
        ))
        .await;

    let snapshot_file = fs::read_dir(&temp_out_dir)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| {
            let file_name = entry.file_name();
            file_name.to_string_lossy().starts_with("snapshot")
        })
        .unwrap();

    let zip_path = snapshot_file.path();
    let file = File::open(zip_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();

    let contents = {
        let mut test_file = archive.by_name("test.txt").unwrap();
        let mut contents = String::new();
        test_file.read_to_string(&mut contents).unwrap();
        contents
    };

    assert_eq!(contents.trim(), "test");

    let contents = {
        let mut server_logs_file = archive.by_name("server_logs.txt").unwrap();
        let mut contents = String::new();
        server_logs_file.read_to_string(&mut contents).unwrap();
        contents
    };

    assert!(contents.trim().contains("INFO shard-"));
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["snapshot", "--help"],
            format!(
                r#"collect iggy server troubleshooting data

{USAGE_PREFIX} snapshot [OPTIONS]

Options:
  -c, --compression <COMPRESSION>
          Specify snapshot compression method.
{CLAP_INDENT}
          Available options:
{CLAP_INDENT}
          - `stored`: No compression
          - `deflated`: Standard deflate compression
          - `bzip2`: Higher compression ratio but slower
          - `zstd`: Fast compression and decompression
          - `lzma`: High compression, suitable for large files
          - `xz`: Similar to `lzma` but often faster in decompression
{CLAP_INDENT}
          Examples:
          - `--compression bzip2` for higher compression.
          - `--compression none` to store without compression.

  -s, --snapshot-types <SNAPSHOT_TYPES>...
          Specify types of snapshots to include.
{CLAP_INDENT}
          Available snapshot types:
          - `filesystem_overview`: Provides an overview of the filesystem structure.
          - `process_list`: Captures the list of active processes.
          - `resource_usage`: Monitors CPU, memory, and other system resources.
          - `test`: Used for testing purposes.
          - `server_logs`: Server logs from the specified logging directory, useful for system diagnostics.
          - `server_config`: Server configuration.
          - `all`: Take all available snapshots.
{CLAP_INDENT}
          Examples:
          - `--snapshot-types filesystem_overview process_list`
          - `--snapshot-types resource_usage`

  -o, --out-dir <OUT_DIR>
          Define the output directory for the snapshot file.
{CLAP_INDENT}
          This directory will contain the snapshot files generated by the command.
{CLAP_INDENT}
          Examples:
          - `--out-dir /var/snapshots`
          - `--out-dir ./snapshots`

  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["snapshot", "-h"],
            format!(
                r#"collect iggy server troubleshooting data

{USAGE_PREFIX} snapshot [OPTIONS]

Options:
  -c, --compression <COMPRESSION>           Specify snapshot compression method.
  -s, --snapshot-types <SNAPSHOT_TYPES>...  Specify types of snapshots to include.
  -o, --out-dir <OUT_DIR>                   Define the output directory for the snapshot file.
  -h, --help                                Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
