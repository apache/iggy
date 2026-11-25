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

use crate::cli::common::list_runs::{ListRun, execute_list_runs};
use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use futures::future::BoxFuture;
use iggy::prelude::{
    Client, Identifier, IggyExpiry, MAX_PAGE_LIMIT, StreamCreateOptions, TopicCreateOptions,
};
use iggy_common::MAX_PARTITIONS_COUNT;
use serial_test::parallel;
use std::ops::RangeInclusive;

const PAGES_STREAM: &str = "pages";
const PAGES_TOPIC: &str = "events";
const PARTITIONS: u32 = 5;
/// The field of the partition ID in a list mode row.
const ID_COLUMN: usize = 0;
/// The columns of the partition table of `topic get`.
const TABLE_HEADER: [&str; 8] = [
    "Partition id",
    "Created",
    "Segments count",
    "Current offset",
    "Size",
    "Messages count",
    "Members",
    "In sync",
];

struct TestPartitionListCmd;

#[async_trait]
impl IggyCmdTestCase for TestPartitionListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        create_topic(client, PARTITIONS).await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .args(vec!["partition", "list", PAGES_STREAM, PAGES_TOPIC])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stdout = String::from_utf8(command_state.success().get_output().stdout.clone())
            .expect("UTF-8 output");
        let mut lines = stdout.lines();
        assert_eq!(
            lines.next(),
            Some(
                format!(
                    "Executing list partitions for stream with ID: {PAGES_STREAM} and topic with ID: {PAGES_TOPIC} in table mode"
                )
                .as_str()
            )
        );
        let cells = |line: &str| -> Vec<String> {
            line.split('|')
                .map(|cell| cell.trim().to_owned())
                .filter(|cell| !cell.is_empty())
                .collect()
        };
        // The lines between rows start with `|-` or `|=`.
        let mut rows = lines.filter(|line| line.starts_with("| ")).map(cells);
        assert_eq!(rows.next(), Some(TABLE_HEADER.map(str::to_owned).to_vec()));
        let first_cells: Vec<String> = rows.map(|row| row[0].clone()).collect();
        assert_eq!(first_cells, ids(0..=PARTITIONS - 1), "{stdout}");
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestPartitionListCmd).await;
}

#[tokio::test]
#[parallel]
pub async fn should_print_pages() {
    execute_list_runs(
        seed_partitions,
        ID_COLUMN,
        vec![
            ListRun::new(
                "partition list pages events -l list",
                &["0", "1", "2", "3", "4"],
                false,
            ),
            ListRun::new(
                "partition list pages events -l list --desc --limit 2",
                &["4", "3"],
                true,
            ),
            ListRun::new(
                "partition list pages events -l list --desc --after {next}",
                &["2", "1", "0"],
                false,
            ),
        ],
    )
    .await;
}

#[tokio::test]
#[parallel]
pub async fn should_print_more_rows_than_one_page() {
    let last = MAX_PAGE_LIMIT;
    execute_list_runs(
        seed_more_partitions_than_one_page,
        ID_COLUMN,
        vec![
            ListRun {
                args: "partition list pages events -l list".to_owned(),
                rows: ids(0..=last),
                next: false,
            },
            ListRun {
                args: format!("partition list pages events -l list --limit {}", last + 1),
                rows: ids(0..=last),
                next: false,
            },
            ListRun {
                args: format!("partition list pages events -l list --limit {last}"),
                rows: ids(0..=last - 1),
                next: true,
            },
            ListRun {
                args: "partition list pages events -l list --after {next}".to_owned(),
                rows: ids(last..=last),
                next: false,
            },
        ],
    )
    .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["partition", "list", "--help"],
            format!(
                r#"List partitions for the specified topic ID
and stream ID.

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy partition list 1 1
 iggy partition list prod sensor -l list
 iggy partition list prod sensor --desc --limit 100

{USAGE_PREFIX} partition list [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list partitions
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to list partitions
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

      --desc
          Print the rows in descending order

      --limit <N>
          Print at most N rows
{CLAP_INDENT}
          If more rows follow them, the last line is `next: <CURSOR>`. Pass that cursor to --after to print the rows that follow.

      --after <CURSOR>
          Print the rows after this cursor
{CLAP_INDENT}
          The cursor comes from the `next:` line of a list with --limit. Use the same other flags as that list.

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
            vec!["partition", "list", "-h"],
            format!(
                r#"List partitions for the specified topic ID
and stream ID.

{USAGE_PREFIX} partition list [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to list partitions
  <TOPIC_ID>   Topic ID to list partitions

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
      --desc                   Print the rows in descending order
      --limit <N>              Print at most N rows
      --after <CURSOR>         Print the rows after this cursor
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}

fn ids(range: RangeInclusive<u32>) -> Vec<String> {
    range.map(|id| id.to_string()).collect()
}

fn seed_partitions(client: &dyn Client) -> BoxFuture<'_, ()> {
    Box::pin(create_topic(client, PARTITIONS))
}

/// One request creates at most [`MAX_PARTITIONS_COUNT`] partitions, so the
/// last one comes in a second request.
fn seed_more_partitions_than_one_page(client: &dyn Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        create_topic(client, MAX_PARTITIONS_COUNT).await;
        client
            .create_partitions(
                &Identifier::named(PAGES_STREAM).expect("a stream name"),
                &Identifier::named(PAGES_TOPIC).expect("a topic name"),
                MAX_PAGE_LIMIT + 1 - MAX_PARTITIONS_COUNT,
            )
            .await
            .expect("create partitions");
    })
}

async fn create_topic(client: &dyn Client, partitions_count: u32) {
    client
        .create_stream(PAGES_STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(PAGES_STREAM).expect("a stream name"),
            PAGES_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(partitions_count),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}
