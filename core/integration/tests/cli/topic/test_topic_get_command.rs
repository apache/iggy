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

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    TestTopicId, USAGE_PREFIX, clear_session_entry,
};
use assert_cmd::Command;
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::Durability;
use iggy::prelude::IggyExpiry;
use iggy::prelude::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::prelude::{IggyMessage, Partitioning, StreamClient, TopicClient, TopicCreateOptions};
use integration::harness::{TestHarnessBuilder, TestServerConfig};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    partitions_count: u32,
    durability: Durability,
    consumer_offset_durability: Durability,
    expected_partitions: Vec<Vec<String>>,
    expected_options: Vec<Vec<String>>,
    expected_size: String,
    expected_messages_count: u64,
    populate_partitions: bool,
}

impl TestTopicGetCmd {
    fn new(
        stream_name: String,
        topic_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id: 0,
            stream_name,
            topic_id: 0,
            topic_name,
            using_stream_id,
            using_topic_id,
            partitions_count: 1,
            durability: Durability::Replicated,
            consumer_offset_durability: Durability::Replicated,
            expected_partitions: Vec::new(),
            expected_options: Vec::new(),
            expected_size: String::new(),
            expected_messages_count: 0,
            populate_partitions: false,
        }
    }

    fn with_topic_options(mut self, partitions_count: u32, durability: Durability) -> Self {
        self.partitions_count = partitions_count;
        self.durability = durability;
        self.consumer_offset_durability = match durability {
            Durability::Persisted => Durability::Replicated,
            Durability::Replicated => Durability::Persisted,
        };
        self.populate_partitions = true;
        self
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name)
            .await
            .expect("stream should be created");
        self.stream_id = stream.id;

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(self.partitions_count),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    durability: self.durability,
                    consumer_offset_durability: self.consumer_offset_durability,
                    preallocate_segments: Some(false),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .expect("topic should be created");
        self.topic_id = topic.id;

        if self.populate_partitions {
            for (index, partition) in topic.partitions.iter().enumerate() {
                let mut messages = (0..=index)
                    .map(|_| "topic get test payload".parse::<IggyMessage>().unwrap())
                    .collect::<Vec<_>>();
                client
                    .send_messages(
                        &self.stream_id.try_into().unwrap(),
                        &self.topic_id.try_into().unwrap(),
                        &Partitioning::partition_id(partition.id),
                        &mut messages,
                    )
                    .await
                    .expect("messages should be sent");
            }
        }

        let details = client
            .get_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
            )
            .await
            .expect("topic details should be retrievable")
            .expect("created topic should exist");
        self.expected_partitions = details
            .partitions
            .iter()
            .map(|partition| {
                vec![
                    partition.id.to_string(),
                    partition.created_at.to_utc_string("%Y-%m-%d %H:%M:%S"),
                    partition.segments_count.to_string(),
                    partition.current_offset.to_string(),
                    partition.size.to_string(),
                    partition.messages_count.to_string(),
                ]
            })
            .collect();
        self.expected_options = details
            .options
            .iter()
            .map(|(key, option)| {
                vec![
                    String::from_utf8_lossy(key.as_bytes()).into_owned(),
                    option.value.to_string_value(),
                ]
            })
            .collect();
        self.expected_size = details.size.to_string();
        self.expected_messages_count = details.messages_count;
        assert_eq!(
            self.expected_partitions.len(),
            self.partitions_count as usize
        );
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("get")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let topic_id = match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        };

        let start_message =
            format!("Executing get topic with ID: {topic_id} from stream with ID: {stream_id}");

        let assertion = command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!("Topic id            | {}", self.topic_id)))
            .stdout(contains(format!(
                "Topic name          | {}",
                self.topic_name
            )))
            .stdout(contains(format!(
                "Topic size          | {}",
                self.expected_size
            )))
            .stdout(contains("Message expiry      | unlimited"))
            .stdout(contains("Max topic size      | unlimited"))
            .stdout(contains(format!(
                "Topic message count | {}",
                self.expected_messages_count
            )))
            .stdout(contains(format!(
                "Partitions count    | {}",
                self.partitions_count
            )));

        let stdout = std::str::from_utf8(&assertion.get_output().stdout)
            .expect("CLI output should be UTF-8");
        let mut partition_rows =
            nested_table_rows(stdout, "Partitions", 6).expect("complete partition rows");
        assert_eq!(
            partition_rows.remove(0),
            [
                "Partition id",
                "Created",
                "Segments count",
                "Current offset",
                "Size",
                "Messages count"
            ]
        );
        let mut expected_partitions = self.expected_partitions.clone();
        partition_rows.sort();
        expected_partitions.sort();
        assert_eq!(partition_rows, expected_partitions);

        let mut option_rows =
            nested_table_rows(stdout, "Options", 2).expect("complete option rows");
        assert_eq!(option_rows.remove(0), ["Option", "Value"]);
        let mut expected_options = self.expected_options.clone();
        option_rows.sort();
        expected_options.sort();
        assert_eq!(option_rows, expected_options);
        assert!(option_rows.contains(&vec!["durability".to_owned(), self.durability.to_string()]));
        assert!(option_rows.contains(&vec![
            "consumer_offset_durability".to_owned(),
            self.consumer_offset_durability.to_string(),
        ]));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("main"),
            String::from("sync"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("customer"),
            String::from("probe"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("development"),
            String::from("testing"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("other"),
            String::from("topic"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_multiple_partitions_and_persisted_durability() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(
            TestTopicGetCmd::new(
                String::from("durable-stream"),
                String::from("multi-partition-topic"),
                TestStreamId::Named,
                TestTopicId::Named,
            )
            .with_topic_options(3, Durability::Persisted),
        )
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_multiple_partitions_using_numeric_identifiers() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(
            TestTopicGetCmd::new(
                String::from("numeric-stream"),
                String::from("numeric-topic"),
                TestStreamId::Numeric,
                TestTopicId::Numeric,
            )
            .with_topic_options(3, Durability::Replicated),
        )
        .await;
}

struct TestMissingTopicGetCmd;

#[async_trait]
impl IggyCmdTestCase for TestMissingTopicGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        client.create_stream("missing-topic-stream").await.unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .args(vec![
                "topic",
                "get",
                "missing-topic-stream",
                "missing-topic",
            ])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(contains(
                "Topic with ID: missing-topic in stream missing-topic-stream was not found",
            ))
            .stdout(predicates::str::contains("Partitions").not());
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        client
            .delete_stream(&"missing-topic-stream".try_into().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
#[parallel]
pub async fn should_report_missing_topic() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestMissingTopicGetCmd).await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "--help"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy topic get 1 1
 iggy topic get prod 2
 iggy topic get test debugs
 iggy topic get 2 debugs

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

Options:
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
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "-h"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to get topic
  <TOPIC_ID>   Topic ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}

fn nested_table_rows(
    stdout: &str,
    property: &str,
    columns: usize,
) -> Result<Vec<Vec<String>>, &'static str> {
    let expected_header: &[&str] = match property {
        "Partitions" => &[
            "Partition id",
            "Created",
            "Segments count",
            "Current offset",
            "Size",
            "Messages count",
        ],
        "Options" => &["Option", "Value"],
        _ => return Err("unsupported nested table property"),
    };
    if columns != expected_header.len() {
        return Err("incorrect nested table column count");
    }

    let mut lines = stdout.lines();
    let header_line = lines
        .find(|line| {
            line.strip_prefix('|')
                .and_then(|body| body.split_once('|'))
                .is_some_and(|(key, _)| key.trim() == property)
        })
        .ok_or("nested table section was not found")?;
    let (key, value) = nested_table_body(header_line)?;
    let header_cells = value.split('|').map(str::trim).collect::<Vec<_>>();
    if header_cells != expected_header {
        return Err("incorrect nested table header");
    }

    // Outer borders span both cells; an inner separator never closes the section.
    let border_body = format!(
        "{}+{}",
        "-".repeat(key.chars().count()),
        "-".repeat(value.chars().count())
    );
    let row_boundary = format!("|{border_body}|");
    let table_boundary = format!("+{border_body}+");
    let separator_line = lines
        .next()
        .ok_or("nested table header separator is missing")?;
    let (separator_key, separator) = nested_table_body(separator_line)?;
    let separator = separator.trim();
    if !separator_key.trim().is_empty()
        || separator.is_empty()
        || !separator.bytes().all(|character| character == b'=')
        || separator.len() > value.chars().count().saturating_sub(2)
    {
        return Err("invalid nested table header separator");
    }

    let header_widths = value
        .strip_prefix(' ')
        .ok_or("nested table header is missing its padding")?
        .split('|')
        .take(columns - 1)
        .map(|cell| cell.chars().count())
        .collect::<Vec<_>>();
    let used_width = header_widths.iter().sum::<usize>() + columns - 1;
    let last_width = separator
        .len()
        .checked_sub(used_width)
        .filter(|width| *width >= expected_header[columns - 1].len() + 2)
        .ok_or("invalid nested table separator width")?;
    let inner_row_separator = header_widths
        .into_iter()
        .chain([last_width])
        .map(|width| "-".repeat(width))
        .collect::<Vec<_>>()
        .join("+");

    let mut rows = vec![header_cells.into_iter().map(str::to_owned).collect()];
    let mut pending_record = false;
    while let Some(line) = lines.next() {
        if line == row_boundary || line == table_boundary {
            if rows.len() == 1 || pending_record {
                return Err("nested table is empty or ends after a row separator");
            }
            if line == row_boundary {
                let next_line = lines.next().ok_or("next topic property is missing")?;
                let (next_key, _) = nested_table_body(next_line)?;
                if next_key.trim().is_empty() || next_key.trim() == property {
                    return Err("nested table boundary does not start another property");
                }
            }
            return Ok(rows);
        }
        let (key, value) = nested_table_body(line)?;
        if !key.trim().is_empty() {
            return Err("unexpected property within nested table");
        }
        let value = value.trim();
        if value == inner_row_separator {
            if rows.len() == 1 || pending_record {
                return Err("unexpected nested table row separator");
            }
            pending_record = true;
            continue;
        }
        let cells = value.split('|').map(str::trim).collect::<Vec<_>>();
        if cells.len() != columns || cells.iter().any(|cell| cell.is_empty()) {
            return Err("nested table row has missing, empty, or extra cells");
        }
        if property == "Partitions" {
            let partition_id = cells[0]
                .parse::<u32>()
                .map_err(|_| "invalid partition id")?;
            for index in [2, 3, 5] {
                cells[index]
                    .parse::<u64>()
                    .map_err(|_| "invalid numeric partition field")?;
            }
            if rows[1..]
                .iter()
                .any(|row: &Vec<String>| row[0].parse::<u32>().ok() == Some(partition_id))
            {
                return Err("duplicate partition id");
            }
        }
        rows.push(cells.into_iter().map(str::to_owned).collect());
        pending_record = false;
    }
    Err("nested table closing boundary is missing")
}

fn nested_table_body(line: &str) -> Result<(&str, &str), &'static str> {
    line.strip_prefix('|')
        .and_then(|body| body.strip_suffix('|'))
        .ok_or("nested table row is missing an outer delimiter")?
        .split_once('|')
        .ok_or("nested table row is missing its property separator")
}

const PARTITION_HEADER: &str = " Partition id | Created             | Segments count | Current offset | Size    | Messages count ";
const VALID_PARTITION: &str = "1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | 4";

fn nested_rows_fixture(records: &[&str]) -> String {
    let header_width = PARTITION_HEADER.len();
    let boundary = format!("|------------+{}|", "-".repeat(header_width + 2));
    let options = " Option                    | Value     ";
    format!(
        "| Partitions | {PARTITION_HEADER} |\n|            | {} |\n{}\n{boundary}\n\
         | Options    | {options:header_width$} |\n\
         |            | {:header_width$} |\n\
         |            | {:header_width$} |\n+------------+{}+\n",
        "=".repeat(header_width),
        records.join("\n"),
        "=".repeat(options.len()),
        " consumer_offset_durability | persisted ",
        "-".repeat(header_width + 2),
    )
}

#[test]
fn nested_rows_preserve_complete_records_and_exact_option_keys() {
    let stdout = nested_rows_fixture(&[
        "|            | 1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | 4 |",
        "|            | --------------+---------------------+----------------+----------------+---------+---------------- |",
        "|            | 2 | 2026-10-13 12:00:01 | 3 | 31 | 2.34 KB | 5 |",
    ]);
    let partitions = nested_table_rows(&stdout, "Partitions", 6).unwrap();
    assert_eq!(partitions.len(), 3);
    assert_eq!(
        partitions[1],
        ["1", "2026-10-13 12:00:00", "2", "30", "1.23 KB", "4"]
    );
    assert_eq!(
        partitions[2],
        ["2", "2026-10-13 12:00:01", "3", "31", "2.34 KB", "5"]
    );
    let options = nested_table_rows(&stdout, "Options", 2).unwrap();
    assert_eq!(
        options,
        [
            vec!["Option", "Value"],
            vec!["consumer_offset_durability", "persisted"]
        ]
    );
    assert!(!options.contains(&vec!["durability".to_owned(), "persisted".to_owned()]));
}

#[test]
fn nested_rows_reject_malformed_partition_records_before_and_after_valid_rows() {
    let valid = format!("|            | {VALID_PARTITION} |");
    for malformed in [
        "| | 1 | 2026-10-13 12:00:00 | 2 |\n| | 30 | 1.23 KB | 4 |",
        "| | 1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB |",
        "| | 1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | 4 | extra |",
        "| | 1 | 2026-10-13 12:00:00 | 2 | 30 | | 4 |",
        "  | 1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | 4 |",
        "unframed data row",
        "| | |",
        "| | 2 | 2026-10-13 12:00:01 | 3 | 31 | 2.34 KB | 5",
    ] {
        for records in [[malformed, valid.as_str()], [valid.as_str(), malformed]] {
            let stdout = nested_rows_fixture(&records);
            assert!(
                nested_table_rows(&stdout, "Partitions", 6).is_err(),
                "malformed record must invalidate the table: {malformed}"
            );
        }
    }
}

#[test]
fn nested_rows_reject_malformed_separator_like_lines() {
    let valid = format!("|            | {VALID_PARTITION} |");
    let malformed = "| | 2 | truncated";
    for separator in [
        "|-----|",
        "+-----+",
        "|-----+-----|",
        "+-----+-----+",
        "|---=--+-----|",
        "|            | ===== |",
        "|            | -----+----- |",
        "|            | ---=--+-----+-----+-----+-----+----- |",
        "|            | ------+-----+-----+-----+-----+----- |",
        "|            | |",
    ] {
        for records in [
            vec![separator, malformed, valid.as_str()],
            vec![valid.as_str(), separator, malformed],
            vec![malformed, separator, valid.as_str()],
            vec![valid.as_str(), separator],
        ] {
            let stdout = nested_rows_fixture(&records);
            assert!(
                nested_table_rows(&stdout, "Partitions", 6).is_err(),
                "invalid separator must not hide records: {separator}"
            );
        }
    }
}

#[test]
fn nested_rows_reject_multiple_malformed_rows() {
    let valid = format!("|            | {VALID_PARTITION} |");
    for records in [
        vec!["| | truncated |", "| | |", valid.as_str()],
        vec![valid.as_str(), "| | truncated |", valid.as_str(), "| | |"],
    ] {
        assert!(nested_table_rows(&nested_rows_fixture(&records), "Partitions", 6).is_err());
    }
}

#[test]
fn nested_rows_require_header_separator_records_and_closure() {
    let valid = format!("|            | {VALID_PARTITION} |");
    let stdout = nested_rows_fixture(&[&valid]);
    let partition_output = stdout.split("| Options").next().unwrap();
    let unclosed = partition_output
        .lines()
        .take(3)
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        nested_table_rows(&unclosed, "Partitions", 6),
        Err("nested table closing boundary is missing")
    );
    assert!(nested_table_rows(&nested_rows_fixture(&[]), "Partitions", 6).is_err());
    assert!(
        nested_table_rows(
            &stdout.replace("Partition id", "Wrong header"),
            "Partitions",
            6
        )
        .is_err()
    );
    assert!(nested_table_rows(&stdout.replacen('=', "-", 1), "Partitions", 6).is_err());
    assert!(nested_table_rows("", "Partitions", 6).is_err());
    let row_boundary = format!("|------------+{}|", "-".repeat(PARTITION_HEADER.len() + 2));
    let table_boundary = row_boundary.replace('|', "+");
    let closed = partition_output.replace(&row_boundary, &table_boundary);
    assert_eq!(
        nested_table_rows(&closed, "Partitions", 6).unwrap().len(),
        2
    );
}

#[test]
fn nested_rows_reject_missing_closing_delimiter() {
    let valid = format!("|            | {VALID_PARTITION} |");
    let truncated = "| | 2 | 2026-10-13 12:00:01 | 3 | 31 | 2.34 KB | 5";
    for records in [[truncated, valid.as_str()], [valid.as_str(), truncated]] {
        assert_eq!(
            nested_table_rows(&nested_rows_fixture(&records), "Partitions", 6),
            Err("nested table row is missing an outer delimiter")
        );
    }
}

#[test]
fn nested_rows_reject_boundaries_followed_by_partition_records() {
    let valid = format!("|            | {VALID_PARTITION} |");
    let boundary = format!("|------------+{}|", "-".repeat(PARTITION_HEADER.len() + 2));
    let stdout = nested_rows_fixture(&[&valid, &boundary, "| | truncated |"]);
    assert!(nested_table_rows(&stdout, "Partitions", 6).is_err());
}

#[test]
fn nested_rows_reject_invalid_numeric_fields_and_duplicate_partition_ids() {
    let valid = format!("|            | {VALID_PARTITION} |");
    assert!(nested_table_rows(&nested_rows_fixture(&[&valid, &valid]), "Partitions", 6).is_err());
    for malformed in [
        "| | bad | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | 4 |",
        "| | 1 | 2026-10-13 12:00:00 | bad | 30 | 1.23 KB | 4 |",
        "| | 1 | 2026-10-13 12:00:00 | 2 | bad | 1.23 KB | 4 |",
        "| | 1 | 2026-10-13 12:00:00 | 2 | 30 | 1.23 KB | bad |",
        "| | - | - | - | - | - | - |",
    ] {
        assert!(nested_table_rows(&nested_rows_fixture(&[malformed]), "Partitions", 6).is_err());
    }
}

#[tokio::test]
#[parallel]
#[ignore = "opt-in end-to-end test of the CLI persisted durability flag"]
pub async fn should_show_persisted_durability_created_through_cli() {
    let mut harness = TestHarnessBuilder::default()
        .server(TestServerConfig::default())
        .build()
        .unwrap();
    harness.start().await.expect("test server should start");
    let client = harness.tcp_root_client().await.unwrap();
    let stream = client.create_stream("cli-persisted-stream").await.unwrap();
    let address = harness.server().raw_tcp_addr().unwrap();
    let socket_address = harness.server().tcp_addr().unwrap();
    clear_session_entry(socket_address);
    let cli = || {
        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();
        command.env("IGGY_TOKEN", "").env("COLUMNS", "200").args([
            "--tcp-server-address",
            &address,
            "--username",
            DEFAULT_ROOT_USERNAME,
            "--password",
            DEFAULT_ROOT_PASSWORD,
        ]);
        command
    };
    cli()
        .args([
            "topic",
            "create",
            "cli-persisted-stream",
            "cli-persisted-topic",
            "3",
            "none",
            "--durability",
            "persisted",
            "--set",
            "preallocate_segments=false",
        ])
        .assert()
        .success();

    let details = client
        .get_topic(
            &stream.id.try_into().unwrap(),
            &"cli-persisted-topic".try_into().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(details.partitions_count, 3);
    assert_eq!(details.partitions.len(), 3);
    let option = details
        .options
        .iter()
        .find(|(key, _)| key.as_bytes() == b"durability")
        .unwrap();
    assert_eq!(option.1.value.to_string_value(), "persisted");

    let assertion = cli()
        .args([
            "topic",
            "get",
            "cli-persisted-stream",
            "cli-persisted-topic",
        ])
        .assert()
        .success();
    let stdout = std::str::from_utf8(&assertion.get_output().stdout).unwrap();
    let partitions = nested_table_rows(stdout, "Partitions", 6).unwrap();
    assert_eq!(partitions.len(), 4);
    let mut actual_ids = partitions[1..]
        .iter()
        .map(|row| row[0].parse::<u32>().unwrap())
        .collect::<Vec<_>>();
    let mut expected_ids = details
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect::<Vec<_>>();
    actual_ids.sort();
    expected_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    let options = nested_table_rows(stdout, "Options", 2).unwrap();
    assert!(options.contains(&vec!["durability".to_owned(), "persisted".to_owned()]));
    assert!(options.contains(&vec![
        "consumer_offset_durability".to_owned(),
        "replicated".to_owned()
    ]));

    client
        .delete_topic(
            &stream.id.try_into().unwrap(),
            &details.id.try_into().unwrap(),
        )
        .await
        .unwrap();
    client
        .delete_stream(&stream.id.try_into().unwrap())
        .await
        .unwrap();
    clear_session_entry(socket_address);
    harness.stop().await.unwrap();
}
