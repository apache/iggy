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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd,
    TestStreamId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use futures::future::BoxFuture;
use iggy::prelude::Client;
use iggy::prelude::Identifier;
use iggy::prelude::IggyExpiry;
use iggy::prelude::StreamCreateOptions;
use iggy::prelude::TopicCreateOptions;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

const PAGES_STREAM: &str = "pages";
/// The field of the topic name in a list mode row.
const NAME_COLUMN: usize = 2;

struct TestTopicListCmd {
    stream_id: u32,
    stream_name: String,
    topic_name: String,
    using_stream_id: TestStreamId,
    output: OutputFormat,
}

impl TestTopicListCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_name: String,
        using_stream_id: TestStreamId,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_name,
            using_stream_id,
            output,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        args.extend(self.output.to_args().into_iter().map(String::from));

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list topics from stream with ID: {} in {} mode",
                stream_id, self.output
            )))
            .stdout(contains(self.topic_name.clone()));
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
        .execute_test(TestTopicListCmd::new(
            0,
            String::from("main"),
            String::from("sync"),
            TestStreamId::Named,
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            1,
            String::from("customer"),
            String::from("topic"),
            TestStreamId::Named,
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            2,
            String::from("production"),
            String::from("data"),
            TestStreamId::Named,
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_print_pages() {
    execute_list_runs(
        seed_topics,
        NAME_COLUMN,
        vec![
            ListRun::new(
                "topic list pages -l list",
                &["pages-t1", "pages-t4", "pages-t3"],
                false,
            ),
            ListRun::new(
                "topic list pages -l list --sort created --desc --limit 1",
                &["pages-t4"],
                true,
            ),
            ListRun::new(
                "topic list pages -l list --sort created --desc --after {next}",
                &["pages-t3", "pages-t1"],
                false,
            ),
            ListRun::new("topic list pages -l list --name t1", &["pages-t1"], false),
            ListRun::new(
                "topic list pages -o json --sort created --desc --limit 1",
                &["pages-t4"],
                true,
            ),
            ListRun::new(
                "topic list pages -o json --sort created --desc --after {next}",
                &["pages-t3", "pages-t1"],
                false,
            ),
        ],
    )
    .await;
}

/// Topic `pages-t4` takes the ID of the deleted `pages-t2`, so the ID order
/// is t1 t4 t3 and the creation order is t1 t3 t4.
fn seed_topics(client: &dyn Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        let stream_id = &Identifier::named(PAGES_STREAM).expect("a stream name");
        client
            .create_stream(PAGES_STREAM, &StreamCreateOptions::default())
            .await
            .expect("create stream");
        let create_topic = |name| async move {
            client
                .create_topic(
                    stream_id,
                    name,
                    &TopicCreateOptions {
                        partitions_count: Some(1),
                        message_expiry: Some(IggyExpiry::NeverExpire),
                        ..TopicCreateOptions::default()
                    },
                )
                .await
                .expect("create topic");
        };
        for name in ["pages-t1", "pages-t2", "pages-t3"] {
            create_topic(name).await;
        }
        client
            .delete_topic(
                stream_id,
                &Identifier::named("pages-t2").expect("a topic name"),
            )
            .await
            .expect("delete topic");
        create_topic("pages-t4").await;
    })
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "list", "--help"],
            format!(
                r#"List all topics in given stream ID

Stream ID can be specified as a stream name or ID

Examples
 iggy topic list 1
 iggy topic list prod
 iggy topic list prod --sort created --desc --limit 20
 iggy topic list prod --name sensor --limit 50

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list topics
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

  -o, --output <OUTPUT>
          Output format (table, list or json), cannot be combined with --list-mode
{CLAP_INDENT}
          [possible values: table, list, json]

      --sort <SORT>
          Order of the rows, by ID or by creation time
{CLAP_INDENT}
          [default: id]
          [possible values: id, created]

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

      --name <TEXT>
          Print only the rows whose name holds this text (case-sensitive)

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
            vec!["topic", "list", "-h"],
            format!(
                r#"List all topics in given stream ID

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to list topics

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -o, --output <OUTPUT>        Output format (table, list or json), cannot be combined with --list-mode [possible values: table, list, json]
      --sort <SORT>            Order of the rows, by ID or by creation time [default: id] [possible values: id, created]
      --desc                   Print the rows in descending order
      --limit <N>              Print at most N rows
      --after <CURSOR>         Print the rows after this cursor
      --name <TEXT>            Print only the rows whose name holds this text (case-sensitive)
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
