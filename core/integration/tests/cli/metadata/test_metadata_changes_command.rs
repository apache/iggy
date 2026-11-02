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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::{Client, Identifier, StreamCreateOptions};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

const STREAM: &str = "metadata-cli";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Read {
    Changes,
    CurrentState,
    CurrentStateOfChildGroup,
}

struct TestMetadataChangesCmd {
    read: Read,
}

#[async_trait]
impl IggyCmdTestCase for TestMetadataChangesCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        client
            .create_stream(STREAM, &StreamCreateOptions::default())
            .await
            .expect("stream created");
    }

    fn get_command(&self) -> IggyCmdCommand {
        let command = IggyCmdCommand::new()
            .arg("metadata")
            .arg("changes")
            .with_env_credentials();
        match self.read {
            Read::Changes => command.arg("--groups").arg("streams"),
            Read::CurrentState => command
                .arg("--from")
                .arg("current")
                .arg("--groups")
                .arg("streams"),
            Read::CurrentStateOfChildGroup => command
                .arg("--from")
                .arg("current")
                .arg("--groups")
                .arg("topics"),
        }
    }

    fn verify_command(&self, command_state: Assert) {
        match self.read {
            Read::Changes => {
                command_state
                    .success()
                    .stderr(starts_with("Executing metadata changes command\n"))
                    .stdout(starts_with(r#"{"status":"ok""#))
                    .stdout(contains(r#""kind":"stream_created""#))
                    .stdout(contains(format!(r#""name":"{STREAM}""#)));
            }
            Read::CurrentState => {
                command_state
                    .success()
                    .stdout(contains(r#""phase":"current""#))
                    .stdout(contains(format!(r#""name":"{STREAM}""#)))
                    .stdout(contains(r#""current_page_token":null"#));
            }
            Read::CurrentStateOfChildGroup => {
                command_state
                    .success()
                    .stdout(contains(r#""phase":"current""#))
                    .stdout(contains(format!(r#""name":"{STREAM}""#)));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        client
            .delete_stream(&Identifier::named(STREAM).expect("valid name"))
            .await
            .expect("stream deleted");
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    for read in [
        Read::Changes,
        Read::CurrentState,
        Read::CurrentStateOfChildGroup,
    ] {
        let mut iggy_cmd_test = IggyCmdTest::default();
        iggy_cmd_test.setup().await;
        iggy_cmd_test
            .execute_test(TestMetadataChangesCmd { read })
            .await;
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["metadata", "changes", "--help"],
            format!(
                r#"read the metadata change log

Prints every answered page as one JSON object per line: the change
records or current state items, the status and the cursor to continue
from. Requires root or the global manage_servers permission.

Examples
 iggy metadata changes
 iggy metadata changes --from latest
 iggy metadata changes --after mc1_... --groups streams,topics
 iggy metadata changes --from current --groups streams,topics --follow

{USAGE_PREFIX} metadata changes [OPTIONS]

Options:
      --from <FROM>
          Where to start reading
{CLAP_INDENT}
          earliest: the oldest operation the log still holds completely,
          latest: no records, only the cursor at the newest record,
          current: the current state, page by page, then its catch-up cursor.
{CLAP_INDENT}
          [default: earliest]
          [possible values: earliest, latest, current]

      --after <AFTER>
          Continue after this cursor, the `next` value of an earlier page

      --groups <GROUPS>
          Groups to read, comma separated, every group when omitted
{CLAP_INDENT}
          streams, topics, partitions, consumer_groups, consumer_group_members,
          users, personal_access_tokens, sessions

      --since <SINCE>
          Oldest prepare timestamp to include, in microseconds since the epoch

      --until <UNTIL>
          Newest prepare timestamp to include, in microseconds since the epoch

  -l, --limit <LIMIT>
          Records or items per page, 1 to 1000
{CLAP_INDENT}
          [default: 100]

  -f, --follow
          Keep reading new changes once the log has no more
{CLAP_INDENT}
          When the cursor's history is no longer retained, the command rebuilds
          from the current state and goes on following.

      --interval <INTERVAL>
          Pause between pages while following
{CLAP_INDENT}
          [default: 1s]

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
            vec!["metadata", "changes", "-h"],
            format!(
                r#"read the metadata change log

{USAGE_PREFIX} metadata changes [OPTIONS]

Options:
      --from <FROM>          Where to start reading [default: earliest] [possible values: earliest, latest, current]
      --after <AFTER>        Continue after this cursor, the `next` value of an earlier page
      --groups <GROUPS>      Groups to read, comma separated, every group when omitted
      --since <SINCE>        Oldest prepare timestamp to include, in microseconds since the epoch
      --until <UNTIL>        Newest prepare timestamp to include, in microseconds since the epoch
  -l, --limit <LIMIT>        Records or items per page, 1 to 1000 [default: 100]
  -f, --follow               Keep reading new changes once the log has no more
      --interval <INTERVAL>  Pause between pages while following [default: 1s]
  -h, --help                 Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
