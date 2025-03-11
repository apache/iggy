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

use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "help"],
            format!(
                r#"user operations

{USAGE_PREFIX} user <COMMAND>

Commands:
  create       Create user with given username and password [aliases: c]
  delete       Delete user with given ID [aliases: d]
  get          Get details of a single user with given ID [aliases: g]
  list         List all users [aliases: l]
  name         Change username for user with given ID [aliases: n]
  status       Change status for user with given ID [aliases: s]
  password     Change password for user with given ID [aliases: pwd]
  permissions  Set permissions for user with given ID [aliases: p]
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
