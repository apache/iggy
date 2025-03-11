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

use crate::cli::common::{IggyCmdTest, TestHelpCmd, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["login", "--help"],
            format!(
                r#"login to Iggy server

Command logs in to Iggy server using provided credentials and stores session token
in platform-specific secure storage. Session token is used for authentication in
subsequent commands until logout command is executed.

{USAGE_PREFIX} login [EXPIRY]...

Arguments:
  [EXPIRY]...
          Login session expiry time in human-readable format
          
          Expiry time must be expressed in human-readable format like 1hour 15min 2s.
          If not set default value 15minutes is used. Using "none" disables session expiry time.

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
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["login", "-h"],
            format!(
                r#"login to Iggy server

{USAGE_PREFIX} login [EXPIRY]...

Arguments:
  [EXPIRY]...  Login session expiry time in human-readable format

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
