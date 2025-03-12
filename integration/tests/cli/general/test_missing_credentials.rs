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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::starts_with;
use serial_test::parallel;

struct TestNoCredentialsCmd {}

#[async_trait]
impl IggyCmdTestCase for TestNoCredentialsCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("me")
    }

    fn verify_command(&self, command_state: Assert) {
        // Use starts_with without closing bracket (CI tests run with RUST_BACKTRACE which causes rust to emit
        // longer message with full stack trace at the end of which closing bracket is located).
        command_state
            .failure()
            .stderr(starts_with(
                "Error: CommandError(Iggy command line tool error\n\nCaused by:\n    Missing iggy server credentials",
            ));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_fail_with_error_message() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestNoCredentialsCmd {}).await;
}
