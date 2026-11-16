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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use serde_json::Value;

#[derive(Clone, Copy)]
pub(crate) enum JsonCase {
    Get,
    List,
    Empty,
    Missing,
    Conflict,
    Table,
    TextList,
}

/// Resource-specific parts of the `--output json` test matrix.
#[async_trait]
pub(crate) trait JsonOutputTarget: Copy + Send + Sync {
    /// Name of the item that `Get` and `List` must show.
    const ITEM_NAME: &'static str;

    async fn prepare(&self, case: JsonCase, client: &dyn Client);

    async fn expected(&self, case: JsonCase, client: &dyn Client) -> Option<Value>;

    /// Arguments after the global options, starting with the command group.
    fn args(&self, case: JsonCase) -> Vec<&'static str>;

    /// Lets a target neutralize fields that legitimately change between the
    /// server read and the CLI run.
    fn align_expected(&self, _case: JsonCase, _actual: &Value, _expected: &mut Value) {}

    async fn cleanup(&self, case: JsonCase, client: &dyn Client);
}

struct JsonOutputTestCase<T> {
    target: T,
    case: JsonCase,
    quiet: bool,
    expected: Option<Value>,
}

#[async_trait]
impl<T: JsonOutputTarget> IggyCmdTestCase for JsonOutputTestCase<T> {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        self.target.prepare(self.case, client).await;
        self.expected = self.target.expected(self.case, client).await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        let mut command = IggyCmdCommand::new().with_env_credentials();
        if self.quiet {
            command = command.opt("-q");
        }
        command.args(self.target.args(self.case))
    }

    fn verify_command(&self, command_state: Assert) {
        if matches!(self.case, JsonCase::Missing | JsonCase::Conflict) {
            let output = command_state.failure();
            assert!(output.get_output().stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.get_output().stderr);
            if matches!(self.case, JsonCase::Missing) {
                assert!(stderr.contains("not found"), "{stderr}");
            } else {
                assert!(stderr.contains("cannot be used with"), "{stderr}");
            }
            return;
        }
        let output = command_state.success();
        let stdout = &output.get_output().stdout;
        if matches!(self.case, JsonCase::Table | JsonCase::TextList) {
            assert!(String::from_utf8_lossy(stdout).contains(T::ITEM_NAME));
            return;
        }
        let value: Value = serde_json::from_slice(stdout).expect("stdout must be one JSON value");
        let mut expected = self.expected.as_ref().unwrap().clone();
        self.target.align_expected(self.case, &value, &mut expected);
        assert_eq!(value, expected);
        match self.case {
            JsonCase::Get => assert_eq!(value["name"], T::ITEM_NAME),
            JsonCase::List => assert!(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["name"] == T::ITEM_NAME)
            ),
            JsonCase::Empty => assert_eq!(value, serde_json::json!([])),
            _ => unreachable!(),
        }
        let stderr = String::from_utf8_lossy(&output.get_output().stderr);
        assert_eq!(stderr.contains("Executing "), !self.quiet, "{stderr}");
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        self.target.cleanup(self.case, client).await;
    }
}

pub(crate) async fn run_json_output_cases<T: JsonOutputTarget>(target: T) {
    let mut test = IggyCmdTest::default();
    test.setup().await;
    for case in [
        JsonCase::Get,
        JsonCase::List,
        JsonCase::Empty,
        JsonCase::Missing,
        JsonCase::Conflict,
        JsonCase::Table,
        JsonCase::TextList,
    ] {
        for quiet in [false, true] {
            if quiet && matches!(case, JsonCase::Table | JsonCase::TextList) {
                continue;
            }
            test.execute_test(JsonOutputTestCase {
                target,
                case,
                quiet,
                expected: None,
            })
            .await;
        }
    }
}
