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

//! Runs of a list command in list mode or JSON mode, with the page flags. A
//! run can pass the cursor of the run before it to `--after`, as a user does.

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use futures::future::BoxFuture;
use iggy::prelude::Client;
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// In the arguments of a run, the cursor that the run before it printed.
pub(crate) const NEXT: &str = "{next}";

const NEXT_PREFIX: &str = "next: ";

/// A run with these arguments prints JSON, not list mode rows.
const JSON_OUTPUT: &str = "-o json";

/// A JSON run with this flag prints a page object, without it a bare array.
const LIMIT_FLAG: &str = "--limit";

/// The key of the entity name in a JSON row.
const NAME_KEY: &str = "name";

/// Creates the entities that the runs list.
pub(crate) type Seed = for<'a> fn(&'a dyn Client) -> BoxFuture<'a, ()>;

/// One run of a list command.
pub(crate) struct ListRun {
    /// The words after `iggy`, split at spaces.
    pub(crate) args: String,
    /// The field of each row that the run must print, in order. In JSON
    /// mode, the name of each row.
    pub(crate) rows: Vec<String>,
    /// Whether the run must print a cursor: the last line `next:`, or the
    /// `next` field in JSON mode.
    pub(crate) next: bool,
}

impl ListRun {
    pub(crate) fn new(args: &str, rows: &[&str], next: bool) -> Self {
        Self {
            args: args.to_owned(),
            rows: rows.iter().map(|&row| row.to_owned()).collect(),
            next,
        }
    }
}

/// Seeds a new server, then executes the runs in order. `column` is the field
/// of a list mode row that a run compares.
pub(crate) async fn execute_list_runs(seed: Seed, column: usize, runs: Vec<ListRun>) {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    let next = Arc::new(Mutex::new(None));
    let mut seed = Some(seed);
    for run in runs {
        iggy_cmd_test
            .execute_test(TestListRunCmd {
                seed: seed.take(),
                run,
                column,
                next: Arc::clone(&next),
            })
            .await;
    }
}

struct TestListRunCmd {
    seed: Option<Seed>,
    run: ListRun,
    column: usize,
    /// The cursor that the last run printed.
    next: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl IggyCmdTestCase for TestListRunCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        if let Some(seed) = self.seed {
            seed(client).await;
        }
    }

    fn get_command(&self) -> IggyCmdCommand {
        let next = self.next.lock().expect("an unpoisoned lock").clone();
        let args: Vec<String> = self
            .run
            .args
            .split(' ')
            .map(|arg| match arg {
                NEXT => next.clone().expect("the run before printed a cursor"),
                arg => arg.to_owned(),
            })
            .collect();
        IggyCmdCommand::new().args(args).with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stdout = String::from_utf8(command_state.success().get_output().stdout.clone())
            .expect("UTF-8 output");
        let (rows, next) = if self.run.args.contains(JSON_OUTPUT) {
            self.json_rows(&stdout)
        } else {
            self.list_mode_rows(&stdout)
        };
        assert_eq!(rows, self.run.rows, "{}", self.run.args);
        assert_eq!(next.is_some(), self.run.next, "{}: {stdout}", self.run.args);
        *self.next.lock().expect("an unpoisoned lock") = next;
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

impl TestListRunCmd {
    /// The fields and the cursor of a list mode run.
    fn list_mode_rows(&self, stdout: &str) -> (Vec<String>, Option<String>) {
        let lines: Vec<&str> = stdout.lines().collect();
        let Some((executing, rest)) = lines.split_first() else {
            panic!("{}: no output", self.run.args);
        };
        assert!(executing.starts_with("Executing "), "{stdout}");
        let (rows, next) = match rest.split_last() {
            Some((last, rows)) if last.starts_with(NEXT_PREFIX) => {
                (rows, last.strip_prefix(NEXT_PREFIX))
            }
            _ => (rest, None),
        };
        let fields = rows
            .iter()
            .map(|row| {
                row.split('|')
                    .nth(self.column)
                    .expect("a list mode row")
                    .to_owned()
            })
            .collect();
        (fields, next.map(str::to_owned))
    }

    /// The names and the cursor of a JSON run. The whole stdout must be one
    /// JSON document: a page object with `--limit`, otherwise a bare array.
    fn json_rows(&self, stdout: &str) -> (Vec<String>, Option<String>) {
        let output: Value = serde_json::from_str(stdout)
            .unwrap_or_else(|error| panic!("{}: {error}: {stdout}", self.run.args));
        let (items, next) = if self.run.args.contains(LIMIT_FLAG) {
            (&output["items"], output.get("next"))
        } else {
            (&output, None)
        };
        let names = items
            .as_array()
            .unwrap_or_else(|| panic!("{}: no row array: {stdout}", self.run.args))
            .iter()
            .map(|item| item[NAME_KEY].as_str().expect("a row name").to_owned())
            .collect();
        let next = next.map(|next| next.as_str().expect("a cursor string").to_owned());
        (names, next)
    }
}
