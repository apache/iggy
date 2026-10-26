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

use crate::cli::common::{IggyCmdCommand, IggyCmdTestCase, ensure_keyring_store};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use predicates::str::{contains, starts_with};

#[derive(Debug)]
pub(super) struct TestSessionStatusCmd {
    server_address: String,
    active: bool,
}

impl TestSessionStatusCmd {
    pub(super) fn new(server_address: String, active: bool) -> Self {
        ensure_keyring_store();
        Self {
            server_address,
            active,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestSessionStatusCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("session").arg("status")
    }

    fn verify_command(&self, command_state: Assert) {
        let active = if self.active {
            "Yes (token presence only, freshness not verified)"
        } else {
            "No"
        };
        command_state
            .success()
            .stdout(starts_with("Executing session status command\n"))
            .stdout(contains(format!(
                "Server Address | {}",
                self.server_address
            )))
            .stdout(contains(format!("Session Active | {active}")));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}
