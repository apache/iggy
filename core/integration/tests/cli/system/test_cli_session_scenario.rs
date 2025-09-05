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

use crate::cli::common::IggyCmdTest;
use crate::cli::system::test_login_cmd::{TestLoginCmd, TestLoginCmdType};
use crate::cli::system::test_logout_cmd::TestLogoutCmd;
use crate::cli::system::test_me_command::{Scenario, TestMeCmd};
use iggy_common::TransportProtocol;
use serial_test::serial;

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    let server_address = iggy_cmd_test.get_tcp_server_address();
    assert!(server_address.is_some());
    let server_address = server_address.unwrap();

    // Login with admin credentials (username and password)
    iggy_cmd_test
        .execute_test(TestLoginCmd::new(
            server_address.clone(),
            TestLoginCmdType::Success,
        ))
        .await;
    // Check if session works using "me" command (which requires authentication)
    // Command shall be executed without credentials and should be successful
    iggy_cmd_test
        .execute_test(TestMeCmd::new(
            TransportProtocol::Tcp,
            Scenario::SuccessWithoutCredentials,
        ))
        .await;
    // Login again with admin credentials (username and password)
    // Command should inform about already open session
    iggy_cmd_test
        .execute_test(TestLoginCmd::new(
            server_address.clone(),
            TestLoginCmdType::AlreadyLoggedIn,
        ))
        .await;
    // Logout from the server
    iggy_cmd_test
        .execute_test(TestLogoutCmd::new(server_address.clone()))
        .await;
    // Login with admin credentials (username and password)
    // In prepare_server_state create PAT with session name, keyring shall be empty
    // Command should inform about already open session (and store PAT in keyring)
    iggy_cmd_test
        .execute_test(TestLoginCmd::new(
            server_address.clone(),
            TestLoginCmdType::AlreadyLoggedInWithToken,
        ))
        .await;
    // Logout from the server
    iggy_cmd_test
        .execute_test(TestLogoutCmd::new(server_address.clone()))
        .await;
    // Login with admin credentials (username and password) and timeout
    // Minimum timeout for session is 1 second (due to PAT timeout in SDK)
    let login_session_timeout = 1;
    iggy_cmd_test
        .execute_test(TestLoginCmd::new(
            server_address.clone(),
            TestLoginCmdType::SuccessWithTimeout(login_session_timeout),
        ))
        .await;
    // sleep for given seconds (min 1 second)
    tokio::time::sleep(tokio::time::Duration::from_secs(login_session_timeout)).await;
    // Check if session expired using "me" command (which requires authentication,
    // not provided in this case).
    // Command shall be executed without credentials and should fail with proper
    // error message.
    iggy_cmd_test
        .execute_test(TestMeCmd::new(
            TransportProtocol::Tcp,
            Scenario::FailureDueToSessionTimeout(server_address),
        ))
        .await;
}
