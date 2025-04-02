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
use crate::cli::message::test_message_poll_to_file_command::TestMessagePollToFileCmd;
use crate::cli::message::test_message_send_from_file_command::TestMessageSendFromFileCmd;
use iggy::prelude::*;
use serial_test::parallel;
use std::collections::HashMap;
use std::str::FromStr;

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_messages: Vec<&str> = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris",
        "nisi ut aliquip ex ea commodo consequat",
        "Duis aute irure dolor in reprehenderit in voluptate velit esse",
        "cillum dolore eu fugiat nulla pariatur",
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa",
        "qui officia deserunt mollit anim id est laborum",
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem",
        "accusantium doloremque laudantium, totam rem aperiam, eaque ipsa",
    ];

    let test_headers = HashMap::from([
        (
            HeaderKey::from_str("HeaderKey1").unwrap(),
            HeaderValue::from_str("HeaderValue1").unwrap(),
        ),
        (
            HeaderKey::from_str("HeaderKey2").unwrap(),
            HeaderValue::from_str("HeaderValue2").unwrap(),
        ),
        (
            HeaderKey::from_str("HeaderKey3").unwrap(),
            HeaderValue::from_str("HeaderValue3").unwrap(),
        ),
    ]);

    iggy_cmd_test.setup().await;

    let temp_file = tempfile::Builder::new().tempfile().unwrap();
    let temp_path = temp_file.path().to_path_buf();
    temp_file.close().unwrap();
    let temp_path_str = temp_path.to_str().unwrap();

    let message_count = test_messages.len();

    iggy_cmd_test
        .execute_test(TestMessagePollToFileCmd::new(
            "input_stream",
            "input_topic",
            &test_messages,
            message_count,
            PollingStrategy::offset(0),
            test_headers.clone(),
            temp_path_str,
            false,
        ))
        .await;

    iggy_cmd_test
        .execute_test(TestMessageSendFromFileCmd::new(
            false,
            temp_path_str,
            "output_stream",
            "output_topic",
            &test_messages,
            message_count,
            test_headers,
        ))
        .await;
}
