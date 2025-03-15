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

use super::{parse_sent_message, verify_stdout_contains_expected_logs};
use crate::examples::{IggyExampleTest, IggyExampleTestCase};
use serial_test::parallel;

struct TestBasic<'a> {
    expected_producer_output: Vec<&'a str>,
    expected_consumer_output: Vec<&'a str>,
}

impl IggyExampleTestCase for TestBasic<'_> {
    fn verify_log_output(&self, producer_stdout: &str, consumer_stdout: &str) {
        verify_stdout_contains_expected_logs(
            producer_stdout,
            consumer_stdout,
            &self.expected_producer_output,
            &self.expected_consumer_output,
        );
    }

    fn verify_message_output(&self, producer_stdout: &str, consumer_stdout: &str) {
        let producer_captured_message = parse_sent_message(producer_stdout);
        // remove leading + trailing `,` and add trailing elipses
        let producer_formatted_message = format!(
            "{}...",
            &producer_captured_message[1..producer_captured_message.len() - 1]
        );
        assert!(
            consumer_stdout.contains(&producer_formatted_message),
            "Consumer output does not contain expected line: '{}'",
            &producer_formatted_message
        );
    }
}

#[tokio::test]
#[parallel]
async fn should_successfully_execute() {
    let mut iggy_example_test = IggyExampleTest::new("basic");
    iggy_example_test.setup(false).await;

    iggy_example_test
        .execute_test(TestBasic {
            expected_producer_output: vec![
                "Basic producer has started, selected transport: tcp",
                "Stream does not exist, creating...",
                "Messages will be sent to stream: example-stream, topic: example-topic, partition: 1 with interval 1ms.",
            ],
            expected_consumer_output: vec![
                "Basic consumer has started, selected transport: tcp",
                "Validating if stream: example-stream exists..",
                "Stream: example-stream was found.",
                "Validating if topic: example-topic exists..",
                "Topic: example-topic was found.",
                "Messages will be polled by consumer: 1 from stream: example-stream, topic: example-topic, partition: 1 with interval 1ms."
            ],
        })
        .await;
}
