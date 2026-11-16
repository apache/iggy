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

use crate::cli::common::json_output::{JsonCase, JsonOutputTarget, run_json_output_cases};
use async_trait::async_trait;
use iggy::prelude::{Client, StreamCreateOptions};
use serde_json::Value;
use serial_test::parallel;

#[derive(Clone, Copy)]
struct StreamTarget;

#[async_trait]
impl JsonOutputTarget for StreamTarget {
    const ITEM_NAME: &'static str = "json-stream";

    async fn prepare(&self, case: JsonCase, client: &dyn Client) {
        if !matches!(
            case,
            JsonCase::Empty | JsonCase::Missing | JsonCase::Conflict
        ) {
            client
                .create_stream("json-stream", &StreamCreateOptions::default())
                .await
                .unwrap();
        }
    }

    async fn expected(&self, case: JsonCase, client: &dyn Client) -> Option<Value> {
        match case {
            JsonCase::Get => Some(
                serde_json::to_value(
                    client
                        .get_stream(&"json-stream".try_into().unwrap())
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .unwrap(),
            ),
            JsonCase::List | JsonCase::Empty => {
                Some(serde_json::to_value(client.get_streams().await.unwrap()).unwrap())
            }
            _ => None,
        }
    }

    fn args(&self, case: JsonCase) -> Vec<&'static str> {
        let args = match case {
            JsonCase::Get => vec!["get", "json-stream", "-o", "json"],
            JsonCase::Missing => vec!["get", "missing-stream", "-o", "json"],
            JsonCase::List | JsonCase::Empty => vec!["list", "-o", "json"],
            JsonCase::Conflict => vec!["list", "-l", "list", "-o", "json"],
            JsonCase::Table => vec!["list", "-o", "table"],
            JsonCase::TextList => vec!["list", "-o", "list"],
        };
        [vec!["stream"], args].concat()
    }

    async fn cleanup(&self, case: JsonCase, client: &dyn Client) {
        if !matches!(
            case,
            JsonCase::Empty | JsonCase::Missing | JsonCase::Conflict
        ) {
            client
                .delete_stream(&"json-stream".try_into().unwrap())
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
#[parallel]
async fn given_stream_output_when_requested_should_preserve_json_and_output_modes() {
    run_json_output_cases(StreamTarget).await;
}

#[cfg(target_os = "linux")]
mod stdout_errors {
    use std::fs::OpenOptions;
    use std::process::Command;

    use iggy::prelude::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
    use integration::harness::cargo_bin_path;
    use serial_test::parallel;

    use crate::cli::common::IggyCmdTest;

    #[tokio::test]
    #[parallel]
    async fn given_full_stdout_when_listing_streams_as_json_should_return_an_error() {
        let mut test = IggyCmdTest::default();
        test.setup().await;
        let address = test.get_tcp_server_address().unwrap();
        let stdout = OpenOptions::new().write(true).open("/dev/full").unwrap();

        let output = Command::new(cargo_bin_path("iggy").unwrap())
            .env("IGGY_USERNAME", DEFAULT_ROOT_USERNAME)
            .env("IGGY_PASSWORD", DEFAULT_ROOT_PASSWORD)
            .env("IGGY_TOKEN", "")
            .args([
                "--transport",
                "tcp",
                "--tcp-server-address",
                &address,
                "--quiet",
                "stream",
                "list",
                "--output",
                "json",
            ])
            .stdout(stdout)
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("os error 28"), "{stderr}");
    }
}
