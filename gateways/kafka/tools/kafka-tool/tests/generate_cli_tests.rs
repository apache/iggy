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

//! CLI regression for PR #3519 review: `generate` must accept repeated `--api-key`.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn generate_accepts_repeated_api_key_flags() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let output = std::env::temp_dir().join(format!("kafka-gen-test-{nanos}"));

    let status = Command::new(env!("CARGO_BIN_EXE_kafka-message-gen"))
        .arg("generate")
        .arg("--output")
        .arg(&output)
        .arg("--api-key")
        .arg("0")
        .arg("--api-key")
        .arg("1")
        .status()
        .expect("run kafka-message-gen generate");

    assert!(
        status.success(),
        "generate should accept multiple --api-key flags (PR description / Verify parity); \
         got exit status {status:?}"
    );

    let _ = std::fs::remove_dir_all(output);
}
