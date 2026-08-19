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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Removes its directory on drop (including on test panic/failure), so a failed assertion here
/// cannot leak a `kafka-gen-test-*` directory into the system temp dir.
struct TempOutputDir(PathBuf);

impl Drop for TempOutputDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn has_file_with_prefix(dir: &Path, prefix: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(prefix))
    })
}

#[test]
fn generate_accepts_repeated_api_key_flags() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let output = TempOutputDir(std::env::temp_dir().join(format!("kafka-gen-test-{nanos}")));

    let status = Command::new(env!("CARGO_BIN_EXE_kafka-message-gen"))
        .arg("generate")
        .arg("--output")
        .arg(&output.0)
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

    // A successful exit status alone doesn't prove either requested key actually produced
    // fixtures - `cmd_generate` used to swallow a requested key generating zero files as a
    // silent no-op. Check the files landed, not just that the process didn't error.
    assert!(
        has_file_with_prefix(&output.0, "000_Produce_v"),
        "expected at least one 000_Produce_v*.bin fixture in {}",
        output.0.display()
    );
    assert!(
        has_file_with_prefix(&output.0, "001_Fetch_v"),
        "expected at least one 001_Fetch_v*.bin fixture in {}",
        output.0.display()
    );
}
