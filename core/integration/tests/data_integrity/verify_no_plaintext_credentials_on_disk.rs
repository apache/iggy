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

use iggy::prelude::*;
use integration::iggy_harness;
use std::fs;
use std::path::{Path, PathBuf};

const USERNAME: &str = "plaintext-regression-user";
const PLAINTEXT_PASSWORD: &str = "plaintext-password-regression-2943";
const PAT_NAME: &str = "plaintext-regression-pat";

#[iggy_harness]
async fn should_not_persist_plaintext_password_or_pat_to_disk(harness: &mut TestHarness) {
    let root_client = harness.root_client().await.unwrap();
    root_client
        .create_user(USERNAME, PLAINTEXT_PASSWORD, UserStatus::Active, None)
        .await
        .unwrap();

    let raw_pat = root_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .unwrap();
    let raw_pat_token = &raw_pat.token;

    assert!(!raw_pat_token.is_empty(), "Expected non-empty PAT value");

    let data_path = harness.server().data_path();

    drop(root_client);
    harness.stop().await.unwrap();

    assert_secret_not_persisted(&data_path, PLAINTEXT_PASSWORD, "plaintext password");
    assert_secret_not_persisted(&data_path, raw_pat_token, "raw PAT");
}

fn assert_secret_not_persisted(root: &Path, secret: &str, secret_name: &str) {
    let secret = secret.as_bytes();
    for path in collect_files(root) {
        let contents = fs::read(&path).unwrap_or_else(|e| {
            panic!("Failed to read persisted file {}: {e}", path.display());
        });
        assert!(
            !contains_subslice(&contents, secret),
            "Found {secret_name} persisted in file {}",
            path.display()
        );
    }
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files_recursive(root, &mut files);
    files
}

fn collect_files_recursive(path: &Path, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(path).unwrap_or_else(|e| {
        panic!("Failed to read persisted directory {}: {e}", path.display());
    });

    for entry in entries {
        let entry = entry.unwrap_or_else(|e| {
            panic!(
                "Failed to read entry in persisted directory {}: {e}",
                path.display()
            );
        });
        let entry_path = entry.path();
        let file_type = entry.file_type().unwrap_or_else(|e| {
            panic!("Failed to get file type for {}: {e}", entry_path.display());
        });

        if file_type.is_dir() {
            collect_files_recursive(&entry_path, files);
        } else if file_type.is_file() {
            files.push(entry_path);
        }
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }

    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
