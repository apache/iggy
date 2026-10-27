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

use iggy::prelude::*;
use iggy_common::{
    ServerKeyContext, StreamKeyOrigin, WRAPPED_STREAM_KEY_LENGTH, WrappedStreamKey, text,
};
use integration::iggy_harness;
use std::fs;
use std::path::{Path, PathBuf};

const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const USERNAME: &str = "plaintext-regression-user";
const PLAINTEXT_PASSWORD: &str = "plaintext-password-regression-2943";
const PAT_NAME: &str = "plaintext-regression-pat";

#[iggy_harness]
async fn should_not_persist_plaintext_password_or_pat_to_disk(harness: &mut TestHarness) {
    let root_client = harness.root_client().await.unwrap();
    root_client
        .create_user(
            USERNAME,
            PLAINTEXT_PASSWORD,
            UserStatus::Active,
            None,
            &UserCreateOptions::default(),
        )
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

#[iggy_harness(server(
    encryption.enabled = "true",
    encryption.key = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE="
))]
async fn given_stream_keys_when_scanning_state_should_find_only_wrapped_keys(
    harness: &mut TestHarness,
) {
    let client_key = StreamEncryptionKey::try_from([77u8; 32].as_slice()).unwrap();
    let refused_key = StreamEncryptionKey::try_from([88u8; 32].as_slice()).unwrap();
    let client = harness.root_client().await.unwrap();
    for (name, key) in [
        ("client-key-stream", Some(client_key.clone())),
        ("keyless-stream", None),
    ] {
        client
            .create_stream(
                name,
                &StreamCreateOptions {
                    encryption_key: key,
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .unwrap();
    }
    // A failed create carries a key too: it must not be kept either.
    let duplicate = client
        .create_stream(
            "client-key-stream",
            &StreamCreateOptions {
                encryption_key: Some(refused_key.clone()),
                ..StreamCreateOptions::default()
            },
        )
        .await;
    assert!(duplicate.is_err());
    let data_path = harness.server().data_path();
    drop(client);
    harness.stop().await.unwrap();

    let context = ServerKeyContext::from_config(CONFIG_KEY, &[], true).unwrap();
    let kek = hex_bytes(&context.kek_fingerprint().unwrap());
    let mut stored_keys = Vec::new();
    for origin in [StreamKeyOrigin::Client, StreamKeyOrigin::Server] {
        let mut prefix = vec![1u8, origin as u8];
        prefix.extend_from_slice(&kek);
        let envelope = collect_files(&data_path)
            .iter()
            .find_map(|path| {
                let contents = fs::read(path).ok()?;
                let start = contents
                    .windows(prefix.len())
                    .position(|window| window == prefix.as_slice())?;
                contents
                    .get(start..start + WRAPPED_STREAM_KEY_LENGTH)
                    .map(<[u8]>::to_vec)
            })
            .unwrap_or_else(|| panic!("a stored {origin:?} envelope"));
        let wrapped = WrappedStreamKey::try_from(envelope.as_slice()).unwrap();
        stored_keys.push(context.unwrap(&wrapped).expect("the envelope opens"));
    }
    assert_eq!(stored_keys[0], client_key);
    for (index, key) in stored_keys.iter().chain([&refused_key]).enumerate() {
        let name = format!("stream key {index}");
        assert_bytes_not_persisted(&data_path, key.as_bytes(), &name);
        assert_secret_not_persisted(&data_path, &text::as_base64(key.as_bytes()), &name);
    }
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

fn assert_secret_not_persisted(root: &Path, secret: &str, secret_name: &str) {
    assert_bytes_not_persisted(root, secret.as_bytes(), secret_name);
}

fn assert_bytes_not_persisted(root: &Path, secret: &[u8], secret_name: &str) {
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
