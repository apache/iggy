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

use compio::fs::OpenOptions;
use err_trail::ErrContext;
use iggy_common::IggyError;
use tracing::trace;

/// Path handle for a segment's index file, validated openable at segment
/// build. Reads go through the partition's own index reader; this exists so
/// storage plumbing (bootstrap, state transfer) can resolve the index path.
#[derive(Debug)]
pub struct IndexReader {
    file_path: String,
}

impl IndexReader {
    /// Opens the index file read-only to prove it exists, then drops the
    /// descriptor: nothing reads through this type.
    pub async fn new(file_path: &str) -> Result<Self, IggyError> {
        OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .error(|e: &std::io::Error| format!("Failed to open index file: {file_path}. {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        trace!("Validated index file for reading: {file_path}");
        Ok(Self {
            file_path: file_path.to_string(),
        })
    }

    pub fn path(&self) -> String {
        self.file_path.clone()
    }
}
