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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::{
    Client, StreamCreateOptions, StreamDetails, StreamEncryptionKey, StreamEncryptionSource,
};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{Level, event};

pub struct CreateStreamCmd {
    name: String,
    stream_key_file: Option<PathBuf>,
}

impl CreateStreamCmd {
    pub fn new(name: String, stream_key_file: Option<PathBuf>) -> Self {
        Self {
            name,
            stream_key_file,
        }
    }

    fn get_stream_id_info(&self) -> String {
        "ID auto incremented".to_string()
    }

    /// Only a file named on this call supplies the key, so no key spreads across streams.
    fn options(&self) -> anyhow::Result<StreamCreateOptions> {
        let encryption_key = self
            .stream_key_file
            .as_ref()
            .map(|path| {
                let text = fs::read_to_string(path).with_context(|| {
                    format!(
                        "Problem reading stream encryption key file: {}",
                        path.display()
                    )
                })?;
                StreamEncryptionKey::from_str(text.trim_end())
                    .context("Stream encryption key must be standard base64 of exactly 32 bytes")
            })
            .transpose()?;
        Ok(StreamCreateOptions {
            encryption_key,
            ..StreamCreateOptions::default()
        })
    }
}

#[async_trait]
impl CliCommand for CreateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "create stream with name: {} and {}",
            self.name,
            self.get_stream_id_info(),
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let options = self.options()?;
        let stream = client
            .create_stream(&self.name, &options)
            .await
            .with_context(|| {
                format!(
                    "Problem creating stream (name: {} and {})",
                    self.name,
                    self.get_stream_id_info(),
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with name: {} and {} created",
            self.name,
            self.get_stream_id_info(),
        );
        print_encryption(&stream);

        Ok(())
    }
}

fn print_encryption(stream: &StreamDetails) {
    let Some(source) = stream
        .encryption_key_source()
        .filter(|source| *source != StreamEncryptionSource::None)
    else {
        return;
    };
    event!(target: PRINT_TARGET, Level::INFO,
        "Stream encryption key source: {source}, fingerprint: {}, KEK fingerprints: {}",
        stream.encryption_key_fingerprint().unwrap_or_default(),
        stream.encryption_kek_fingerprints().join(", "),
    );
}
