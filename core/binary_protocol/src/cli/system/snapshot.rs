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

use std::path::Path;

use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::Client;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::get_snapshot::GetSnapshot;
use iggy_common::{SnapshotCompression, SystemSnapshotType};
use tokio::io::AsyncWriteExt;
use tracing::{event, Level};

pub struct GetSnapshotCmd {
    _get_snapshot: GetSnapshot,
    out_dir: String,
}

impl GetSnapshotCmd {
    pub fn new(
        compression: Option<SnapshotCompression>,
        snapshot_types: Option<Vec<SystemSnapshotType>>,
        out_dir: Option<String>,
    ) -> Self {
        let mut cmd = GetSnapshotCmd::default();

        if let Some(compress) = compression {
            cmd._get_snapshot.compression = compress;
        }
        if let Some(types) = snapshot_types {
            cmd._get_snapshot.snapshot_types = types
        }
        if let Some(out) = out_dir {
            cmd.out_dir = out
        }

        cmd
    }
}

impl Default for GetSnapshotCmd {
    fn default() -> Self {
        Self {
            _get_snapshot: GetSnapshot::default(),
            out_dir: ".".to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for GetSnapshotCmd {
    fn explain(&self) -> String {
        "snapshot command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let snapshot_data = client
            .snapshot(
                self._get_snapshot.compression,
                self._get_snapshot.snapshot_types.to_owned(),
            )
            .await
            .with_context(|| "Problem sending snapshot command".to_owned())?;
        let file_path = Path::new(&self.out_dir).join(format!(
            "snapshot_{}.zip",
            chrono::Local::now().format("%Y%m%d_%H%M%S")
        ));
        let file_size = snapshot_data.0.len();

        let mut file = tokio::fs::File::create(&file_path)
            .await
            .with_context(|| format!("Failed to create file at {:?}", file_path))?;

        file.write_all(&snapshot_data.0)
            .await
            .with_context(|| "Failed to write snapshot data to file".to_owned())?;

        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["File Path", file_path.to_string_lossy().as_ref()]);
        table.add_row(vec!["File Size (bytes)", &file_size.to_string()]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
