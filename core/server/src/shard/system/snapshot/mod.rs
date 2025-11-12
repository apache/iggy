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

mod procdump;

use crate::configs::system::SystemConfig;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt;
use iggy_common::{IggyDuration, IggyError, Snapshot, SnapshotCompression, SystemSnapshotType};
use std::path::PathBuf;
use std::time::Instant;
use tempfile::NamedTempFile;
use tracing::{error, info};

// NOTE(hubcio): compio has a `process` module, but it currently blocks the executor when the runtime
// has thread_pool_limit(0) configured (which we do on non-macOS platforms in bootstrap.rs).
// To use compio::process::Command, we need to either:
// 1. Enable thread pool by removing/increasing thread_pool_limit(0)
// 2. Use std::process::Command with compio::runtime::spawn_blocking (requires thread pool)
// 3. Find alternative approach that doesn't rely on thread pool
// See: https://compio.rs/docs/compio/process and bootstrap::create_shard_executor
use std::process::Command;

impl IggyShard {
    pub async fn get_snapshot(
        &self,
        session: &Session,
        compression: SnapshotCompression,
        snapshot_types: &Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.ensure_authenticated(session)?;

        let snapshot_types = if snapshot_types.contains(&SystemSnapshotType::All) {
            if snapshot_types.len() > 1 {
                error!("When using 'All' snapshot type, no other types can be specified");
                return Err(IggyError::InvalidCommand);
            }
            &SystemSnapshotType::all_snapshot_types()
        } else {
            snapshot_types
        };

        let mut zip_writer = ZipFileWriter::new(Vec::new());
        let compression = match compression {
            SnapshotCompression::Stored => Compression::Stored,
            SnapshotCompression::Deflated => Compression::Deflate,
            SnapshotCompression::Bzip2 => Compression::Bz,
            SnapshotCompression::Lzma => Compression::Lzma,
            SnapshotCompression::Xz => Compression::Xz,
            SnapshotCompression::Zstd => Compression::Zstd,
        };

        info!("Executing snapshot commands: {:?}", snapshot_types);
        let now = Instant::now();

        for snapshot_type in snapshot_types {
            info!("Processing snapshot type: {:?}", snapshot_type);
            match get_command_result(snapshot_type, &self.config.system).await {
                Ok(temp_file) => {
                    info!(
                        "Got temp file for {:?}: {}",
                        snapshot_type,
                        temp_file.path().display()
                    );
                    let filename = format!("{snapshot_type}.txt");
                    let entry = ZipEntryBuilder::new(filename.clone().into(), compression);

                    // Read file using compio fs
                    let content = match compio::fs::read(temp_file.path()).await {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to read temporary file: {}", e);
                            continue;
                        }
                    };

                    info!(
                        "Read {} bytes from temp file for {}",
                        content.len(),
                        filename
                    );

                    if let Err(e) = zip_writer.write_entry_whole(entry, &content).await {
                        error!("Failed to write to snapshot file: {}", e);
                        continue;
                    }
                    info!("Wrote entry {} to zip file", filename);
                }
                Err(e) => {
                    error!(
                        "Failed to execute command for snapshot type {:?}: {}",
                        snapshot_type, e
                    );
                    continue;
                }
            }
        }

        info!(
            "Snapshot commands {:?} finished in {}",
            snapshot_types,
            IggyDuration::new(now.elapsed())
        );

        let zip_data = zip_writer
            .close()
            .await
            .map_err(|_| IggyError::SnapshotFileCompletionFailed)?;

        info!("Final zip size: {} bytes", zip_data.len());
        Ok(Snapshot::new(zip_data))
    }
}

async fn write_command_output_to_temp_file(
    command: &mut Command,
) -> Result<NamedTempFile, std::io::Error> {
    let output = command.output()?;

    info!(
        "Command output: {} bytes, stderr: {}",
        output.stdout.len(),
        String::from_utf8_lossy(&output.stderr)
    );

    let temp_file = NamedTempFile::new()?;

    // Use compio to write the file - create/truncate to ensure clean write
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(temp_file.path())
        .await?;

    // Write the command output - compio takes ownership of the buffer
    let stdout = output.stdout;
    let (result, _buf) = file.write_all_at(stdout, 0).await.into();
    result?;

    file.sync_all().await?;

    info!(
        "Wrote {} bytes to temp file: {}",
        _buf.len(),
        temp_file.path().display()
    );

    Ok(temp_file)
}

async fn get_filesystem_overview() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("ls").args(["-la", "/tmp", "/proc"])).await
}

async fn get_process_info() -> Result<NamedTempFile, std::io::Error> {
    let temp_file = NamedTempFile::new()?;
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(temp_file.path())
        .await?;

    let mut position = 0;
    let ps_output = Command::new("ps").arg("aux").output()?;
    let (result, written) = file
        .write_all_at(b"=== Process List (ps aux) ===\n", 0)
        .await
        .into();
    result?;
    position += written.len() as u64;

    let (result, written) = file.write_all_at(ps_output.stdout, position).await.into();
    result?;
    position += written.len() as u64;

    let (result, written) = file.write_all_at(b"\n\n", position).await.into();
    result?;
    position += written.len() as u64;

    let (result, written) = file
        .write_all_at(b"=== Detailed Process Information ===\n", position)
        .await
        .into();
    result?;
    position += written.len() as u64;

    let proc_info = procdump::get_proc_info().await?;
    let bytes = proc_info.as_bytes().to_owned();
    let (result, _) = file.write_all_at(bytes, position).await.into();
    result?;
    file.sync_all().await?;

    Ok(temp_file)
}

async fn get_resource_usage() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("top").args(["-H", "-b", "-n", "1"])).await
}

async fn get_test_snapshot() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("echo").arg("test")).await
}

async fn get_server_logs(config: &SystemConfig) -> Result<NamedTempFile, std::io::Error> {
    let base_directory = PathBuf::from(config.get_system_path());
    let logs_subdirectory = PathBuf::from(&config.logging.path);
    let logs_path = base_directory.join(logs_subdirectory);

    let list_and_cat = format!(
        r#"ls -tr "{logs}" | xargs -I {{}} cat "{logs}/{{}}" "#,
        logs = logs_path.display()
    );

    write_command_output_to_temp_file(Command::new("sh").args(["-c", &list_and_cat])).await
}

async fn get_server_config(config: &SystemConfig) -> Result<NamedTempFile, std::io::Error> {
    let base_directory = PathBuf::from(config.get_system_path());
    let config_path = base_directory.join("runtime").join("current_config.toml");

    write_command_output_to_temp_file(Command::new("cat").arg(config_path)).await
}

async fn get_command_result(
    snapshot_type: &SystemSnapshotType,
    config: &SystemConfig,
) -> Result<NamedTempFile, std::io::Error> {
    match snapshot_type {
        SystemSnapshotType::FilesystemOverview => get_filesystem_overview().await,
        SystemSnapshotType::ProcessList => get_process_info().await,
        SystemSnapshotType::ResourceUsage => get_resource_usage().await,
        SystemSnapshotType::Test => get_test_snapshot().await,
        SystemSnapshotType::ServerLogs => get_server_logs(config).await,
        SystemSnapshotType::ServerConfig => get_server_config(config).await,
        SystemSnapshotType::All => {
            // This should not be reached (we filter out `All`` at the call site)
            unreachable!(
                "SystemSnapshotType::All should be handled before calling get_command_result()."
            )
        }
    }
}
