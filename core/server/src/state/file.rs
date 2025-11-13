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

use crate::state::command::EntryCommand;
use crate::state::{COMPONENT, StateEntry};
use crate::streaming::persistence::persister::PersisterKind;
use crate::streaming::utils::file;
use crate::versioning::SemanticVersion;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use compio::io::AsyncReadExt;
use err_trail::ErrContext;
use iggy_common::BytesSerializable;
use iggy_common::EncryptorKind;
use iggy_common::IggyByteSize;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tracing::{debug, error, info};

pub const BUF_CURSOR_CAPACITY_BYTES: usize = 512 * 1000;
const FILE_STATE_PARSE_ERROR: &str = "STATE - failed to parse file state";

#[derive(Debug)]
pub struct FileState {
    current_index: Arc<AtomicU64>,
    entries_count: Arc<AtomicU64>,
    current_leader: Arc<AtomicU32>,
    term: Arc<AtomicU64>,
    version: u32,
    path: String,
    persister: Arc<PersisterKind>,
    encryptor: Option<EncryptorKind>,
}

impl FileState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: &str,
        version: &SemanticVersion,
        persister: Arc<PersisterKind>,
        encryptor: Option<EncryptorKind>,
        current_index: Arc<AtomicU64>,
        entries_count: Arc<AtomicU64>,
        current_leader: Arc<AtomicU32>,
        term: Arc<AtomicU64>,
    ) -> Self {
        Self {
            current_index,
            entries_count,
            current_leader,
            term,
            path: path.into(),
            persister,
            encryptor,
            version: version.get_numeric_version().expect("Invalid version"),
        }
    }

    pub fn current_index(&self) -> u64 {
        self.current_index.load(Ordering::SeqCst)
    }

    pub fn entries_count(&self) -> u64 {
        self.entries_count.load(Ordering::SeqCst)
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    pub async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        assert!(Path::new(&self.path).exists());

        let entries = self
            .load_entries()
            .await
            .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to load entries"))?;
        let entries_count = entries.len() as u64;
        self.entries_count.store(entries_count, Ordering::SeqCst);
        if entries_count == 0 {
            self.current_index.store(0, Ordering::SeqCst);
        } else {
            let last_index = entries[entries_count as usize - 1].index;
            self.current_index.store(last_index, Ordering::SeqCst);
        }

        Ok(entries)
    }

    pub async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            return Err(IggyError::StateFileNotFound);
        }

        let file = file::open(&self.path)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to open state file, path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let file_size = file
            .metadata()
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to load state file metadata, path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        if file_size == 0 {
            info!("State file is empty");
            return Ok(Vec::new());
        }

        info!(
            "Loading state, file size: {}",
            IggyByteSize::from(file_size).as_human_string()
        );
        let mut entries = Vec::new();
        let mut total_size: u64 = 0;
        let mut cursor = std::io::Cursor::new(file);
        let mut current_index = 0;
        let mut entries_count = 0;
        loop {
            let index = cursor
                .read_u64_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} index. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 8;
            // Greater than one, because one of the entries after a fresh reboot is the default root user.
            if entries_count > 1 && index != current_index + 1 {
                error!(
                    "State file is corrupted, expected index: {}, got: {}",
                    current_index + 1,
                    index
                );
                return Err(IggyError::StateFileCorrupted);
            }

            current_index = index;
            entries_count += 1;
            let term = cursor
                .read_u64_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} term. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 8;
            let leader_id = cursor
                .read_u32_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} leader_id. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 4;
            let version = cursor
                .read_u32_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} version. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 4;
            let flags = cursor
                .read_u64_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} flags. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 8;
            let timestamp = IggyTimestamp::from(
                cursor
                    .read_u64_le()
                    .await
                    .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} timestamp. {error}"))
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            total_size += 8;
            let user_id = cursor
                .read_u32_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} user_id. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 4;
            let checksum = cursor
                .read_u64_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} checksum. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 8;
            let context_length = cursor
                .read_u32_le()
                .await
                .with_error(|error| {
                    format!("{FILE_STATE_PARSE_ERROR} context context_length. {error}")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?
                as usize;
            total_size += 4;
            let mut context = BytesMut::with_capacity(context_length);
            context.put_bytes(0, context_length);
            let (result, context) = cursor.read_exact(context).await.into();

            result
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} code. {error}"))
                .map_err(|_| IggyError::CannotReadFile)?;
            let context = context.freeze();
            total_size += context_length as u64;
            let code = cursor
                .read_u32_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} code. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?;
            total_size += 4;
            let mut command_length = cursor
                .read_u32_le()
                .await
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} command_length. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?
                as usize;
            total_size += 4;
            let mut command = BytesMut::with_capacity(command_length);
            command.put_bytes(0, command_length);
            let (result, command) = cursor.read_exact(command).await.into();
            result
                .with_error(|error| format!("{FILE_STATE_PARSE_ERROR} command. {error}"))
                .map_err(|_| IggyError::CannotReadFile)?;
            total_size += command_length as u64;
            let command_payload;
            if let Some(encryptor) = &self.encryptor {
                debug!("Decrypting state entry with index: {index}");
                command_payload = Bytes::from(encryptor.decrypt(&command.freeze())?);
                command_length = command_payload.len();
            } else {
                command_payload = command.freeze();
            }

            let mut entry_command = BytesMut::with_capacity(4 + 4 + command_length);
            entry_command.put_u32_le(code);
            entry_command.put_u32_le(command_length as u32);
            entry_command.extend(command_payload);
            let command = entry_command.freeze();
            EntryCommand::from_bytes(command.clone()).with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to parse entry command from bytes")
            })?;
            let calculated_checksum = StateEntry::calculate_checksum(
                index, term, leader_id, version, flags, timestamp, user_id, &context, &command,
            );
            let entry = StateEntry::new(
                index,
                term,
                leader_id,
                version,
                flags,
                timestamp,
                user_id,
                calculated_checksum,
                context,
                command,
            );
            debug!("Read state entry: {entry}");
            if entry.checksum != checksum {
                return Err(IggyError::InvalidStateEntryChecksum(
                    entry.checksum,
                    checksum,
                    entry.index,
                ));
            }

            entries.push(entry);
            if total_size == file_size {
                break;
            }
        }

        info!("Loaded {entries_count} state entries, current index: {current_index}");
        Ok(entries)
    }

    pub async fn apply(&self, user_id: u32, command: &EntryCommand) -> Result<(), IggyError> {
        debug!("Applying state entry with command: {command}, user ID: {user_id}");
        let timestamp = IggyTimestamp::now();
        let index = if self.entries_count.load(Ordering::SeqCst) == 0 {
            0
        } else {
            self.current_index.fetch_add(1, Ordering::SeqCst) + 1
        };
        let term = self.term.load(Ordering::SeqCst);
        let current_leader = self.current_leader.load(Ordering::SeqCst);
        let version = self.version;
        let flags = 0;
        let context = Bytes::new();
        let mut command = command.to_bytes();
        let checksum = StateEntry::calculate_checksum(
            index,
            term,
            current_leader,
            version,
            flags,
            timestamp,
            user_id,
            &context,
            &command,
        );

        if let Some(encryptor) = &self.encryptor {
            debug!("Encrypting state entry command with index: {index}");
            let command_code = command.slice(0..4).get_u32_le();
            let mut command_length = command.slice(4..8).get_u32_le() as usize;
            let command_payload = command.slice(8..8 + command_length);
            let encrypted_command_payload = encryptor
                .encrypt(&command_payload)
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to encrypt state entry command, index: {index}"
                    )
                })?;
            command_length = encrypted_command_payload.len();
            let mut command_bytes = BytesMut::with_capacity(4 + 4 + command_length);
            command_bytes.put_u32_le(command_code);
            command_bytes.put_u32_le(command_length as u32);
            command_bytes.extend(encrypted_command_payload);
            command = command_bytes.freeze();
        }

        let entry = StateEntry::new(
            index,
            term,
            current_leader,
            version,
            flags,
            timestamp,
            user_id,
            checksum,
            context,
            command,
        );
        let bytes = entry.to_bytes();
        let len = bytes.len();
        self.entries_count.fetch_add(1, Ordering::SeqCst);
        self.persister
            .append(&self.path, bytes)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to append state entry data to file, path: {}, data size: {}",
                    self.path,
                    len
                )
            })?;
        debug!("Applied state entry: {entry}");
        Ok(())
    }
}
