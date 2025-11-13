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

use compio::fs::create_dir;
use iggy::prelude::{Aes256GcmEncryptor, EncryptorKind};
use server::state::file::FileState;
use server::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
use server::streaming::utils::file::overwrite;
use server::versioning::SemanticVersion;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use uuid::Uuid;

mod file;
mod system;

pub struct StateSetup {
    directory_path: String,
    state: FileState,
    version: u32,
}

impl StateSetup {
    pub async fn init() -> StateSetup {
        StateSetup::create(None).await
    }

    pub async fn init_with_encryptor() -> StateSetup {
        StateSetup::create(Some(&[1; 32])).await
    }

    pub async fn create(encryption_key: Option<&[u8]>) -> StateSetup {
        let directory_path = format!("state_{}", Uuid::now_v7().to_u128_le());
        let messages_file_path = format!("{directory_path}/log");
        create_dir(&directory_path).await.unwrap();
        overwrite(&messages_file_path).await.unwrap();

        let version = SemanticVersion::from_str("1.2.3").unwrap();
        let persister = PersisterKind::FileWithSync(FileWithSyncPersister {});
        let encryptor = encryption_key
            .map(|key| EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(key).unwrap()));
        let state_current_index = Arc::new(AtomicU64::new(0));
        let state_entries_count = Arc::new(AtomicU64::new(0));
        let state_current_leader = Arc::new(AtomicU32::new(0));
        let state_term = Arc::new(AtomicU64::new(0));
        let state = FileState::new(
            &messages_file_path,
            &version,
            Arc::new(persister),
            encryptor,
            state_current_index,
            state_entries_count,
            state_current_leader,
            state_term,
        );

        Self {
            directory_path,
            state,
            version: version.get_numeric_version().unwrap(),
        }
    }

    pub fn state(&self) -> &FileState {
        &self.state
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl Drop for StateSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory_path).unwrap();
    }
}
