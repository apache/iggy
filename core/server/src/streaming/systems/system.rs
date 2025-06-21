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

use crate::archiver::{ArchiverKind, ArchiverKindType};
use crate::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
use crate::configs::system::SystemConfig;
use crate::map_toggle_str;
use crate::state::StateKind;
use crate::state::file::FileState;
use crate::state::system::SystemState;
use crate::streaming::clients::client_manager::ClientManager;
use crate::streaming::diagnostics::metrics::Metrics;
use crate::streaming::persistence::persister::*;
use crate::streaming::session::Session;
use crate::streaming::storage::SystemStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::COMPONENT;
use crate::streaming::users::permissioner::Permissioner;
use crate::streaming::users::user::User;
use crate::versioning::SemanticVersion;
use ahash::AHashMap;
use error_set::ErrContext;
use iggy_common::locking::IggySharedMut;
use iggy_common::locking::IggySharedMutFn;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError, UserId};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{create_dir_all, remove_dir_all};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::Instant;
use tracing::{error, info, instrument, trace};

#[derive(Debug)]
pub struct SharedSystem {
    system: Arc<RwLock<System>>,
}

impl SharedSystem {
    pub fn new(system: System) -> SharedSystem {
        SharedSystem {
            system: Arc::new(RwLock::new(system)),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<System> {
        self.system.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<System> {
        self.system.write().await
    }
}

impl Clone for SharedSystem {
    fn clone(&self) -> Self {
        SharedSystem {
            system: self.system.clone(),
        }
    }
}

#[derive(Debug)]
pub struct System {
    pub permissioner: Permissioner,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) streams: AHashMap<u32, Stream>,
    pub(crate) streams_ids: AHashMap<String, u32>,
    pub(crate) users: AHashMap<UserId, User>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) client_manager: IggySharedMut<ClientManager>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
    pub(crate) metrics: Metrics,
    pub(crate) state: Arc<StateKind>,
    pub(crate) archiver: Option<Arc<ArchiverKind>>,
    pub personal_access_token: PersonalAccessTokenConfig,
}

impl System {
    pub fn new(
        config: Arc<SystemConfig>,
        data_maintenance_config: DataMaintenanceConfig,
        pat_config: PersonalAccessTokenConfig,
    ) -> System {
        let version = SemanticVersion::current().expect("Invalid version");
        info!(
            "Server-side encryption is {}.",
            map_toggle_str(config.encryption.enabled)
        );

        let encryptor: Option<EncryptorKind> = match config.encryption.enabled {
            true => Some(EncryptorKind::Aes256Gcm(
                Aes256GcmEncryptor::from_base64_key(&config.encryption.key).unwrap(),
            )),
            false => None,
        };

        let state_persister = Self::resolve_persister(config.state.enforce_fsync);
        let partition_persister = Self::resolve_persister(config.partition.enforce_fsync);

        let state = Arc::new(StateKind::File(FileState::new(
            &config.get_state_messages_file_path(),
            &version,
            state_persister,
            encryptor,
        )));

        //TODO: Just shut the fuck up rust-analyzer.
        let encryptor: Option<EncryptorKind> = match config.encryption.enabled {
            true => Some(EncryptorKind::Aes256Gcm(
                Aes256GcmEncryptor::from_base64_key(&config.encryption.key).unwrap(),
            )),
            false => None,
        };
        Self::create(
            config.clone(),
            SystemStorage::new(config, partition_persister),
            state,
            encryptor.map(Arc::new),
            data_maintenance_config,
            pat_config,
        )
    }

    fn resolve_persister(enforce_fsync: bool) -> Arc<PersisterKind> {
        match enforce_fsync {
            true => Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister)),
            false => Arc::new(PersisterKind::File(FilePersister)),
        }
    }

    pub fn create(
        system_config: Arc<SystemConfig>,
        storage: SystemStorage,
        state: Arc<StateKind>,
        encryptor: Option<Arc<EncryptorKind>>,
        data_maintenance_config: DataMaintenanceConfig,
        pat_config: PersonalAccessTokenConfig,
    ) -> System {
        let archiver_config = data_maintenance_config.archiver;
        let archiver: Option<Arc<ArchiverKind>> = if archiver_config.enabled {
            info!("Archiving is enabled, kind: {}", archiver_config.kind);
            match archiver_config.kind {
                ArchiverKindType::Disk => Some(Arc::new(ArchiverKind::get_disk_archiver(
                    archiver_config
                        .disk
                        .clone()
                        .expect("Disk archiver config is missing"),
                ))),
                ArchiverKindType::S3 => Some(Arc::new(
                    ArchiverKind::get_s3_archiver(
                        archiver_config
                            .s3
                            .clone()
                            .expect("S3 archiver config is missing"),
                    )
                    .expect("Failed to create S3 archiver"),
                )),
            }
        } else {
            info!("Archiving is disabled.");
            None
        };

        System {
            config: system_config,
            streams: AHashMap::new(),
            streams_ids: AHashMap::new(),
            storage: Arc::new(storage),
            encryptor,
            client_manager: IggySharedMut::new(ClientManager::default()),
            permissioner: Permissioner::default(),
            metrics: Metrics::init(),
            users: AHashMap::new(),
            state,
            personal_access_token: pat_config,
            archiver,
        }
    }

    #[instrument(skip_all, name = "trace_system_init")]
    pub async fn init(&mut self) -> Result<(), IggyError> {
        let system_path = self.config.get_system_path();
        if !Path::new(&system_path).exists() && create_dir_all(&system_path).await.is_err() {
            return Err(IggyError::CannotCreateBaseDirectory(system_path));
        }

        let state_path = self.config.get_state_path();
        if !Path::new(&state_path).exists() && create_dir_all(&state_path).await.is_err() {
            return Err(IggyError::CannotCreateStateDirectory(state_path));
        }

        let streams_path = self.config.get_streams_path();
        if !Path::new(&streams_path).exists() && create_dir_all(&streams_path).await.is_err() {
            return Err(IggyError::CannotCreateStreamsDirectory(streams_path));
        }

        let runtime_path = self.config.get_runtime_path();
        if Path::new(&runtime_path).exists() && remove_dir_all(&runtime_path).await.is_err() {
            return Err(IggyError::CannotRemoveRuntimeDirectory(runtime_path));
        }

        if create_dir_all(&runtime_path).await.is_err() {
            return Err(IggyError::CannotCreateRuntimeDirectory(runtime_path));
        }

        info!(
            "Initializing system, data will be stored at: {}",
            self.config.get_system_path()
        );

        let state_entries = self.state.init().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to initialize state entries")
        })?;
        let system_state = SystemState::init(state_entries)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to initialize system state")
            })?;
        let now = Instant::now();
        //DONE
        /*
        self.load_version().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to load version")
        })?;
        */
        self.load_users(system_state.users.into_values().collect())
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load users")
            })?;
        self.load_streams(system_state.streams.into_values().collect())
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load streams")
            })?;
        if let Some(archiver) = self.archiver.as_ref() {
            archiver
                .init()
                .await
                .expect("Failed to initialize archiver");
        }
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    #[instrument(skip_all, name = "trace_shutdown")]
    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        self.persist_messages().await?;
        Ok(())
    }

    #[instrument(skip_all, name = "trace_persist_messages")]
    pub async fn persist_messages(&self) -> Result<usize, IggyError> {
        trace!("Saving buffered messages on disk...");
        let mut saved_messages_number = 0;
        for stream in self.streams.values() {
            saved_messages_number += stream.persist_messages().await?;
        }

        Ok(saved_messages_number)
    }

    pub fn ensure_authenticated(&self, session: &Session) -> Result<(), IggyError> {
        if !session.is_active() {
            error!("{COMPONENT} - session is inactive, session: {session}");
            return Err(IggyError::StaleClient);
        }

        if session.is_authenticated() {
            Ok(())
        } else {
            error!("{COMPONENT} - unauthenticated access attempt, session: {session}");
            Err(IggyError::Unauthenticated)
        }
    }
}
