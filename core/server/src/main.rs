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

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use dotenvy::dotenv;
use err_trail::ErrContext;
use figlet_rs::FIGfont;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError};
use server::args::Args;
use server::bootstrap::{
    create_directories, create_shard_connections, create_shard_executor, load_config, load_streams,
    load_users, resolve_persister, update_system_info,
};
use server::configs::config_provider::{self};
use server::configs::sharding::CpuAllocation;
use server::diagnostics::{print_io_uring_permission_info, print_locked_memory_limit_info};
use server::io::fs_utils;
use server::log::logger::Logging;
use server::server_error::ServerError;
use server::shard::namespace::IggyNamespace;
use server::shard::system::info::SystemInfo;
use server::shard::transmission::id::ShardId;
use server::shard::{IggyShard, calculate_shard_assignment};
use server::slab::traits_ext::{
    EntityComponentSystem, EntityComponentSystemMutCell, IntoComponents,
};
use server::state::file::FileState;
use server::state::system::SystemState;
use server::streaming::clients::client_manager::{Client, ClientManager};
use server::streaming::diagnostics::metrics::Metrics;
use server::streaming::storage::SystemStorage;
use server::streaming::utils::MemoryPool;
use server::streaming::utils::ptr::EternalPtr;
use server::versioning::SemanticVersion;
use std::collections::HashSet;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "MAIN";
const SHARDS_TABLE_CAPACITY: usize = 16384;

static SHUTDOWN_START_TIME: AtomicU64 = AtomicU64::new(0);

#[instrument(skip_all, name = "trace_start_server")]
fn main() -> Result<(), ServerError> {
    let rt = match compio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::OutOfMemory => print_locked_memory_limit_info(),
                std::io::ErrorKind::PermissionDenied => print_io_uring_permission_info(),
                _ => {}
            }
            panic!("Cannot create runtime: {e}");
        }
    };
    rt.block_on(async move {
        let standard_font = FIGfont::standard().unwrap();
        let figure = standard_font.convert("Iggy Server");
        println!("{}", figure.unwrap());

        if let Some(sha) = option_env!("VERGEN_GIT_SHA") {
            println!("Commit SHA: {sha}");
        }

        if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
            if dotenvy::from_path(&env_path).is_ok() {
                println!("Loaded environment variables from path: {env_path}");
            }
        } else if let Ok(path) = dotenv() {
            println!(
                "Loaded environment variables from .env file at path: {}",
                path.display()
            );
        }
        let args = Args::parse();
        let is_follower = args.follower;

        // FIRST DISCRETE LOADING STEP.
        // Load config and create directories.
        // Remove `local_data` directory if run with `--fresh` flag.
        let config_provider = config_provider::resolve(&args.config_provider)?;
        let config = load_config(&config_provider).await.with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to load config during bootstrap")
        })?;
        if args.fresh {
            let system_path = config.system.get_system_path();
            if compio::fs::metadata(&system_path).await.is_ok() {
                println!(
                    "Removing system path at: {} because `--fresh` flag was set",
                    system_path
                );
                if let Err(e) = fs_utils::remove_dir_all(&system_path).await {
                    eprintln!("Failed to remove system path at {}: {}", system_path, e);
                }
            }
        }

        // SECOND DISCRETE LOADING STEP.
        // Create directories.
        create_directories(&config.system).await?;

        // Initialize logging
        // THIRD DISCRETE LOADING STEP.
        let mut logging = Logging::new(config.telemetry.clone());
        logging.early_init();

        // From this point on, we can use tracing macros to log messages.
        logging.late_init(config.system.get_system_path(), &config.system.logging)?;

        if is_follower {
            info!("Server is running in FOLLOWER mode for testing leader redirection");
        }

        if args.with_default_root_credentials {
            let username_set = std::env::var("IGGY_ROOT_USERNAME").is_ok();
            let password_set = std::env::var("IGGY_ROOT_PASSWORD").is_ok();

            if !username_set || !password_set {
                if !username_set {
                    unsafe {
                        std::env::set_var("IGGY_ROOT_USERNAME", "iggy");
                    }
                }
                if !password_set {
                    unsafe {
                        std::env::set_var("IGGY_ROOT_PASSWORD", "iggy");
                    }
                }
                info!(
                    "Using default root credentials (username: iggy, password: iggy) - FOR DEVELOPMENT ONLY! \
                    If root user already exists, existing credentials will be reused."
                );
            } else {
                warn!(
                    "--with-default-root-credentials flag is ignored because root credentials are already set via environment variables"
                );
            }
        }

        // FOURTH DISCRETE LOADING STEP.
        MemoryPool::init_pool(config.system.clone());

        // SIXTH DISCRETE LOADING STEP.
        let partition_persister = resolve_persister(config.system.partition.enforce_fsync);
        let storage = SystemStorage::new(config.system.clone(), partition_persister);

        // SEVENTH DISCRETE LOADING STEP.
        let current_version = SemanticVersion::current().expect("Invalid version");
        let mut system_info;
        let load_system_info = storage.info.load().await;
        match load_system_info {
            Ok(info) => {
                system_info = info;
            }
            Err(e) => {
                if let IggyError::ResourceNotFound(_) = e {
                    info!("System info not found, creating...");
                    system_info = SystemInfo::default();
                    update_system_info(&storage, &mut system_info, &current_version).await?;
                } else {
                    panic!("Failed to load system info from disk. {e}");
                }
            }
        }
        info!("Loaded {system_info}.");
        let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
        if current_version.is_equal_to(&loaded_version) {
            info!("System version {current_version} is up to date.");
        } else if current_version.is_greater_than(&loaded_version) {
            info!(
                "System version {current_version} is greater than {loaded_version}, checking the available migrations..."
            );
            update_system_info(&storage, &mut system_info, &current_version).await?;
        } else {
            info!(
                "System version {current_version} is lower than {loaded_version}, possible downgrade."
            );
            update_system_info(&storage, &mut system_info, &current_version).await?;
        }

        // EIGHTH DISCRETE LOADING STEP.
        info!(
            "Server-side encryption is {}.",
            match config.system.encryption.enabled {
                true => "enabled",
                false => "disabled",
            }
        );
        let encryptor: Option<EncryptorKind> = match config.system.encryption.enabled {
            true => Some(EncryptorKind::Aes256Gcm(
                Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
            )),
            false => None,
        };

        // TENTH DISCRETE LOADING STEP.
        let state_persister = resolve_persister(config.system.state.enforce_fsync);
        let state_current_index = Arc::new(AtomicU64::new(0));
        let state_entries_count = Arc::new(AtomicU64::new(0));
        let state_current_leader = Arc::new(AtomicU32::new(0));
        let state_term = Arc::new(AtomicU64::new(0));
        let state = FileState::new(
            &config.system.get_state_messages_file_path(),
            &current_version,
            state_persister,
            encryptor.clone(),
            state_current_index.clone(),
            state_entries_count.clone(),
            state_current_leader.clone(),
            state_term.clone(),
        );
        let state = SystemState::load(state).await?;
        let (streams_state, users_state) = state.decompose();
        let streams = load_streams(streams_state.into_values(), &config.system).await?;
        let users = load_users(users_state.into_values());

        // ELEVENTH DISCRETE LOADING STEP.
        let shards_set = config.system.sharding.cpu_allocation.to_shard_set();
        match &config.system.sharding.cpu_allocation {
            CpuAllocation::All => {
                info!(
                    "Using all available CPU cores ({} shards with affinity)",
                    shards_set.len()
                );
            }
            CpuAllocation::Count(count) => {
                info!("Using {count} shards with affinity to cores 0..{count}");
            }
            CpuAllocation::Range(start, end) => {
                info!(
                    "Using {} shards with affinity to cores {start}..{end}",
                    end - start
                );
            }
        }

        #[cfg(feature = "disable-mimalloc")]
        warn!("Using default system allocator because code was build with `disable-mimalloc` feature");
        #[cfg(not(feature = "disable-mimalloc"))]
        info!("Using mimalloc allocator");

        // DISCRETE STEP.
        // Increment the metrics.
        let metrics = Metrics::init();

        // TWELFTH DISCRETE LOADING STEP.
        info!("Starting {} shard(s)", shards_set.len());
        let (connections, shutdown_handles) = create_shard_connections(&shards_set);
        let mut handles = Vec::with_capacity(shards_set.len());

        // TODO: Persist the shards table and load it from the disk, so it does not have to be
        // THIRTEENTH DISCRETE LOADING STEP.
        // Shared resources bootstrap.
        let shards_table = Box::new(DashMap::with_capacity(SHARDS_TABLE_CAPACITY));
        let shards_table = Box::leak(shards_table);
        let shards_table: EternalPtr<DashMap<IggyNamespace, ShardId>> = shards_table.into();

        let client_manager = Box::new(DashMap::new());
        let client_manager = Box::leak(client_manager);
        let client_manager: EternalPtr<DashMap<u32, Client>> = client_manager.into();
        let client_manager = ClientManager::new(client_manager);

        streams.with_components(|components| {
            let (root, ..) = components.into_components();
            for (_, stream) in root.iter() {
                stream.topics().with_components(|components| {
                    let (root, ..) = components.into_components();
                    for (_, topic) in root.iter() {
                        topic.partitions().with_components(|components| {
                            let (root, ..) = components.into_components();
                            for (_, partition) in root.iter() {
                                let stream_id = stream.id();
                                let topic_id = topic.id();
                                let partition_id = partition.id();
                                let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                                let shard_id = ShardId::new(calculate_shard_assignment(
                                    &ns,
                                    shards_set.len() as u32,
                                ));
                                shards_table.insert(ns, shard_id);
                            }
                        });
                    }
                })
            }
        });

        for (id, cpu_id) in shards_set
            .into_iter()
            .enumerate()
            .map(|(idx, cpu)| (idx as u16, cpu))
        {
            let streams = streams.clone();
            let shards_table = shards_table.clone();
            let users = users.clone();
            let connections = connections.clone();
            let config = config.clone();
            let encryptor = encryptor.clone();
            let metrics = metrics.clone();
            let current_version = current_version.clone();
            let state_persister = resolve_persister(config.system.state.enforce_fsync);
            let state = FileState::new(
                &config.system.get_state_messages_file_path(),
                &current_version,
                state_persister,
                encryptor.clone(),
                state_current_index.clone(),
                state_entries_count.clone(),
                state_current_leader.clone(),
                state_term.clone(),
            );
            let client_manager = client_manager.clone();

            // TODO: Explore decoupling the `Log` from `Partition` entity.
            // Ergh... I knew this will backfire to include `Log` as part of the `Partition` entity,
            // We have to initialize with a default log for every partition, once we `Clone` the Streams / Topics / Partitions,
            // because `Clone` impl for `Partition` does not clone the actual log, just creates an empty one.
            streams.with_components(|components| {
                let (root, ..) = components.into_components();
                for (_, stream) in root.iter() {
                    stream.topics().with_components_mut(|components| {
                        let (mut root, ..) = components.into_components();
                        for (_, topic) in root.iter_mut() {
                            let partitions_count = topic.partitions().len();
                            for log_id in 0..partitions_count {
                                let id = topic.partitions_mut().insert_default_log();
                                assert_eq!(
                                    id, log_id,
                                    "main: partition_insert_default_log: id mismatch when creating default log"
                                );
                            }
                        }
                    })
                }
            });

            let handle = std::thread::Builder::new()
                .name(format!("shard-{id}"))
                .spawn(move || {
                    let affinity_set = HashSet::from([cpu_id]);
                    let rt = create_shard_executor(affinity_set);
                    rt.block_on(async move {
                        let builder = IggyShard::builder();
                        let shard = builder
                            .id(id)
                            .streams(streams)
                            .state(state)
                            .users(users)
                            .shards_table(shards_table)
                            .connections(connections)
                            .clients_manager(client_manager)
                            .config(config)
                            .encryptor(encryptor)
                            .version(current_version)
                            .metrics(metrics)
                            .is_follower(is_follower)
                            .build();
                        let shard = Rc::new(shard);

                        if let Err(e) = shard.run().await {
                            error!("Failed to run shard-{id}: {e}");
                        }
                        info!("Run completed");
                    })
                })
                .unwrap_or_else(|e| panic!("Failed to spawn thread for shard-{id}: {e}"));
            handles.push(handle);
        }

        let shutdown_handles_for_signal = shutdown_handles.clone();
        ctrlc::set_handler(move || {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            SHUTDOWN_START_TIME.store(now, Ordering::SeqCst);

            info!("Received shutdown signal (SIGTERM/SIGINT), initiating graceful shutdown...");

            for (shard_id, stop_sender) in &shutdown_handles_for_signal {
                if let Err(e) = stop_sender.try_send(()) {
                    error!(
                        "Failed to send shutdown signal to shard {}: {}",
                        shard_id, e
                    );
                }
            }
        })
        .expect("Error setting Ctrl-C handler");

        info!("Iggy server is running. Press Ctrl+C or send SIGTERM to shutdown.");

        for (idx, handle) in handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|_| panic!("Failed to join shard thread-{}", idx));
        }

        let shutdown_duration_msg = {
            let start_time = SHUTDOWN_START_TIME.load(Ordering::SeqCst);
            if start_time > 0 {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let elapsed = now - start_time;
                format!(" (shutdown took {} ms)", elapsed)
            } else {
                String::new()
            }
        };

        info!(
            "All shards have shut down. Iggy server is exiting.{}",
            shutdown_duration_msg
        );

        Ok(())
    })
}
