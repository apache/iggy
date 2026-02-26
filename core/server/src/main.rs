/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
use iggy_common::SemanticVersion;
use iggy_common::sharding::{IggyNamespace, LocalIdx, PartitionLocation, ShardId};
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError, MemoryPool};
use server::SEMANTIC_VERSION;
use server::args::Args;
use server::bootstrap::{
    create_directories, create_shard_connections, create_shard_executor, load_config,
    load_metadata, resolve_persister, update_system_info,
};
use server::diagnostics::{print_io_uring_permission_info, print_locked_memory_limit_info};
use server::io::fs_utils;
use server::log::logger::Logging;
use server::metadata::{Metadata, create_metadata_handles};
use server::server_error::ServerError;
use server::shard::system::info::SystemInfo;
use server::shard::{IggyShard, calculate_shard_assignment};
use server::shard_allocator::ShardAllocator;
use server::state::file::FileState;
use server::state::system::SystemState;
use server::streaming::clients::client_manager::{Client, ClientManager};
use server::streaming::diagnostics::metrics::Metrics;
use server::streaming::storage::SystemStorage;
use server::streaming::utils::ptr::EternalPtr;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "MAIN";
const SHARDS_TABLE_CAPACITY: usize = 16384;

static SHUTDOWN_START_TIME: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN_INITIATED: AtomicBool = AtomicBool::new(false);

enum ShardExitStatus {
    Success,
    Error(String),
    Panic(String),
}

fn initiate_shutdown(
    reason: &str,
    shutdown_handles: &[(u16, server::shard::transmission::connector::StopSender)],
) {
    if SHUTDOWN_INITIATED.swap(true, Ordering::SeqCst) {
        return;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    SHUTDOWN_START_TIME.store(now, Ordering::SeqCst);

    info!("{reason}, initiating graceful shutdown...");

    for (shard_id, stop_sender) in shutdown_handles {
        if let Err(e) = stop_sender.try_send(()) {
            error!("Failed to send shutdown signal to shard {shard_id}: {e}");
        }
    }
}

fn extract_panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

fn print_ascii_art(text: &str) {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert(text);
    println!("{}", figure.unwrap());
}

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
        print_ascii_art("Iggy Server");

        let is_follower = args.follower;

        // FIRST DISCRETE LOADING STEP.
        // Initialize early logging before config parsing so we can log during bootstrap.
        let mut logging = Logging::new();
        logging.early_init();

        // SECOND DISCRETE LOADING STEP.
        // Load config and create directories.
        // Remove `local_data` directory if run with `--fresh` flag.
        let config = load_config().await.error(|e: &ServerError| {
            format!("{COMPONENT} (error: {e}) - failed to load config during bootstrap")
        })?;
        if args.fresh {
            let system_path = config.system.get_system_path();
            if compio::fs::metadata(&system_path).await.is_ok() {
                warn!(
                    "Removing system path at: {} because `--fresh` flag was set",
                    system_path
                );
                if let Err(e) = fs_utils::remove_dir_all(&system_path).await {
                    warn!("Failed to remove system path at {system_path}: {e}");
                }
            }
        }

        // THIRD DISCRETE LOADING STEP.
        // Create directories.
        create_directories(&config.system).await?;

        // FOURTH DISCRETE LOADING STEP.
        // Complete logging setup with config (file output, telemetry).
        // From this point on, logs are persisted to file and telemetry is active.
        logging.late_init(
            config.system.get_system_path(),
            &config.system.logging,
            &config.telemetry,
        )?;

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

        // FIFTH DISCRETE LOADING STEP.
        MemoryPool::init_pool(&config.system.memory_pool.into_other());

        // SIXTH DISCRETE LOADING STEP.
        let partition_persister = resolve_persister(config.system.partition.enforce_fsync);
        let storage = SystemStorage::new(config.system.clone(), partition_persister);

        // SEVENTH DISCRETE LOADING STEP.
        let current_version = SEMANTIC_VERSION;
        info!("Current semantic version: {:?}", current_version);

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

        // Create left-right handles for metadata
        let (mut metadata_writer, metadata_reader) = create_metadata_handles();

        // Create shared metadata reader - each shard will get a clone
        let metadata = Metadata::new(metadata_reader);

        // Load initial metadata using the writer
        load_metadata(
            users_state.into_values(),
            streams_state.into_values(),
            &mut metadata_writer,
        );

        // ELEVENTH DISCRETE LOADING STEP.
        let shard_allocator = ShardAllocator::new(&config.system.sharding.cpu_allocation)?;
        let shard_assignment = shard_allocator.to_shard_assignments()?;

        #[cfg(feature = "disable-mimalloc")]
        warn!("Using default system allocator because code was build with `disable-mimalloc` feature");
        #[cfg(not(feature = "disable-mimalloc"))]
        info!("Using mimalloc allocator");

        // DISCRETE STEP.
        // Increment the metrics.
        let metrics = Metrics::init();

        // TWELFTH DISCRETE LOADING STEP.
        info!(
            "Enable TCP socket migration across shards: {}.",
            config.tcp.socket_migration
        );

        info!("Starting {} shard(s)", shard_assignment.len());
        let (connections, shutdown_handles) = create_shard_connections(&shard_assignment);
        let shards_count = shard_assignment.len();
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(shards_count);

        // Channel for shard completion notifications
        let (shard_done_tx, shard_done_rx) = mpsc::channel::<(u16, ShardExitStatus)>();

        // TODO: Persist the shards table and load it from the disk, so it does not have to be
        // THIRTEENTH DISCRETE LOADING STEP.
        // Shared resources bootstrap.
        let shards_table = Box::new(DashMap::with_capacity(SHARDS_TABLE_CAPACITY));
        let shards_table = Box::leak(shards_table);
        let shards_table: EternalPtr<DashMap<IggyNamespace, PartitionLocation>> = shards_table.into();

        let client_manager = Box::new(DashMap::new());
        let client_manager = Box::leak(client_manager);
        let client_manager: EternalPtr<DashMap<u32, Client>> = client_manager.into();
        let client_manager = ClientManager::new(client_manager);

        // Populate shards_table from SharedMetadata partitions (hierarchical traversal)
        metadata.with_metadata(|metadata| {
            for (stream_id, stream_meta) in metadata.streams.iter() {
                for (topic_id, topic_meta) in stream_meta.topics.iter() {
                    for (partition_id, _partition_meta) in topic_meta.partitions.iter().enumerate() {
                        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                        let shard_id = ShardId::new(calculate_shard_assignment(
                            &ns,
                            shard_assignment.len() as u32,
                        ));
                        // TODO(hubcio): LocalIdx is 0 until IggyPartitions is integrated
                        let location = PartitionLocation::new(shard_id, LocalIdx::new(0));
                        shards_table.insert(ns, location);
                    }
                }
            }
        });

        // Wrap metadata_writer in Option so we can take it for shard 0
        let mut metadata_writer_opt = Some(metadata_writer);

        for (id, assignment) in shard_assignment
            .into_iter()
            .enumerate()
            .map(|(idx, assignment)| (idx as u16, assignment))
        {
            let shards_table = shards_table.clone();
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
            let shard_metadata = metadata.clone();

            // Take metadata_writer for shard 0 only
            let shard_metadata_writer = if id == 0 {
                metadata_writer_opt.take()
            } else {
                None
            };

            let shard_done_tx = shard_done_tx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("shard-{id}"))
                .spawn(move || {
                    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        if let Err(e) = assignment.bind_cpu() {
                            error!("Failed to bind cpu: {e:?}");
                        }

                        if let Err(e) = assignment.bind_memory() {
                            error!("Failed to bind memory: {e:?}");
                        }

                        let rt = create_shard_executor();
                        rt.block_on(async move {
                            let mut builder = IggyShard::builder();
                            builder = builder
                                .id(id)
                                .state(state)
                                .shards_table(shards_table)
                                .connections(connections)
                                .clients_manager(client_manager)
                                .config(config)
                                .encryptor(encryptor)
                                .version(current_version)
                                .metrics(metrics)
                                .is_follower(is_follower)
                                .metadata(shard_metadata);

                            if let Some(writer) = shard_metadata_writer {
                                builder = builder.metadata_writer(writer);
                            }

                            let shard = builder.build();

                            let shard = Rc::new(shard);

                            if let Err(e) = shard.run().await {
                                error!("Failed to run shard-{id}: {e}");
                                return Err(e.to_string());
                            }
                            info!("Shard {id} run completed");

                            Ok(())
                        })
                    }));

                    let status = match result {
                        Ok(Ok(())) => ShardExitStatus::Success,
                        Ok(Err(msg)) => ShardExitStatus::Error(msg),
                        Err(panic_payload) => {
                            ShardExitStatus::Panic(extract_panic_message(panic_payload))
                        }
                    };

                    let _ = shard_done_tx.send((id, status));
                })
                .unwrap_or_else(|e| panic!("Failed to spawn thread for shard-{id}: {e}"));
            handles.push(handle);
        }

        drop(shard_done_tx);

        let shutdown_handles_for_signal = shutdown_handles.clone();
        ctrlc::set_handler(move || {
            initiate_shutdown(
                "Received shutdown signal (SIGTERM/SIGINT)",
                &shutdown_handles_for_signal,
            );
        })
        .expect("Error setting Ctrl-C handler");

        info!("Iggy server is running. Press Ctrl+C or send SIGTERM to shutdown.");

        let mut completed_shards = 0usize;
        let mut failure_message: Option<String> = None;

        while completed_shards < shards_count {
            match shard_done_rx.recv() {
                Ok((shard_id, status)) => {
                    completed_shards += 1;

                    match status {
                        ShardExitStatus::Success => {
                            info!("Shard {shard_id} exited successfully");
                        }
                        ShardExitStatus::Error(msg) => {
                            error!("Shard {shard_id} exited with error: {msg}");
                            if failure_message.is_none() {
                                failure_message =
                                    Some(format!("Shard {shard_id} exited with error: {msg}"));
                            }
                            initiate_shutdown(
                                &format!("Shard {shard_id} exited with error"),
                                &shutdown_handles,
                            );
                        }
                        ShardExitStatus::Panic(msg) => {
                            error!("Shard {shard_id} panicked: {msg}");
                            if failure_message.is_none() {
                                failure_message =
                                    Some(format!("Shard {shard_id} panicked: {msg}"));
                            }
                            initiate_shutdown(
                                &format!("Shard {shard_id} panicked"),
                                &shutdown_handles,
                            );
                        }
                    }
                }
                Err(_) => {
                    error!("Shard completion channel closed unexpectedly");
                    break;
                }
            }
        }

        for (idx, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                warn!("Shard {idx} thread join returned panic: {e:?}");
            }
        }

        let shutdown_duration_msg = {
            let start_time = SHUTDOWN_START_TIME.load(Ordering::SeqCst);
            if start_time > 0 {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let elapsed = now.saturating_sub(start_time);
                format!(" (shutdown took {} ms)", elapsed)
            } else {
                String::new()
            }
        };

        if let Some(msg) = failure_message {
            error!(
                "Server shutting down due to shard failure.{}",
                shutdown_duration_msg
            );
            return Err(ServerError::ShardFailure { message: msg });
        }

        info!(
            "All shards have shut down. Iggy server is exiting.{}",
            shutdown_duration_msg
        );

        Ok(())
    })
}
