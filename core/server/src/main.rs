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

use std::collections::HashSet;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use dotenvy::dotenv;
use error_set::ErrContext;
use figlet_rs::FIGfont;
use iggy_common::create_user::CreateUser;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, Identifier, IggyError};
use lending_iterator::lending_iterator::constructors::into_lending_iter;
use server::args::Args;
use server::binary::handlers::streams;
use server::bootstrap::{
    create_directories, create_root_user, create_shard_connections, create_shard_executor,
    load_config, load_streams, load_users, resolve_persister, update_system_info,
};
use server::configs::config_provider::{self};
use server::configs::server::ServerConfig;
use server::configs::sharding::CpuAllocation;
use server::io::fs_utils;
use server::log::logger::Logging;
use server::server_error::{ConfigError, ServerError};
use server::shard::namespace::IggyNamespace;
use server::shard::system::info::SystemInfo;
use server::shard::{IggyShard, ShardInfo, calculate_shard_assignment};
use server::slab::traits_ext::{
    EntityComponentSystem, EntityComponentSystemMutCell, IntoComponents,
};
use server::state::StateKind;
use server::state::command::EntryCommand;
use server::state::file::FileState;
use server::state::models::CreateUserWithId;
use server::state::system::SystemState;
use server::streaming::diagnostics::metrics::Metrics;
use server::streaming::storage::SystemStorage;
use server::streaming::utils::MemoryPool;
use server::streaming::utils::ptr::EternalPtr;
use server::versioning::SemanticVersion;
use server::{IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV, map_toggle_str, shard_info};
use tokio::time::Instant;
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "MAIN";
const SHARDS_TABLE_CAPACITY: usize = 16384;

static SHUTDOWN_START_TIME: AtomicU64 = AtomicU64::new(0);

#[instrument(skip_all, name = "trace_start_server")]
#[compio::main]
async fn main() -> Result<(), ServerError> {
    let startup_timestamp = Instant::now();
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
    // FIRST DISCRETE LOADING STEP.
    // TODO: I think we could get rid of config provider, since we support only TOML
    // as config provider.
    let config_provider = config_provider::resolve(&args.config_provider)?;
    // Load config and create directories.
    // Remove `local_data` directory if run with `--fresh` flag.
    let config = load_config(&config_provider)
        .await
        .with_error_context(|error| {
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
                "Using default root credentials (username: iggy, password: iggy) - FOR DEVELOPMENT ONLY!"
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
        map_toggle_str(config.system.encryption.enabled)
    );
    let encryptor: Option<EncryptorKind> = match config.system.encryption.enabled {
        true => Some(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
        )),
        false => None,
    };

    // TENTH DISCRETE LOADING STEP.
    let state_persister = resolve_persister(config.system.state.enforce_fsync);
    let state = StateKind::File(FileState::new(
        &config.system.get_state_messages_file_path(),
        &current_version,
        state_persister,
        encryptor.clone(),
    ));
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
    let shards_table: EternalPtr<DashMap<IggyNamespace, ShardInfo>> = shards_table.into();
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
                            let shard_id = calculate_shard_assignment(&ns, shards_set.len() as u32);
                            let shard_info = ShardInfo::new(shard_id);
                            shards_table.insert(ns, shard_info);
                        }
                    });
                }
            })
        }
    });

    for (id, cpu_id) in shards_set
        .into_iter()
        .enumerate()
        .map(|(id, shard_id)| (id as u16, shard_id))
    {
        let streams = streams.clone();
        let shards_table = shards_table.clone();
        let users = users.clone();
        let persister = storage.persister.clone();
        let connections = connections.clone();
        let config = config.clone();
        let encryptor = encryptor.clone();
        let metrics = metrics.clone();
        let state_persister = resolve_persister(config.system.state.enforce_fsync);
        let state = StateKind::File(FileState::new(
            &config.system.get_state_messages_file_path(),
            &current_version,
            state_persister,
            encryptor.clone(),
        ));

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
                        .config(config)
                        .encryptor(encryptor)
                        .version(current_version)
                        .metrics(metrics)
                        .build();
                    let shard = Rc::new(shard);

                    if let Err(e) = shard.run(persister).await {
                        error!("Failed to run shard-{id}: {e}");
                    }
                    shard_info!(shard.id, "Run completed");
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
        handle.join().expect("Failed to join shard thread");
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

    /*
    #[cfg(feature = "disable-mimalloc")]
    warn!("Using default system allocator because code was build with `disable-mimalloc` feature");
    #[cfg(not(feature = "disable-mimalloc"))]
    info!("Using mimalloc allocator");

    let system = SharedSystem::new(System::new(
        config.system.clone(),
        config.cluster.clone(),
        config.cluster.clone(),
        config.data_maintenance.clone(),
        config.personal_access_token.clone(),
    ));
    */

    /*

    let mut current_config = config.clone();

    if config.http.enabled {
        let http_addr = http_server::start(config.http, system.clone()).await;
        current_config.http.address = http_addr.to_string();
    }

    if config.quic.enabled {
        let quic_addr = quic_server::start(config.quic, system.clone());
        current_config.quic.address = quic_addr.to_string();
    }

    if config.tcp.enabled {
        let tcp_addr = tcp_server::start(config.tcp, system.clone()).await;
        current_config.tcp.address = tcp_addr.to_string();
    }

    let runtime_path = current_config.system.get_runtime_path();
    let current_config_path = format!("{runtime_path}/current_config.toml");
    let current_config_content =
        toml::to_string(&current_config).expect("Cannot serialize current_config");
    tokio::fs::write(current_config_path, current_config_content).await?;

    let elapsed_time = startup_timestamp.elapsed();
    info!(
        "Iggy server has started - overall startup took {} ms.",
        elapsed_time.as_millis()
    );

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c.recv() => {
            info!("Received SIGINT. Shutting down Iggy server...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down Iggy server...");
        }
    }

    #[cfg(windows)]
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Received CTRL-C. Shutting down Iggy server...");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }

    let shutdown_timestamp = Instant::now();
    let mut system = system.write().await;
    system.shutdown().await?;
    let elapsed_time = shutdown_timestamp.elapsed();

    info!(
        "Iggy server has shutdown successfully. Shutdown took {} ms.",
        elapsed_time.as_millis()
    );
    */
    Ok(())
}
