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
use std::sync::atomic::AtomicBool;

use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use error_set::ErrContext;
use figlet_rs::FIGfont;
use iggy_common::create_user::CreateUser;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError};
use server::archiver::{ArchiverKind, ArchiverKindType};
use server::args::Args;
use server::bootstrap::{
    create_directories, create_root_user, create_shard_connections, create_shard_executor,
    load_config, resolve_persister, update_system_info,
};
use server::configs::config_provider::{self};
use server::configs::server::ServerConfig;
use server::configs::sharding::CpuAllocation;
use server::io::fs_utils;
#[cfg(not(feature = "tokio-console"))]
use server::log::logger::Logging;
#[cfg(feature = "tokio-console")]
use server::log::tokio_console::Logging;
use server::server_error::{ConfigError, ServerError};
use server::shard::IggyShard;
use server::shard::system::info::SystemInfo;
use server::state::StateKind;
use server::state::command::EntryCommand;
use server::state::file::FileState;
use server::state::models::CreateUserWithId;
use server::state::system::SystemState;
use server::streaming::storage::SystemStorage;
use server::streaming::utils::MemoryPool;
use server::versioning::SemanticVersion;
use server::{IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV, map_toggle_str, shard_info};
use tokio::time::Instant;
use tracing::{error, info, instrument};

const COMPONENT: &str = "MAIN";

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
    // NINTH DISCRETE LOADING STEP.
    let archiver_config = &config.data_maintenance.archiver;
    let archiver: Option<ArchiverKind> = if archiver_config.enabled {
        info!("Archiving is enabled, kind: {}", archiver_config.kind);
        match archiver_config.kind {
            ArchiverKindType::Disk => Some(ArchiverKind::get_disk_archiver(
                archiver_config
                    .disk
                    .clone()
                    .expect("Disk archiver config is missing"),
            )),
            ArchiverKindType::S3 => Some(
                ArchiverKind::get_s3_archiver(
                    archiver_config
                        .s3
                        .clone()
                        .expect("S3 archiver config is missing"),
                )
                .expect("Failed to create S3 archiver"),
            ),
        }
    } else {
        info!("Archiving is disabled.");
        None
    };

    // TENTH DISCRETE LOADING STEP.
    let state_persister = resolve_persister(config.system.state.enforce_fsync);
    let state = StateKind::File(FileState::new(
        &config.system.get_state_messages_file_path(),
        &current_version,
        state_persister,
        encryptor.clone(),
    ));
    let init_state = SystemState::load(state).await?;

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

    // TWELFTH DISCRETE LOADING STEP.
    info!("Starting {} shard(s)", shards_set.len());
    let (connections, shutdown_handles) = create_shard_connections(&shards_set);
    let mut handles = Vec::with_capacity(shards_set.len());

    for shard_id in shards_set {
        let id = shard_id as u16;
        let storage = storage.clone();
        let connections = connections.clone();
        let config = config.clone();
        let encryptor = encryptor.clone();
        let archiver = archiver.clone();
        let init_state = init_state.clone();
        let state_persister = resolve_persister(config.system.state.enforce_fsync);
        let state = StateKind::File(FileState::new(
            &config.system.get_state_messages_file_path(),
            &current_version,
            state_persister,
            encryptor.clone(),
        ));

        let handle = std::thread::Builder::new()
            .name(format!("shard-{id}"))
            .spawn(move || {
                let affinity_set = HashSet::from([shard_id]);
                let rt = create_shard_executor(affinity_set);
                rt.block_on(async move {
                    let builder = IggyShard::builder();
                    let shard: Rc<IggyShard> = builder
                        .id(id)
                        .connections(connections)
                        .config(config)
                        .storage(storage)
                        .archiver(archiver)
                        .encryptor(encryptor)
                        .version(current_version)
                        .state(state)
                        .init_state(init_state)
                        .build()
                        .into();

                    if let Err(e) = shard.run().await {
                        error!("Failed to run shard-{id}: {e}");
                    }
                    shard_info!(shard.id, "Run completed");
                })
            })
            .unwrap_or_else(|e| panic!("Failed to spawn thread for shard-{id}: {e}"));
        handles.push(handle);
    }

    let shutdown_handles_for_signal = shutdown_handles.clone();
    /*
        ::set_handler(move || {
            info!("Received shutdown signal (SIGTERM/SIGINT), initiating graceful shutdown...");

            for (shard_id, stop_sender) in &shutdown_handles_for_signal {
                info!("Sending shutdown signal to shard {}", shard_id);
                if let Err(e) = stop_sender.send_blocking(()) {
                    error!(
                        "Failed to send shutdown signal to shard {}: {}",
                        shard_id, e
                    );
                }
            }
        })
        .expect("Error setting Ctrl-C handler");
    */

    info!("Iggy server is running. Press Ctrl+C or send SIGTERM to shutdown.");
    for (idx, handle) in handles.into_iter().enumerate() {
        handle.join().expect("Failed to join shard thread");
    }

    info!("All shards have shut down. Iggy server is exiting.");

    /*
    #[cfg(feature = "disable-mimalloc")]
    tracing::warn!(
        "Using default system allocator because code was build with `disable-mimalloc` feature"
    );
    #[cfg(not(feature = "disable-mimalloc"))]
    info!("Using mimalloc allocator");


    let system = SharedSystem::new(System::new(
        config.system.clone(),
        config.data_maintenance.clone(),
        config.personal_access_token.clone(),
    ));
    */

    // Workaround to ensure that the statistics are initialized before the server
    // loads streams and starts accepting connections. This is necessary to
    // have the correct statistics when the server starts.
    //system.write().await.get_stats().await?;
    //system.write().await.init().await?;

    /*
    let _command_handler = BackgroundServerCommandHandler::new(system.clone(), &config)
        .install_handler(SaveMessagesExecutor)
        .install_handler(MaintainMessagesExecutor)
        .install_handler(ArchiveStateExecutor)
        .install_handler(CleanPersonalAccessTokensExecutor)
        .install_handler(SysInfoPrintExecutor)
        .install_handler(VerifyHeartbeatsExecutor);

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::interrupt())?,
            signal(SignalKind::terminate())?,
        )
    };

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
