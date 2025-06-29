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

use std::rc::Rc;
use std::sync::Arc;
use std::thread::available_parallelism;

use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use error_set::ErrContext;
use figlet_rs::FIGfont;
use iggy_common::create_user::CreateUser;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError};
use server::args::Args;
use server::bootstrap::{
    create_default_executor, create_directories, create_root_user, create_shard_connections,
    create_shard_executor, load_config, resolve_persister,
};
use server::configs::config_provider::{self};
use server::configs::server::ServerConfig;
#[cfg(not(feature = "tokio-console"))]
use server::log::logger::Logging;
#[cfg(feature = "tokio-console")]
use server::log::tokio_console::Logging;
use server::server_error::ServerError;
use server::shard::IggyShard;
use server::shard::gate::Gate;
use server::state::StateKind;
use server::state::command::EntryCommand;
use server::state::file::FileState;
use server::state::models::CreateUserWithId;
use server::state::system::SystemState;
use server::streaming::utils::{MemoryPool, crypto};
use server::versioning::SemanticVersion;
use server::{IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV, map_toggle_str};
use tokio::time::Instant;
use tracing::{error, info, instrument};

const COMPONENT: &str = "MAIN";

#[instrument(skip_all, name = "trace_start_server")]
fn main() -> Result<(), ServerError> {
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
    // TODO: I think we could get rid of config provider, since we support only TOML
    // as config provider.
    let config_provider = config_provider::resolve(&args.config_provider)?;
    // Load config and create directories.
    // Remove `local_data` directory if run with `--fresh` flag.
    let mut rt = create_default_executor::<monoio::IoUringDriver>();
    let config = rt
        .block_on(async {
            let config = load_config(&config_provider)
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to load config during bootstrap")
                })?;
            if args.fresh {
                let system_path = config.system.get_system_path();
                if monoio::fs::metadata(&system_path).await.is_ok() {
                    println!(
                        "Removing system path at: {} because `--fresh` flag was set",
                        system_path
                    );
                    //TODO: Impl dir walk and remove the files
                    /*
                    if let Err(e) = tokio::fs::remove_dir_all(&system_path).await {
                        eprintln!("Failed to remove system path at {}: {}", system_path, e);
                    }
                    */
                }
            }

            // Create directories.
            create_directories(&config.system).await?;
            Ok::<ServerConfig, ServerError>(config)
        })
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to load config")
        })?;

    // Initialize logging
    let mut logging = Logging::new(config.telemetry.clone());
    logging.early_init();

    // From this point on, we can use tracing macros to log messages.
    logging.late_init(config.system.get_system_path(), &config.system.logging)?;

    // TODO: Make this configurable from config as a range
    // for example this instance of Iggy will use cores from 0..4
    let available_cpus = available_parallelism().expect("Failed to get num of cores");
    let shards_count = available_cpus.into();
    let shards_set = 0..shards_count;
    let connections = create_shard_connections(shards_set.clone());
    let gate = Arc::new(Gate::new());
    let mut handles = Vec::with_capacity(shards_set.len());
    for shard_id in shards_set {
        let id = shard_id as u16;
        let gate = gate.clone();
        let connections = connections.clone();
        let config = config.clone();
        let state_persister = resolve_persister(config.system.state.enforce_fsync);
        let handle = std::thread::Builder::new()
            .name(format!("shard-{id}"))
            .spawn(move || {
                MemoryPool::init_pool(config.system.clone());
                monoio::utils::bind_to_cpu_set(Some(shard_id))
                    .expect(format!("Failed to set CPU affinity for shard-{id}").as_str());

                let mut rt = create_shard_executor();
                rt.block_on(async move {
                    let version = SemanticVersion::current().expect("Invalid version");
                    info!(
                        "Server-side encryption is {}.",
                        map_toggle_str(config.system.encryption.enabled)
                    );
                    let encryptor: Option<EncryptorKind> = match config.system.encryption.enabled {
                        true => Some(EncryptorKind::Aes256Gcm(
                            Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key)
                                .unwrap(),
                        )),
                        false => None,
                    };

                    let state = StateKind::File(FileState::new(
                        &config.system.get_state_messages_file_path(),
                        &version,
                        state_persister,
                        encryptor.clone(),
                    ));

                    // We can't use std::sync::Once because it doesn't support async.
                    // Trait bound on the closure is FnOnce.
                    // Peak into the state to check if the root user exists.
                    // If it does not exist, create it.
                    gate.with_async::<Result<(), IggyError>>(async |gate_state| {
                        // A thread already initialized state
                        // Thus, we can skip it.
                        if let Some(_) = gate_state.inner() {
                            return Ok(());
                        }

                        let state_entries = state.load_entries().await.with_error_context(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to load state entries"
                            )
                        })?;
                        let root_exists = state_entries
                            .into_iter()
                            .find(|entry| {
                                entry
                                    .command()
                                    .and_then(|command| match command {
                                        EntryCommand::CreateUser(payload) if payload.user_id == DEFAULT_ROOT_USER_ID =>
                                        {
                                            Ok(true)
                                        }
                                        _ => Ok(false),
                                    })
                                    .map_or_else(
                                        |err| {
                                            error!("Failed to check if root user exists: {err}");
                                            false
                                        },
                                        |v| v,
                                    )
                            })
                            .is_some();

                        if !root_exists {
                            info!("No users found, creating the root user...");
                            let root = create_root_user();
                            let command = CreateUser {
                                username: root.username.clone(),
                                password: root.password.clone(),
                                status: root.status,
                                permissions: root.permissions.clone(),
                            };
                            state
                                .apply(0, &EntryCommand::CreateUser(CreateUserWithId {
                                    user_id: root.id,
                                    command
                                }))
                                .await
                                .with_error_context(|error| {
                                    format!(
                                        "{COMPONENT} (error: {error}) - failed to apply create user command, username: {}",
                                        root.username
                                    )
                                })?;
                        }

                        gate_state.set_result(());
                        Ok(())
                    })
                    .await;

                    let builder = IggyShard::builder();
                    let shard: Rc<IggyShard> = builder
                        .id(id)
                        .connections(connections)
                        .config(config)
                        .encryptor(encryptor)
                        .version(version)
                        .state(state)
                        .build()
                        .into();

                    //TODO: If one of the shards fails to initialize, we should crash the whole program;
                    if let Err(e) = shard.run().await {
                        error!("Failed to run shard-{id}: {e}");
                    } 
                    //TODO: If one of the shards fails to initialize, we should crash the whole program;
                    //shard.assert_init();
                })
            })
            .expect(format!("Failed to spawn thread for shard-{id}").as_str());
        handles.push(handle);
    }

    handles.into_iter().for_each(|handle| {
        handle.join().expect("Failed to join shard thread");
    });

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
