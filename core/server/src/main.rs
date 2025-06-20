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

use std::thread::available_parallelism;

use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use error_set::ErrContext;
use figlet_rs::FIGfont;
use server::args::Args;
use server::bootstrap::{
    create_default_executor, create_directories, create_root_user, create_shard_connections,
    create_shard_executor, load_config,
};
use server::channels::commands::archive_state::ArchiveStateExecutor;
use server::channels::commands::clean_personal_access_tokens::CleanPersonalAccessTokensExecutor;
use server::channels::commands::maintain_messages::MaintainMessagesExecutor;
use server::channels::commands::print_sysinfo::SysInfoPrintExecutor;
use server::channels::commands::save_messages::SaveMessagesExecutor;
use server::channels::commands::verify_heartbeats::VerifyHeartbeatsExecutor;
use server::channels::handler::BackgroundServerCommandHandler;
use server::configs::config_provider::{self, ConfigProviderKind};
use server::configs::server::ServerConfig;
use server::http::http_server;
#[cfg(not(feature = "tokio-console"))]
use server::log::logger::Logging;
#[cfg(feature = "tokio-console")]
use server::log::tokio_console::Logging;
use server::quic::quic_server;
use server::server_error::ServerError;
use server::shard::IggyShard;
use server::streaming::systems::system::{SharedSystem, System};
use server::streaming::utils::MemoryPool;
use server::tcp::tcp_server;
use tokio::time::Instant;
use tracing::{info, instrument};

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
    let config = std::thread::scope(|scope| {
        let config = scope
            .spawn(move || {
                let mut rt = create_default_executor::<monoio::IoUringDriver>();
                rt.block_on(load_config(&config_provider))
            })
            .join()
            .expect("Failed to load config");
        config
    })?;

    // Create directories and root user.
    // Remove `local_data` directory if run with `--fresh` flag.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut rt = create_default_executor::<monoio::IoUringDriver>();
                rt.block_on(async {
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
                    Ok::<(), ServerError>(())
                })
            })
            .join()
            .expect("Failed to create directories and root user")
    })
    .with_error_context(|err| format!("Failed to create directories, err: {err}"))?;

    // Initialize logging
    let mut logging = Logging::new(config.telemetry.clone());
    logging.early_init();

    // TODO: Make this configurable from config as a range
    // for example this instance of Iggy will use cores from 0..4
    let available_cpus = available_parallelism().expect("Failed to get num of cores");
    let shards_count = available_cpus.into();
    let shards_set = 0..shards_count;
    let connections = create_shard_connections(shards_set.clone());
    for shard_id in shards_set {
        let id = shard_id as u16;
        let connections = connections.clone();
        let server_config = config.clone();
        std::thread::Builder::new()
            .name(format!("shard-{id}"))
            .spawn(move || {
                monoio::utils::bind_to_cpu_set(Some(shard_id))
                    .expect(format!("Failed to set CPU affinity for shard-{id}").as_str());

                let mut rt = create_shard_executor();
                rt.block_on(async move {
                    let builder = IggyShard::builder();
                    let mut shard = builder
                        .id(id)
                        .connections(connections)
                        .server_config(server_config)
                        .build()
                        .await;

                    if let Err(e) = shard.init().await {
                        //TODO: If one of the shards fails to initialize, we should crash the whole program;
                        panic!("Failed to initialize shard-{id}: {e}");
                    }
                    //TODO: If one of the shards fails to initialize, we should crash the whole program;
                    shard.assert_init();
                })
            })
            .expect(format!("Failed to spawn thread for shard-{id}").as_str())
            .join()
            .expect(format!("Failed to join thread for shard-{id}").as_str());
    }

    // From this point on, we can use tracing macros to log messages.
    logging.late_init(config.system.get_system_path(), &config.system.logging)?;

    /*
    #[cfg(feature = "disable-mimalloc")]
    tracing::warn!(
        "Using default system allocator because code was build with `disable-mimalloc` feature"
    );
    #[cfg(not(feature = "disable-mimalloc"))]
    info!("Using mimalloc allocator");

    MemoryPool::init_pool(config.system.clone());

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
