// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![allow(clippy::future_not_send)]

mod args;

use args::Args;
use clap::Parser;
use server_ng::bootstrap::{bootstrap, load_config};
use server_ng::server_error::ServerNgError;
use system_stats::capture_allowed_cpus;
use tracing::{error, info};

fn main() -> Result<(), ServerNgError> {
    // Before shard threads pin themselves: a pinned capture sees one core.
    capture_allowed_cpus();

    let bootstrap_runtime = match server_common::create_shard_executor() {
        Ok(rt) => rt,
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::InvalidInput => {
                    server_common::diagnostics::print_invalid_io_uring_args_info();
                }
                std::io::ErrorKind::OutOfMemory => {
                    server_common::diagnostics::print_locked_memory_limit_info();
                }
                std::io::ErrorKind::PermissionDenied => {
                    server_common::diagnostics::print_io_uring_permission_info();
                }
                _ => {}
            }
            panic!("Cannot create server-ng bootstrap executor: {e}");
        }
    };

    // Bootstrap on a temporary runtime: parse args, init logging, load
    // config, init the memory pool. Then drop the runtime and spawn the
    // per-shard runtimes - each shard thread builds its OWN
    // `compio::runtime::Runtime` via `create_shard_executor`, pinned to
    // its CPU.
    let bootstrap_result: Result<
        (
            configs::server_ng::ServerNgConfig,
            Option<u8>,
            server_common::log::Logging,
        ),
        ServerNgError,
    > = bootstrap_runtime.block_on(async {
        let args = Args::parse();
        if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }

        let mut logging = server_common::log::Logging::new(server_ng::VERSION);
        logging.early_init();
        server_common::print_build_info!(server_ng::VERSION);

        let config = load_config(&mut logging).await?;
        server_common::MemoryPool::init_pool(&config.system.memory_pool.into_other());

        Ok((config, args.replica_id, logging))
    });
    // `_logging` owns the tracing appender worker guards; it must outlive the
    // shard threads or every log line after bootstrap is silently dropped.
    let (config, replica_id, _logging) = bootstrap_result?;
    drop(bootstrap_runtime);

    let shards = bootstrap(config, replica_id)?;
    if let Err(error) = shards.install_ctrlc_handler() {
        // Without a working SIGINT handler the server has no way to
        // observe an operator Ctrl-C and the shutdown flag would never
        // flip, leaving shard threads parked indefinitely. Fail fast
        // rather than boot into an un-killable state.
        error!(error = %error, "failed to install Ctrl-C handler; aborting boot");
        std::process::exit(1);
    }

    info!("server-ng running; waiting on shard threads");
    shards.join_all()?;
    info!("server-ng shutdown complete");
    Ok(())
}
