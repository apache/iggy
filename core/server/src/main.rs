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
use configs::server::ServerConfig;
use server::bootstrap::{
    apply_default_root_credentials, bootstrap, load_config, prepare_runtime_dirs,
};
use server::server_error::ServerError;
use server_common::log::Logging;
use system_stats::capture_allowed_cpus;
use tracing::{error, info};

fn main() -> Result<(), ServerError> {
    // This prelude must stay ahead of the first thread the process ever
    // spawns: `--with-default-root-credentials` writes to the environment,
    // and `set_var` is only sound while single-threaded. `early_init` just
    // installs the tracing registry (buffered until `late_init`) and starts
    // no worker of its own, so it can run here and make the warnings below
    // visible. `create_shard_executor` also reads its capacity knob from the
    // environment, which is why the `.env` load has to precede it.
    let args = Args::parse();
    // `logging` owns the tracing appender worker guards; it must outlive the
    // shard threads or every log line after bootstrap is silently dropped.
    let mut logging = Logging::new(server::VERSION);
    logging.early_init();
    server_common::print_build_info!(server::VERSION);
    if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
        let _ = dotenvy::from_path(&env_path);
    } else {
        let _ = dotenvy::dotenv();
    }
    // SAFETY: no thread has been spawned yet, see the comment above.
    unsafe { apply_default_root_credentials(args.with_default_root_credentials) };

    // Before shard threads pin themselves: a pinned capture sees one core.
    capture_allowed_cpus();

    let bootstrap_runtime = match server_common::create_shard_executor() {
        Ok(rt) => rt,
        Err(e) => {
            let e = server_common::diagnostics::enrich_runtime_create_error(e);
            panic!("Cannot create server bootstrap executor: {e}");
        }
    };

    // Bootstrap on a temporary runtime: load config, prepare the data
    // directory, init the memory pool. Then drop the runtime and spawn the
    // per-shard runtimes - each shard thread builds its OWN
    // `compio::runtime::Runtime` via `create_shard_executor`, pinned to
    // its CPU.
    let bootstrap_result: Result<ServerConfig, ServerError> = bootstrap_runtime.block_on(async {
        let config = load_config().await?;
        prepare_runtime_dirs(&config, &mut logging, args.fresh).await?;
        let memory_pool_settings =
            server_common::MemoryPoolSettings::from(&config.system.memory_pool);
        server_common::MemoryPool::init_pool(&memory_pool_settings);

        Ok(config)
    });
    let config = bootstrap_result?;
    drop(bootstrap_runtime);

    let shards = bootstrap(config, args.replica_id)?;
    if let Err(error) = shards.install_ctrlc_handler() {
        // Without a working SIGINT handler the server has no way to
        // observe an operator Ctrl-C and the shutdown flag would never
        // flip, leaving shard threads parked indefinitely. Fail fast
        // rather than boot into an un-killable state.
        error!(error = %error, "failed to install Ctrl-C handler; aborting boot");
        std::process::exit(1);
    }

    info!("server running; waiting on shard threads");
    let joined = shards.join_all();
    #[cfg(feature = "systemd")]
    if let Err(error) = &joined {
        server::systemd::notify_shutdown_failure(error);
    }
    joined?;
    info!("server shutdown complete");
    Ok(())
}
