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

#![allow(clippy::future_not_send)]

mod args;

use args::Args;
use clap::Parser;
use server_ng::bootstrap::RunServerNg;
use server_ng::server_error::ServerNgError;

fn main() -> Result<(), ServerNgError> {
    // TODO: decouple runtime creation from the `server` crate and move the shared
    // compio executor setup into a lower-level crate/module used by both binaries.
    let runtime = match server::bootstrap::create_shard_executor() {
        Ok(rt) => rt,
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::InvalidInput => {
                    // TODO: decouple io_uring diagnostics from the `server` crate.
                    server::diagnostics::print_invalid_io_uring_args_info();
                }
                std::io::ErrorKind::OutOfMemory => {
                    // TODO: decouple io_uring diagnostics from the `server` crate.
                    server::diagnostics::print_locked_memory_limit_info();
                }
                std::io::ErrorKind::PermissionDenied => {
                    // TODO: decouple io_uring diagnostics from the `server` crate.
                    server::diagnostics::print_io_uring_permission_info();
                }
                _ => {}
            }
            panic!("Cannot create server-ng executor: {e}");
        }
    };
    runtime.block_on(async {
        let args = Args::parse();
        if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }

        // TODO: decouple logging from the `server` crate and move the shared
        // logging bootstrap into a lower-level crate/module.
        let mut logging = server::log::logger::Logging::new();
        logging.early_init();

        let config = server_ng::bootstrap::load_config(&mut logging).await?;
        iggy_common::MemoryPool::init_pool(&config.system.memory_pool.into_other());

        let shard = server_ng::bootstrap::bootstrap(&config, args.replica_id).await?;
        shard.run(&config, args.replica_id).await
    })
}
