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

mod actors;
mod analytics;
mod args;
mod benchmarks;
mod plot;
mod runner;
mod utils;

use crate::{args::common::IggyBenchArgs, runner::BenchmarkRunner};
use chrono::Utc;
use clap::Parser;
use figlet_rs::FIGlet;
use iggy::prelude::IggyError;
use std::path::{PathBuf, is_separator};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use utils::{cpu_name::append_cpu_name_lowercase, create_run_dir};

/// Which SDK framing this binary speaks, printed in the always-on banner so a
/// server that answers a Register handshake with silence is diagnosed in a
/// second rather than mistaken for a hang.
const SDK_FRAMING: &str = "vsr (Register handshake)";

const RUN_START_TIME_FORMAT: &str = "_%Y%m%d-%H%M%S";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let standard_font = FIGlet::standard().unwrap();
    let figure = standard_font.convert("Iggy Bench");
    println!("{}", figure.unwrap());
    println!("SDK framing: {SDK_FRAMING}");

    let mut args = IggyBenchArgs::parse();
    args.validate();

    let benchmark_dir = args
        .output_dir()
        .map(|output_dir| {
            let output_dir = PathBuf::from(output_dir);
            let dir_name = run_dir_name(&args);
            create_run_dir(&output_dir, &dir_name).map_err(|error| {
                IggyError::CannotCreateBaseDirectory(format!(
                    "{}: {error}",
                    output_dir.join(&dir_name).display()
                ))
            })
        })
        .transpose()?;

    // Configure logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));
    let stdout_layer = fmt::layer().with_ansi(true);

    // If output directory is specified, also log to file
    if let Some(ref benchmark_dir) = benchmark_dir {
        let file_appender = tracing_appender::rolling::never(benchmark_dir, "bench.log");
        let file_layer = fmt::layer().with_ansi(false).with_writer(file_appender);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .with(file_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .init();
    }

    let benchmark_runner = BenchmarkRunner::new(args);

    info!("Starting the benchmarks...");
    let ctrl_c = tokio::signal::ctrl_c();
    let benchmark_future = benchmark_runner.run(benchmark_dir.as_deref());

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl-C, exiting...");
            // Clean up unfinished benchmark directory on manual interruption
            if let Some(ref benchmark_dir) = benchmark_dir {
                info!("Cleaning up unfinished benchmark directory...");
                if let Err(e) = std::fs::remove_dir_all(benchmark_dir) {
                    error!("Failed to clean up benchmark directory: {}", e);
                }
            }
        }
        result = benchmark_future => {
            if let Err(e) = result {
                error!("Benchmark failed with error: {:?}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// The UTC start time tells repeated runs of one command apart. A label such as a branch
/// `--gitref` can hold a path separator, which would nest the run below the level that the
/// dashboard reads, so each separator becomes `_`.
fn run_dir_name(args: &IggyBenchArgs) -> String {
    let mut dir_name = args.generate_dir_name();
    append_cpu_name_lowercase(&mut dir_name);
    dir_name.push_str(&Utc::now().format(RUN_START_TIME_FORMAT).to_string());
    dir_name.replace(is_separator, "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_labels_with_path_separators_when_naming_run_dir_should_keep_one_path_component() {
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "-b",
            "10",
            "pinned-producer",
            "tcp",
            "output",
            "--remark",
            "a/b",
            "--gitref",
            "feat/x",
            "--identifier",
            "rack/host",
        ])
        .expect("the arguments parse");

        let dir_name = run_dir_name(&args);

        assert!(!dir_name.contains(is_separator), "{dir_name}");
        assert!(dir_name.contains("_a_b_feat_x_rack_host_"), "{dir_name}");
    }
}
