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
mod poll_artifacts;
mod runner;
mod utils;

use std::fs;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use figlet_rs::FIGlet;
use iggy::prelude::IggyError;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{args::common::IggyBenchArgs, runner::BenchmarkRunner};
use poll_artifacts::{PollArtifacts, RunStatus};
use utils::cpu_name::append_cpu_name_lowercase;

/// Which SDK framing this binary speaks, printed in the always-on banner so a
/// server that answers a Register handshake with silence is diagnosed in a
/// second rather than mistaken for a hang.
const SDK_FRAMING: &str = "vsr (Register handshake)";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let standard_font = FIGlet::standard().unwrap();
    let figure = standard_font.convert("Iggy Bench");
    println!("{}", figure.unwrap());
    println!("SDK framing: {SDK_FRAMING}");

    let mut args = IggyBenchArgs::parse();
    args.validate();

    // Store output_dir before moving args
    let output_dir = args.output_dir();
    let benchmark_dir = output_dir.as_ref().map(|dir| {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
        }
        let mut dir_name = args.generate_dir_name();
        append_cpu_name_lowercase(&mut dir_name);
        dir_path.join(dir_name)
    });

    // Configure logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));
    let stdout_layer = fmt::layer().with_ansi(true);

    // If output directory is specified, also log to file
    if let Some(ref benchmark_dir) = benchmark_dir {
        // Create output directory if it doesn't exist
        fs::create_dir_all(benchmark_dir).unwrap();
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

    let poll_artifacts = benchmark_dir
        .as_ref()
        .map(|_| Arc::new(PollArtifacts::new(&args)));
    args.poll_artifacts = poll_artifacts.clone();
    if let (Some(directory), Some(artifacts)) = (&benchmark_dir, &poll_artifacts) {
        artifacts
            .write(directory, RunStatus::Running)
            .map_err(|error| {
                error!("Failed to initialize poll artifacts: {error}");
                IggyError::CannotWriteToFile
            })?;
    }
    let benchmark_runner = BenchmarkRunner::new(args);
    info!("Starting the benchmarks...");
    let result = benchmark_runner.run().await;
    let status = match &result {
        Ok(status) => *status,
        Err(error) => {
            error!("Benchmark failed with error: {error:?}");
            RunStatus::Failed
        }
    };
    if let (Some(directory), Some(artifacts)) = (&benchmark_dir, &poll_artifacts) {
        artifacts.write(directory, status).map_err(|error| {
            error!("Failed to write poll artifacts: {error}");
            IggyError::CannotWriteToFile
        })?;
        info!("Preserved benchmark artifacts in {}", directory.display());
    }
    result.map(|_| ())
}
