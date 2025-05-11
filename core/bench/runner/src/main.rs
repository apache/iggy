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

mod app;
mod args;

use anyhow::Result;
use app::IggyBenchRunnerApp;
use args::IggyBenchRunnerArgs;
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, format::Format},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = IggyBenchRunnerArgs::parse();

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::registry()
        .with(fmt::layer().event_format(Format::default().with_thread_ids(true)))
        .with(env_filter)
        .try_init()
        .unwrap();

    info!("Starting IggyBenchRunner with args: {:?}", args);

    if let Err(e) = args.validate() {
        error!("Configuration error: {}", e);
        std::process::exit(1);
    }

    info!("Output directory: {}", args.output_dir);
    info!("Log level: {}", args.log_level);

    let app = IggyBenchRunnerApp::new(args)?;
    let res = app.run().await;
    info!("Benchmark run result: {:?}", res);
    res
}
