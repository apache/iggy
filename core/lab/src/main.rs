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

mod args;
mod client;
mod error;
mod eventually;
mod invariants;
mod ops;
mod replay;
mod report;
mod runner;
mod safe_name;
mod scenarios;
mod shadow;
mod trace;
mod worker;

use args::{Command, IggyLabArgs};
use clap::Parser;
use error::LabError;
use replay::LabReplay;
use runner::LabRunner;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<(), LabError> {
    let args = IggyLabArgs::parse();

    match args.command {
        Command::List => {
            println!("Available scenarios:\n");
            for (name, description) in scenarios::list_scenarios() {
                let first_line = description.lines().next().unwrap_or(description);
                println!("  {name:<25} {first_line}");
            }
        }
        Command::Explain { scenario } => {
            let s = scenarios::create_scenario(scenario);
            println!("{}\n", s.name());
            println!("{}", s.describe());
        }
        Command::Run(run_args) => {
            init_tracing();

            let runner = LabRunner::new(run_args);
            let outcome = runner.run().await?;

            if !outcome.passed {
                eprintln!("Run ID: {}", outcome.run_id);
                std::process::exit(1);
            }
        }
        Command::Replay(replay_args) => {
            init_tracing();

            let outcome = LabReplay::new(replay_args).run().await?;

            println!(
                "Replayed {} ops — {} divergences ({} concerning)",
                outcome.total_ops, outcome.divergences_total, outcome.divergences_concerning
            );

            if outcome.divergences_concerning > 0 {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_ansi(true))
        .init();
}
