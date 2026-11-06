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

use clap::{Parser, Subcommand};
use iggy_conformance::agent;
use iggy_conformance::agent_host::AgentCommand;
use iggy_conformance::format::{self, Credentials, Transport};
use iggy_conformance::report::{self, Totals};
use iggy_conformance::runner::{RunConfig, RunReport, Runner};
use iggy_conformance::skips::SkipList;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "Runs the cross-SDK conformance corpus of Apache Iggy")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the corpus against an SDK agent.
    Run(RunArgs),
    /// Serves the agent protocol for the Rust SDK on stdin and stdout.
    Agent,
    /// Runs tests with the Rust agent and prints every answer as a JSON line.
    /// Review the output before it becomes an expectation: a recorded value
    /// turns a bug of the Rust SDK into the contract.
    Record(RunArgs),
    /// Loads and checks every test file without a server.
    Check {
        #[arg(long, default_value = "conformance/tests")]
        tests: PathBuf,
    },
    /// Joins JSON reports of several runs into one Markdown matrix.
    Matrix {
        reports: Vec<PathBuf>,
        #[arg(long)]
        markdown: Option<PathBuf>,
    },
}

#[derive(clap::Args)]
struct RunArgs {
    #[arg(long, default_value = "conformance/tests")]
    tests: PathBuf,
    /// The TCP address of the single node.
    #[arg(
        long,
        env = "IGGY_CONFORMANCE_SERVER",
        default_value = "127.0.0.1:8090"
    )]
    server: String,
    /// The TCP address of a node that cannot reach quorum.
    #[arg(long, env = "IGGY_CONFORMANCE_NO_QUORUM")]
    no_quorum: Option<String>,
    /// `rust` for the built-in agent, or a shell command that starts an agent.
    #[arg(long, default_value = "rust")]
    agent: String,
    #[arg(long, value_enum, default_value = "tcp")]
    transport: TransportArg,
    #[arg(long, env = "IGGY_ROOT_USERNAME", default_value = "iggy")]
    username: String,
    #[arg(long, env = "IGGY_ROOT_PASSWORD", default_value = "iggy")]
    password: String,
    /// Runs only the tests whose id contains this text.
    #[arg(long)]
    filter: Option<String>,
    /// The longest wait for one answer that has no time bound.
    #[arg(long, default_value = "90s", value_parser = humantime::parse_duration)]
    call_timeout: Duration,
    /// The longest wait for the agent's hello, which covers its start.
    #[arg(long, default_value = "300s", value_parser = humantime::parse_duration)]
    hello_timeout: Duration,
    #[arg(long)]
    junit: Option<PathBuf>,
    #[arg(long)]
    json: Option<PathBuf>,
    /// Fails the run on a skip that this list does not allow.
    #[arg(long)]
    skips: Option<PathBuf>,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum TransportArg {
    Tcp,
    Http,
    Quic,
    Websocket,
}

impl From<TransportArg> for Transport {
    fn from(transport: TransportArg) -> Self {
        match transport {
            TransportArg::Tcp => Self::Tcp,
            TransportArg::Http => Self::Http,
            TransportArg::Quic => Self::Quic,
            TransportArg::Websocket => Self::Websocket,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let outcome = match cli.command {
        Command::Agent => agent::serve_stdio()
            .await
            .map_err(|error| format!("agent: {error}")),
        Command::Check { tests } => check(&tests),
        Command::Matrix { reports, markdown } => matrix(&reports, markdown.as_deref()),
        Command::Run(args) => run(args, false).await,
        Command::Record(args) => run(args, true).await,
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn check(tests: &Path) -> Result<(), String> {
    let (files, unread) = format::load_corpus(tests).map_err(|error| error.to_string())?;
    let count: usize = files.iter().map(|file| file.file.tests.len()).sum();
    println!("{} files with {count} tests are valid", files.len());
    for file in unread {
        println!("not read: {}: {}", file.name, file.reason);
    }
    Ok(())
}

fn matrix(paths: &[PathBuf], markdown: Option<&Path>) -> Result<(), String> {
    let mut reports = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let report: RunReport =
            serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        reports.push(report);
    }
    let table = report::matrix_markdown(&reports);
    match markdown {
        Some(path) => std::fs::write(path, table)
            .map_err(|error| format!("cannot write {}: {error}", path.display())),
        None => {
            print!("{table}");
            Ok(())
        }
    }
}

async fn run(args: RunArgs, record: bool) -> Result<(), String> {
    let (files, unread) = format::load_corpus(&args.tests).map_err(|error| error.to_string())?;
    for file in &unread {
        tracing::warn!("Not read: {}: {}", file.name, file.reason);
    }
    let skips = args.skips.as_deref().map(SkipList::load).transpose()?;
    let config = RunConfig {
        server: args.server,
        no_quorum: args.no_quorum,
        root: Credentials {
            username: args.username,
            password: args.password,
        },
        transport: args.transport.into(),
        agent: if record || args.agent == "rust" {
            AgentCommand::BuiltIn
        } else {
            AgentCommand::Shell(args.agent)
        },
        call_timeout: args.call_timeout,
        hello_timeout: args.hello_timeout,
        filter: args.filter.clone(),
        record,
    };
    let mut runner = Runner::start(config)
        .await
        .map_err(|error| error.to_string())?;
    tracing::info!(
        "Running against the {} agent {}",
        runner.hello().sdk,
        runner.hello().sdk_version
    );
    let report = runner.run(&files).await;
    runner.stop().await;

    if let Some(path) = &args.junit {
        write(path, &report::junit_xml(&report))?;
    }
    if let Some(path) = &args.json {
        let json = serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?;
        write(path, &json)?;
    }
    let totals = Totals::of(&report.tests);
    println!(
        "{}: {} passed, {} failed, {} errored, {} skipped",
        report.sdk, totals.passed, totals.failed, totals.errored, totals.skipped
    );
    let violations = skips
        .map(|skips| skips.violations(&report, args.filter.is_some()))
        .unwrap_or_default();
    for violation in &violations {
        eprintln!("{violation}");
    }
    if totals.failed + totals.errored > 0 || !violations.is_empty() {
        return Err("the conformance run failed".to_owned());
    }
    Ok(())
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|error| format!("cannot write {}: {error}", path.display()))
}
