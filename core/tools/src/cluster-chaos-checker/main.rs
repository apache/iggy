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

//! Load generator and log verifier for the cluster chaos harness in
//! `scripts/cluster-chaos`.
//!
//! `produce` records every event it sends and every event the cluster
//! acknowledged. `verify` reads every partition back from offset 0 and
//! checks those records against the log: acknowledged events lost, offset
//! gaps, per-key reordering, events nobody sent, and a tail the topic lists
//! but no read returns.

mod produce;
mod verify;

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use clap::{Parser, Subcommand};
use iggy::prelude::{
    AutoLogin, Client, Credentials, Durability, Identifier, IggyClient, StreamClient, TopicClient,
    TopicCreateOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(about = "Produce to and verify an Iggy cluster under fault injection")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Wait for the cluster to answer, then create the stream and topic.
    Setup(SetupArgs),
    /// Produce keyed events, logging what was attempted and acknowledged.
    Produce(produce::ProduceArgs),
    /// Read the whole topic back and check it against the produce logs.
    Verify(verify::VerifyArgs),
}

#[derive(clap::Args, Clone)]
struct Target {
    /// Comma-separated client (TCP) addresses of the nodes.
    #[arg(long)]
    addresses: String,
    #[arg(long, default_value = "chaos")]
    stream: String,
    #[arg(long, default_value = "events")]
    topic: String,
}

impl Target {
    fn addresses(&self) -> Vec<String> {
        self.addresses.split(',').map(str::to_string).collect()
    }

    fn stream_id(&self) -> Result<Identifier> {
        Ok(Identifier::from_str(&self.stream)?)
    }

    fn topic_id(&self) -> Result<Identifier> {
        Ok(Identifier::from_str(&self.topic)?)
    }
}

#[derive(clap::Args)]
struct SetupArgs {
    #[command(flatten)]
    target: Target,
    #[arg(long, default_value_t = 6)]
    partitions: u32,
    #[arg(long, value_enum, default_value_t = Durability::Replicated)]
    durability: Durability,
    /// Give up when the cluster has not accepted the topic within this long.
    #[arg(long, default_value_t = 180)]
    timeout_seconds: u64,
}

/// One produced event. The payload is this struct as JSON, so the verifier
/// can tell which producer sent what, in which order, without trusting
/// anything the server reports about it.
#[derive(Serialize, Deserialize, Clone)]
struct Event {
    key: String,
    run: String,
    producer: u32,
    symbol: String,
    seq: u64,
    #[serde(default)]
    pad: String,
}

impl Event {
    /// The event as a produce-log line. The padding is left out: the verifier
    /// only checks that it is intact.
    fn record(&self) -> serde_json::Value {
        json!({
            "key": self.key,
            "run": self.run,
            "producer": self.producer,
            "symbol": self.symbol,
            "seq": self.seq,
        })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    // The SDK logs reconnects and redirects, which explain stalled sends.
    Registry::default()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();
    match Cli::parse().command {
        Command::Setup(args) => setup(args).await,
        Command::Produce(args) => produce::run(args).await,
        Command::Verify(args) => verify::run(args).await,
    }
}

async fn setup(args: SetupArgs) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(args.timeout_seconds);
    let addresses = args.target.addresses();
    let mut attempt = 0;
    loop {
        match try_setup(&args, &addresses, attempt).await {
            Ok(()) => {
                println!(
                    "created {}/{} with {} partitions, durability {}",
                    args.target.stream, args.target.topic, args.partitions, args.durability
                );
                return Ok(());
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!("setup attempt {attempt}: {error:#}");
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error.context("cluster never accepted the topic")),
        }
    }
}

/// Idempotent, because a create whose reply was lost to an election may
/// still have committed.
async fn try_setup(args: &SetupArgs, addresses: &[String], attempt: usize) -> Result<()> {
    let client = connect_any(addresses, attempt).await?;
    let stream_id = args.target.stream_id()?;
    let topic_id = args.target.topic_id()?;
    if client.get_stream(&stream_id).await?.is_none() {
        client.create_stream(&args.target.stream).await?;
    }
    if client.get_topic(&stream_id, &topic_id).await?.is_none() {
        let options = TopicCreateOptions {
            partitions_count: Some(args.partitions),
            durability: args.durability,
            ..TopicCreateOptions::default()
        };
        client
            .create_topic(&stream_id, &args.target.topic, &options)
            .await?;
    }
    let _ = client.shutdown().await;
    Ok(())
}

/// `IGGY_USERNAME` / `IGGY_PASSWORD`, defaulting to the harness's root user.
fn credentials() -> (String, String) {
    (
        std::env::var("IGGY_USERNAME").unwrap_or_else(|_| "iggy".into()),
        std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| "iggy".into()),
    )
}

async fn connect(address: &str) -> Result<IggyClient> {
    let (username, password) = credentials();
    let client = IggyClient::builder()
        .with_tcp()
        .with_server_address(address.to_string())
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            username,
            password.into(),
        )))
        .build()?;
    tokio::time::timeout(CONNECT_TIMEOUT, client.connect())
        .await
        .map_err(|_| anyhow!("connect to {address} timed out"))??;
    Ok(client)
}

/// Connects to the first node that answers, trying them in order from
/// `start`. Sign-in may still move the session to the metadata leader.
async fn connect_any(addresses: &[String], start: usize) -> Result<IggyClient> {
    let mut errors = Vec::with_capacity(addresses.len());
    for index in 0..addresses.len() {
        let address = &addresses[(start + index) % addresses.len()];
        match connect(address).await {
            Ok(client) => return Ok(client),
            Err(error) => errors.push(format!("{address}: {error:#}")),
        }
    }
    bail!("no node reachable: {}", errors.join("; "))
}
