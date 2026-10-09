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

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use iggy::prelude::{
    Client, HeaderKey, HeaderKind, HeaderValue, Identifier, IggyClient, IggyError, IggyMessage,
    MessageClient, Partitioning,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::json;

use crate::{Event, Target, connect_any};

/// Carries the event key outside the payload, so the verifier also checks
/// that user headers survive replication and recovery intact.
pub const KEY_HEADER: &str = "event-key";
/// Fills each payload up to `--payload-bytes`; the verifier checks it.
pub const PAD_BYTE: u8 = b'x';

const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// Resends reach back at most this many events per symbol.
const RESEND_HISTORY: usize = 4096;

#[derive(clap::Args, Clone)]
pub struct ProduceArgs {
    #[command(flatten)]
    target: Target,
    /// Label for this run. Keys and log files carry it, so several runs can
    /// write one topic and be verified together.
    #[arg(long, default_value = "a")]
    run: String,
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..))]
    producers: u32,
    /// Message keys per producer. A key always maps to one partition.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..))]
    symbols_per_producer: u32,
    /// Events per send.
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u16).range(1..))]
    batch: u16,
    /// Target new events per second across all producers (0 = unthrottled).
    #[arg(long, default_value_t = 2000)]
    rate: u64,
    #[arg(long, default_value_t = 60)]
    seconds: u64,
    /// Share of sends that resend earlier, already acknowledged events, as a
    /// client retrying after a lost reply does. Without server-side
    /// deduplication each one is a duplicate in the log. A resent copy also
    /// counts as present for its key, so combine this with faults only when
    /// duplicates, not loss, are under test.
    #[arg(long, default_value_t = 0.0, value_parser = parse_share)]
    resend_rate: f64,
    /// Share of resends made from a brand-new session, as a restarted client.
    #[arg(long, default_value_t = 0.25, value_parser = parse_share)]
    fresh_session_rate: f64,
    /// Payload bytes per event, padding included.
    #[arg(long, default_value_t = 256)]
    payload_bytes: usize,
    /// Directory for the attempted and acknowledged logs.
    #[arg(long)]
    out: String,
    /// Fail the run when one send keeps failing for this long.
    #[arg(long, default_value_t = 180)]
    retry_budget_seconds: u64,
}

#[derive(Default)]
struct Counters {
    sends: AtomicU64,
    send_errors: AtomicU64,
    new_events: AtomicU64,
    resent_events: AtomicU64,
    fresh_sessions: AtomicU64,
}

pub async fn run(args: ProduceArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out)?;
    let addresses = args.target.addresses();
    let counters = Arc::new(Counters::default());
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.seconds);
    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..args.producers {
        let (args, addresses, counters) = (args.clone(), addresses.clone(), counters.clone());
        tasks.spawn(produce(producer, args, addresses, counters, deadline));
    }
    let mut failures = Vec::new();
    let mut latencies_us = Vec::new();
    while let Some(done) = tasks.join_next().await {
        match done.map_err(anyhow::Error::from).and_then(|result| result) {
            Ok(latencies) => latencies_us.extend(latencies),
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    latencies_us.sort_unstable();
    let elapsed = started.elapsed().as_secs_f64();
    let new_events = counters.new_events.load(Ordering::Relaxed);
    let resent_events = counters.resent_events.load(Ordering::Relaxed);
    let summary = json!({
        "run": args.run,
        "elapsed_seconds": elapsed,
        "events_per_second": (new_events + resent_events) as f64 / elapsed,
        "send_latency_ms": {
            "p50": percentile_ms(&latencies_us, 0.50),
            "p90": percentile_ms(&latencies_us, 0.90),
            "p99": percentile_ms(&latencies_us, 0.99),
            "p999": percentile_ms(&latencies_us, 0.999),
            "max": latencies_us.last().copied().unwrap_or(0) as f64 / 1000.0,
        },
        "sends": counters.sends.load(Ordering::Relaxed),
        "send_errors": counters.send_errors.load(Ordering::Relaxed),
        "new_events": new_events,
        "resent_events": resent_events,
        "fresh_sessions": counters.fresh_sessions.load(Ordering::Relaxed),
        "producer_failures": failures,
    });
    println!("{summary}");
    std::fs::write(
        format!("{}/produce-{}.json", args.out, args.run),
        summary.to_string(),
    )?;
    if !failures.is_empty() {
        bail!("{} producers failed", failures.len());
    }
    Ok(())
}

fn parse_share(value: &str) -> Result<f64, String> {
    match value.parse::<f64>() {
        Ok(share) if (0.0..=1.0).contains(&share) => Ok(share),
        _ => Err(format!("{value} is not a number between 0 and 1")),
    }
}

fn percentile_ms(sorted_us: &[u64], quantile: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_us.len() - 1) as f64 * quantile).round() as usize;
    sorted_us[rank] as f64 / 1000.0
}

/// One producer: one session, events keyed by symbol, each send retried
/// with the same events until acknowledged. The acknowledged log therefore
/// holds exactly what the cluster promised to keep. Returns the latency of
/// every acknowledged send, retries included, in microseconds.
async fn produce(
    producer: u32,
    args: ProduceArgs,
    addresses: Vec<String>,
    counters: Arc<Counters>,
    deadline: Instant,
) -> Result<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(0x1990 + u64::from(producer));
    let stream_id = args.target.stream_id()?;
    let topic_id = args.target.topic_id()?;
    let symbols: Vec<String> = (0..args.symbols_per_producer)
        .map(|symbol| format!("{}-p{producer}-s{symbol}", args.run))
        .collect();
    let mut client = connect_any(&addresses, producer as usize).await?;
    // An event lands in `attempted` before its first send and in `acked` once
    // a send carrying it succeeded. A crash in between is an unacknowledged
    // event, never a lost one. The keys repeat for the same run label, so
    // logs of an earlier run with that label are never overwritten.
    let mut attempted = BufWriter::new(File::create_new(format!(
        "{}/attempted-{}-{producer}.jsonl",
        args.out, args.run
    ))?);
    let mut acked = BufWriter::new(File::create_new(format!(
        "{}/acked-{}-{producer}.jsonl",
        args.out, args.run
    ))?);
    let mut history: HashMap<String, Vec<Event>> = HashMap::new();
    let pad = char::from(PAD_BYTE)
        .to_string()
        .repeat(args.payload_bytes.saturating_sub(112));
    let batch = usize::from(args.batch);
    let rate_per_producer = args.rate as f64 / f64::from(args.producers);
    let budget = Duration::from_secs(args.retry_budget_seconds);
    let started = Instant::now();
    let mut latencies_us = Vec::new();
    let mut seq = 0u64;
    while Instant::now() < deadline {
        let symbol = symbols[rng.random_range(0..symbols.len())].clone();
        let resend = args.resend_rate > 0.0
            && rng.random_bool(args.resend_rate)
            && history.get(&symbol).is_some_and(|sent| !sent.is_empty());
        let events: Vec<Event> = if resend {
            let sent = &history[&symbol];
            let len = rng.random_range(1..=batch.min(sent.len()));
            let start = rng.random_range(0..=sent.len() - len);
            sent[start..start + len].to_vec()
        } else {
            let new_events: Vec<Event> = (0..batch)
                .map(|_| {
                    seq += 1;
                    Event {
                        key: format!("{}-p{producer}-{seq}", args.run),
                        run: args.run.clone(),
                        producer,
                        symbol: symbol.clone(),
                        seq,
                        pad: pad.clone(),
                    }
                })
                .collect();
            for event in &new_events {
                writeln!(attempted, "{}", event.record())?;
            }
            attempted.flush()?;
            new_events
        };
        let fresh_session = resend && rng.random_bool(args.fresh_session_rate);
        let fresh_start = rng.random_range(0..addresses.len());
        let partitioning = Partitioning::messages_key(symbol.as_bytes())?;
        let send_started = Instant::now();
        let mut attempt = 0u32;
        loop {
            let mut messages = events.iter().map(message).collect::<Result<Vec<_>>>()?;
            let result = if fresh_session && attempt == 0 {
                counters.fresh_sessions.fetch_add(1, Ordering::Relaxed);
                match connect_any(&addresses, fresh_start).await {
                    Ok(other) => {
                        let result =
                            send(&other, &stream_id, &topic_id, &partitioning, &mut messages).await;
                        let _ = other.shutdown().await;
                        result
                    }
                    Err(error) => {
                        eprintln!("producer {producer}: fresh session: {error:#}");
                        Err(IggyError::CannotEstablishConnection)
                    }
                }
            } else {
                send(&client, &stream_id, &topic_id, &partitioning, &mut messages).await
            };
            counters.sends.fetch_add(1, Ordering::Relaxed);
            let Err(error) = result else { break };
            counters.send_errors.fetch_add(1, Ordering::Relaxed);
            if send_started.elapsed() > budget {
                bail!("producer {producer}: a send kept failing for {budget:?}: {error}");
            }
            attempt += 1;
            eprintln!("producer {producer}: send attempt {attempt} failed: {error}");
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt.min(10)))).await;
            // Replace a session that keeps failing, as long-lived clients do.
            if attempt.is_multiple_of(5)
                && let Ok(replacement) =
                    connect_any(&addresses, attempt as usize + producer as usize).await
            {
                let _ = std::mem::replace(&mut client, replacement).shutdown().await;
            }
        }
        latencies_us.push(send_started.elapsed().as_micros() as u64);
        if resend {
            counters
                .resent_events
                .fetch_add(events.len() as u64, Ordering::Relaxed);
        } else {
            counters
                .new_events
                .fetch_add(events.len() as u64, Ordering::Relaxed);
            for event in &events {
                writeln!(acked, "{}", event.record())?;
            }
            acked.flush()?;
            let sent = history.entry(symbol).or_default();
            sent.extend(events);
            if sent.len() > RESEND_HISTORY {
                sent.drain(..sent.len() - RESEND_HISTORY);
            }
        }
        if rate_per_producer > 0.0 {
            let due = Duration::from_secs_f64(seq as f64 / rate_per_producer);
            if let Some(wait) = due.checked_sub(started.elapsed()) {
                tokio::time::sleep(wait).await;
            }
        }
    }
    let _ = client.shutdown().await;
    Ok(latencies_us)
}

async fn send(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
    messages: &mut [IggyMessage],
) -> Result<(), IggyError> {
    tokio::time::timeout(
        SEND_TIMEOUT,
        client.send_messages(stream_id, topic_id, partitioning, messages),
    )
    .await
    .unwrap_or(Err(IggyError::TaskTimeout))
    .map(|_| ())
}

fn message(event: &Event) -> Result<IggyMessage> {
    let headers = BTreeMap::from([(
        HeaderKey::from_str(KEY_HEADER)?,
        HeaderValue::from_raw(HeaderKind::Raw, event.key.as_bytes())?,
    )]);
    Ok(IggyMessage::builder()
        .payload(serde_json::to_vec(event)?.into())
        .user_headers(headers)
        .build()?)
}
