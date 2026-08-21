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

//! Deterministic workload fuzzer for the Iggy simulator.
//!
//! Drives [`simulator::workload::run_with_faults`] (per-tick invariants plus
//! crash, restart and network fault injection) for a number of ticks, then
//! optionally quiesces and asserts the Phase C consensus checks. Everything is a
//! function of `--seed`, logged at start and on panic so any failure replays
//! with `--seed <value>`.
//!
//! ```text
//! workload-fuzz [--seed N] [--ticks N] [--clients N] [--replicas N]
//!               [--plane partition|metadata|mixed|uniform]
//!               [--faults none|light|heavy|swarm] [--no-quiesce]
//!               [--crash-prob F] [--restart-prob F] [--crash-primary]
//!               [network overrides: --packet-loss-prob, --replay-prob, --partition-mode,
//!                --partition-prob, --unpartition-prob, --clog-prob, ...]
//! ```
//!
//! `--plane` selects the op mix (see [`ActionWeights`]). Partition-plane runs drain
//! and converge most readily; `uniform` is the widest per-tick op coverage.
//!
//! `--faults` picks a whole network fault profile, and the individual network flags
//! override single fields of it, so exploring one axis does not mean spelling out
//! the other ten. The default is `none`, a perfect network, so a run says what it
//! injects rather than inheriting it.
//!
//! `--faults swarm` derives every network parameter from `--seed`, which is what a
//! CI campaign wants: `none`/`light`/`heavy` are three points in parameter space,
//! so a thousand seeds against `heavy` is the same network a thousand times. The
//! drawn values print on the `network:` line and `--seed` replays them exactly.

use clap::{Parser, ValueEnum};
use iggy_common::IggyByteSize;
use server_common::sharding::IggyNamespace;
use server_common::{MemoryPool, MemoryPoolSettings};
use simulator::Simulator;
use simulator::client::SimClient;
use simulator::packet::{COMMAND_LABELS, PacketSimulatorOptions, PartitionMode, PartitionSymmetry};
use simulator::workload::actions::Action;
use simulator::workload::options::{ActionWeights, WorkloadOptions};
use simulator::workload::{FaultInjector, Workload, oracle, run_with_faults};
use strum::IntoEnumIterator;

#[derive(Parser)]
#[command(about = "Deterministic workload fuzzer for the Iggy simulator")]
#[allow(clippy::struct_excessive_bools)]
struct Args {
    /// Omitted draws a random seed (logged for replay).
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, default_value_t = 10_000)]
    ticks: u64,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..))]
    clients: u8,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u8).range(1..))]
    replicas: u8,
    /// Op mix to draw from.
    #[arg(long, value_enum, default_value_t = Plane::Partition)]
    plane: Plane,
    /// Probability a consumer-offset store asks for `Quorum` rather than
    /// `NoAck`. `1.0` keeps every offset op on the replicated path.
    #[arg(long, default_value_t = 0.5, value_parser = parse_unit_interval)]
    ack_quorum_ratio: f32,
    /// Per-tick chance one eligible replica is crashed.
    #[arg(long, default_value_t = 0.0, value_parser = parse_unit_interval)]
    crash_prob: f32,
    /// Per-tick chance one crashed replica is restarted. Without this a crash
    /// is permanent and nothing exercises rejoin or log repair.
    #[arg(long, default_value_t = 0.0, value_parser = parse_unit_interval)]
    restart_prob: f32,
    /// Crash the primary too, putting a view change under live traffic.
    #[arg(long)]
    crash_primary: bool,
    /// Bound every replica's metadata WAL to this many slots, which is what forces a
    /// checkpoint (`should_checkpoint` gates on remaining capacity). Unbounded by
    /// default, and an unbounded journal never checkpoints, so WAL drain,
    /// `snapshot_op` movement, `RangeEvicted` and metadata state transfer are all
    /// unreachable until this is set.
    #[arg(long)]
    journal_slots: Option<usize>,
    /// Let a rebuilt partition recover its consensus frontier from the log the
    /// harness carried across the restart.
    ///
    /// OFF by default, because a real replica cannot do this: the partition journal
    /// is in-memory and segments carry no op numbers, so production's
    /// `load_partition` restores the view alone and rejoins quorum-invisible. With it
    /// off a restarted replica comes back at op 0 and the run exercises that rejoin,
    /// where `advance_commit_min`'s sequential-advance assert lives. Turn it on to
    /// look PAST that at something later in the run, studying a system more durable
    /// than Iggy is.
    #[arg(long)]
    restore_partition_frontier: bool,
    /// Fail the run if the entity oracle did not hold at quiesce.
    ///
    /// An eviction disarms it (the forgotten request's fate is unknown) and it
    /// re-arms only once the shadow is proven equal to committed state again. Without
    /// this flag a run whose oracle stayed disarmed still exits 0.
    #[arg(long)]
    require_entity_oracle: bool,
    /// Route every client request through the server's real dispatch handlers
    /// instead of the raw `on_message` fast path. Clients then log in against the
    /// seeded root user and carry a bound session, so the run also covers
    /// authorization and session lifecycle, which exist only on this path.
    #[arg(long)]
    shell: bool,
    #[arg(long)]
    no_quiesce: bool,

    /// Network fault profile. Individual network flags below override single
    /// fields of the profile.
    #[arg(long, value_enum, default_value_t = Faults::None)]
    faults: Faults,
    /// Chance a packet is dropped at delivery time.
    #[arg(long, value_parser = parse_unit_interval_f64)]
    packet_loss_prob: Option<f64>,
    /// Chance a packet is duplicated at delivery time.
    #[arg(long, value_parser = parse_unit_interval_f64)]
    replay_prob: Option<f64>,
    /// Minimum one-way delay, in ticks.
    #[arg(long)]
    one_way_delay_min: Option<u64>,
    /// Mean one-way delay, in ticks (exponentially distributed).
    #[arg(long)]
    one_way_delay_mean: Option<u64>,
    /// Maximum packets queued on a single link; beyond it the link drops.
    #[arg(long)]
    link_capacity: Option<u8>,
    /// How an automatic partition picks its sides.
    #[arg(long, value_enum)]
    partition_mode: Option<PartitionModeArg>,
    /// Whether a partition blocks both directions or just one.
    #[arg(long, value_enum)]
    partition_symmetry: Option<PartitionSymmetryArg>,
    /// Per-tick chance a partition forms while connectivity is whole.
    #[arg(long, value_parser = parse_unit_interval_f64)]
    partition_prob: Option<f64>,
    /// Per-tick chance a standing partition heals.
    #[arg(long, value_parser = parse_unit_interval_f64)]
    unpartition_prob: Option<f64>,
    /// Minimum ticks a partition lasts once formed.
    #[arg(long)]
    partition_stability: Option<u32>,
    /// Minimum ticks of whole connectivity before another partition may form.
    #[arg(long)]
    unpartition_stability: Option<u32>,
    /// Per-tick chance any one path clogs (stops delivering, keeps queueing).
    #[arg(long, value_parser = parse_unit_interval_f64)]
    clog_prob: Option<f64>,
    /// Mean clog duration, in ticks (exponentially distributed).
    #[arg(long)]
    clog_duration_mean: Option<u64>,
}

/// Named network fault profile: one flag for "how hostile is the network", rather
/// than eleven.
///
/// Progress falls off steeply with severity, every lost frame costing a resend
/// timeout: on one namespace with one client, a 3-replica cluster drains roughly 440
/// replies in 5000 ticks on a perfect network, 240 under `light` and 40 under
/// `heavy`. All still drain and converge, so budget ticks accordingly rather than
/// reading a low reply count as a stall.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Faults {
    /// Perfect network. Delays only, no loss and no partitions.
    None,
    /// Occasional loss, duplication and short one-sided partitions. Meant to
    /// stay inside the range where a healthy cluster still drains.
    Light,
    /// Frequent loss, long partitions and clogged paths. Progress stalls for
    /// stretches, so budget ticks generously; the stalls are transient and a healthy
    /// cluster still drains and converges, which is why the quiesce assert treats a
    /// failure to drain as real rather than as expected weather.
    Heavy,
    /// Every parameter drawn from the seed. What a CI campaign should run: the three
    /// fixed profiles above are three points in an eleven-dimensional space, so
    /// throwing seeds at one of them varies the traffic and never the network.
    /// Severity ranges over roughly `none` through half again `heavy`, so some seeds
    /// draw a calm network and some worse than `heavy`; both are the point. See
    /// [`PacketSimulatorOptions::swarm`].
    Swarm,
}

/// Clap mirror of [`PartitionMode`], so the library type stays free of a clap
/// derive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum PartitionModeArg {
    None,
    UniformSize,
    UniformPartition,
    IsolateSingle,
}

impl From<PartitionModeArg> for PartitionMode {
    fn from(value: PartitionModeArg) -> Self {
        match value {
            PartitionModeArg::None => Self::None,
            PartitionModeArg::UniformSize => Self::UniformSize,
            PartitionModeArg::UniformPartition => Self::UniformPartition,
            PartitionModeArg::IsolateSingle => Self::IsolateSingle,
        }
    }
}

/// Clap mirror of [`PartitionSymmetry`]; see [`PartitionModeArg`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum PartitionSymmetryArg {
    Symmetric,
    Asymmetric,
}

impl From<PartitionSymmetryArg> for PartitionSymmetry {
    fn from(value: PartitionSymmetryArg) -> Self {
        match value {
            PartitionSymmetryArg::Symmetric => Self::Symmetric,
            PartitionSymmetryArg::Asymmetric => Self::Asymmetric,
        }
    }
}

impl Faults {
    /// Base network options for this profile. `node_count`, `client_count` and
    /// `seed` are filled by the caller. Takes the seed because [`Faults::Swarm`]
    /// derives its whole shape from it; the fixed profiles ignore it, which is what
    /// makes them reproducible without one.
    fn options(self, seed: u64) -> PacketSimulatorOptions {
        match self {
            // `PacketSimulatorOptions::default` is already a perfect network:
            // delay only, every probability zero.
            Self::None => PacketSimulatorOptions::default(),
            Self::Swarm => PacketSimulatorOptions::swarm(seed),
            Self::Light => PacketSimulatorOptions {
                packet_loss_probability: 0.02,
                replay_probability: 0.01,
                partition_probability: 0.005,
                unpartition_probability: 0.05,
                partition_stability: 20,
                unpartition_stability: 40,
                partition_mode: PartitionMode::IsolateSingle,
                partition_symmetry: PartitionSymmetry::Asymmetric,
                path_clog_probability: 0.002,
                path_clog_duration_mean: 10,
                ..PacketSimulatorOptions::default()
            },
            Self::Heavy => PacketSimulatorOptions {
                packet_loss_probability: 0.10,
                replay_probability: 0.03,
                one_way_delay_mean: 8,
                partition_probability: 0.02,
                unpartition_probability: 0.02,
                partition_stability: 50,
                unpartition_stability: 50,
                partition_mode: PartitionMode::UniformSize,
                partition_symmetry: PartitionSymmetry::Asymmetric,
                path_clog_probability: 0.01,
                path_clog_duration_mean: 25,
                ..PacketSimulatorOptions::default()
            },
        }
    }
}

/// Which plane the sampled ops target. Maps onto an [`ActionWeights`] preset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Plane {
    /// Writes and consumer offsets only.
    Partition,
    /// Replicated metadata mutations only.
    Metadata,
    /// Stream creates over a write-heavy base.
    Mixed,
    /// Every action equally likely.
    Uniform,
}

impl Plane {
    fn weights(self) -> ActionWeights {
        match self {
            Self::Partition => ActionWeights::partition_only(),
            Self::Metadata => ActionWeights::metadata_only(),
            Self::Mixed => ActionWeights::default(),
            Self::Uniform => ActionWeights::uniform(),
        }
    }
}

/// Clap value parser: accept a probability in `[0.0, 1.0]`.
fn parse_unit_interval(raw: &str) -> Result<f32, String> {
    let value: f32 = raw
        .parse()
        .map_err(|_| format!("`{raw}` is not a number"))?;
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(format!("must be within [0.0, 1.0], got {value}"))
    }
}

/// [`parse_unit_interval`] for the network knobs, which are `f64`.
fn parse_unit_interval_f64(raw: &str) -> Result<f64, String> {
    let value: f64 = raw
        .parse()
        .map_err(|_| format!("`{raw}` is not a number"))?;
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(format!("must be within [0.0, 1.0], got {value}"))
    }
}

/// What this run is, on two lines: the cluster and workload shape, then every
/// network parameter.
///
/// Every network field, not the interesting subset. Under [`Faults::Swarm`] these
/// ARE the run's identity, and a report naming half of them cannot be read against
/// a failure without re-deriving the rest by hand.
fn print_run_banner(args: &Args, seed: u64, network: &PacketSimulatorOptions) {
    println!(
        "workload-fuzz: seed={seed} ticks={} clients={} replicas={} plane={:?} \
         faults={:?} shell={} crash_prob={} quiesce={}",
        args.ticks,
        args.clients,
        args.replicas,
        args.plane,
        args.faults,
        args.shell,
        args.crash_prob,
        !args.no_quiesce,
    );
    println!(
        "network: loss={} replay={} delay={}..{} partition={:?}/{:?} \
         p_partition={} p_unpartition={} stability={}/{} clog={} clog_ticks={} \
         link_capacity={}",
        network.packet_loss_probability,
        network.replay_probability,
        network.one_way_delay_min,
        network.one_way_delay_mean,
        network.partition_mode,
        network.partition_symmetry,
        network.partition_probability,
        network.unpartition_probability,
        network.partition_stability,
        network.unpartition_stability,
        network.path_clog_probability,
        network.path_clog_duration_mean,
        network.link_capacity,
    );
}

/// The chosen fault profile with any individually-set network flag applied over
/// it, plus the cluster shape and seed.
fn network_options(args: &Args, replicas: u8, clients: u8, seed: u64) -> PacketSimulatorOptions {
    let mut options = args.faults.options(seed);
    options.node_count = replicas;
    options.client_count = clients;
    options.seed = seed;

    if let Some(value) = args.packet_loss_prob {
        options.packet_loss_probability = value;
    }
    if let Some(value) = args.replay_prob {
        options.replay_probability = value;
    }
    if let Some(value) = args.one_way_delay_min {
        options.one_way_delay_min = value;
    }
    if let Some(value) = args.one_way_delay_mean {
        options.one_way_delay_mean = value;
    }
    if let Some(value) = args.link_capacity {
        options.link_capacity = value;
    }
    if let Some(value) = args.partition_mode {
        options.partition_mode = value.into();
    }
    if let Some(value) = args.partition_symmetry {
        options.partition_symmetry = value.into();
    }
    if let Some(value) = args.partition_prob {
        options.partition_probability = value;
    }
    if let Some(value) = args.unpartition_prob {
        options.unpartition_probability = value;
    }
    if let Some(value) = args.partition_stability {
        options.partition_stability = value;
    }
    if let Some(value) = args.unpartition_stability {
        options.unpartition_stability = value;
    }
    if let Some(value) = args.clog_prob {
        options.path_clog_probability = value;
    }
    if let Some(value) = args.clog_duration_mean {
        options.path_clog_duration_mean = value;
    }

    // Every other override above is one self-sufficient field. The partition knobs
    // are not: the probability roll calls `auto_partition_network`, whose
    // `PartitionMode::None` arm clears every side, so `--partition-prob 0.3` against
    // the default `none` profile prints `p_partition=0.3 partition=None`, reports OK,
    // and partitions zero times. Imply a mode when the caller asked for partitions
    // without naming one; an explicit `--partition-mode` still wins.
    let asked_for_partitions = args.partition_prob.is_some_and(|value| value > 0.0)
        || args.unpartition_prob.is_some()
        || args.partition_stability.is_some()
        || args.unpartition_stability.is_some();
    if asked_for_partitions
        && args.partition_mode.is_none()
        && options.partition_mode == PartitionMode::None
    {
        options.partition_mode = PartitionMode::UniformSize;
    }
    options
}

fn main() {
    let args = Args::parse();

    // Server-side diagnostics (`emit_partition_diag` and friends) are the only
    // record of a request the server dropped after logging, which is the shape that
    // wedges a client's in-flight slot. Without a subscriber they go nowhere and the
    // run looks like an unexplained stall, so install one and let `RUST_LOG` select.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    // A provided seed reproduces a prior run exactly; otherwise draw one and
    // log it. Both the network and workload PRNGs derive from it.
    let seed = args.seed.unwrap_or_else(rand::random);
    let ticks = args.ticks;
    let clients = args.clients;
    let replicas = args.replicas;
    let plane = args.plane;
    let crash_prob = args.crash_prob;
    let quiesce = !args.no_quiesce;

    // Surface the seed on any panic (invariant or oracle violation) so the run
    // is replayable. The process still exits non-zero via the default hook.
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("workload-fuzz FAILED, reproduce with --seed {seed}\n{info}");
    }));

    let network_opts = network_options(&args, replicas, clients, seed);
    print_run_banner(&args, seed, &network_opts);

    // poll_messages / reply paths panic without an initialized pool; disabled
    // pooling falls through to the system allocator.
    MemoryPool::init_pool(&MemoryPoolSettings {
        enabled: false,
        size: IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });

    let (mut sim, sim_clients, ns) = build_cluster(&args, seed, replicas, clients, network_opts);

    let mut options = WorkloadOptions::new(seed, replicas, vec![ns]);
    options.client_count = clients;
    options.crash_per_tick_ratio = crash_prob;
    options.restart_per_tick_ratio = args.restart_prob;
    options.spare_primary = !args.crash_primary;
    options.ack_quorum_ratio = args.ack_quorum_ratio;
    options.weights = plane.weights();
    let mut workload = Workload::new(options);

    let mut injector = FaultInjector::new(seed, replicas);
    let replies = run_with_faults(
        &mut sim,
        &mut workload,
        &sim_clients,
        ticks,
        u64::MAX,
        &mut injector,
    );
    println!(
        "ran {ticks} ticks; {replies} replies; crashes={} restarts={} still down: {}",
        injector.crashes(),
        injector.restarts(),
        sim.crashed.len(),
    );

    // Printed before the quiesce assert, so a failed drain still reports what
    // the run managed to do. Reading it after the assert meant the failure that
    // most needs the numbers is the one that never shows them.
    print_coverage(&workload);

    if quiesce {
        // A failed drain is a hard failure, not a warning. It was a warning while a
        // lost request could not be retried, making stalls expected and
        // unactionable; with the client resending, a request unanswered inside the
        // budget is either a wedge or a liveness bug.
        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut workload, 50_000),
            "{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
        // Then wait for one agreed view before asserting. `assert_converged` resolves
        // the leader as whichever live replica claims to be primary, so asserting
        // mid-view-change finds none or finds a deposed one, both false failures.
        assert!(
            oracle::settle_to_stable_view(&mut sim, &mut workload, 50_000),
            "metadata views never converged after the drain\n{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
        oracle::assert_converged(&sim, &mut workload);
        // Named, not implied. `assert_converged` skips the entity oracle when an
        // eviction disarmed it and the shadow has not been proven consistent since,
        // so "converged" alone would report a run that asserted nothing about entity
        // state exactly like one that asserted everything.
        let entity_oracle = if !workload.serial_run() {
            "skipped (concurrent run)"
        } else if workload.strict_outcome_oracle() {
            "held"
        } else {
            "DISARMED by an eviction and never re-armed"
        };
        println!(
            "quiesced and converged (leader-relative; entity oracle: {entity_oracle}; \
             evictions={})",
            workload.evictions(),
        );
        assert!(
            !args.require_entity_oracle || workload.strict_outcome_oracle(),
            "--require-entity-oracle: the entity oracle was {entity_oracle}, so this run \
             proved nothing about entity state (seed={seed:#x})"
        );
        // Again after the drain: the drain both answers outstanding requests and
        // issues its own resends, so the pre-drain numbers are not the final ones.
        print_coverage(&workload);
    }

    print_command_coverage(&sim);
    println!("workload-fuzz: OK (seed={seed})");
}

/// Stand up the cluster, seed its namespace, and get every client a session.
///
/// The shell path differs in two ways that have to agree: a partition request's
/// namespace resolves against committed metadata, so the stream and topic behind it
/// must exist and not just the partition group; and dispatch admits a request only
/// from a bound session, which only a login mints.
fn build_cluster(
    args: &Args,
    args_seed: u64,
    replicas: u8,
    clients: u8,
    network_opts: PacketSimulatorOptions,
) -> (Simulator, Vec<SimClient>, IggyNamespace) {
    let client_ids: Vec<u128> = (1..=u128::from(clients)).collect();
    // A bounded journal is what makes a checkpoint happen, and a checkpoint needs a
    // data directory for the coordinator to write into. Neither exists on the plain
    // constructors, which is why an unbounded run never reaches WAL drain,
    // `RangeEvicted` or metadata state transfer. The directory is leaked
    // deliberately: it holds the snapshots a failing run is diagnosed from, and the
    // run's own seed names it.
    let mut sim = match args.journal_slots {
        Some(slots) => {
            let root = std::env::temp_dir().join(format!("iggy-workload-fuzz-{args_seed:#x}"));
            std::fs::create_dir_all(&root).expect("fuzz data directory must be creatable");
            println!(
                "checkpointing enabled: journal_slots={slots} data_dir={}",
                root.display()
            );
            let sim = Simulator::with_checkpoints(
                usize::from(replicas),
                client_ids.iter().copied(),
                network_opts,
                args.shell,
                &root,
            );
            sim.set_metadata_journal_slots(slots);
            sim
        }
        None if args.shell => Simulator::with_shards_shell(
            usize::from(replicas),
            1,
            client_ids.iter().copied(),
            network_opts,
        ),
        None => Simulator::new(
            usize::from(replicas),
            client_ids.iter().copied(),
            network_opts,
        ),
    };
    sim.set_restore_partition_frontier(args.restore_partition_frontier);
    let sim_clients: Vec<SimClient> = client_ids.iter().map(|&id| SimClient::new(id)).collect();

    let ns = IggyNamespace::new(1, 1, 0);
    sim.init_partition(ns);
    if args.shell {
        sim.seed_stream_topic_partition(ns);
    }
    for client in &sim_clients {
        if args.shell {
            sim.shell_login(client);
        } else {
            sim.register_client_with_primary(client);
        }
    }
    (sim, sim_clients, ns)
}

/// Which protocol commands the run delivered, and which it never reached.
///
/// The harness wires far more of the command space than any one scenario drives, and
/// "is this path covered?" was previously answered by grepping the source. Counted
/// at delivery, so a command listed here really arrived somewhere.
fn print_command_coverage(sim: &Simulator) {
    let counts = sim.network.command_counts();
    let mut seen: Vec<String> = Vec::new();
    let mut unseen: Vec<&str> = Vec::new();
    for (discriminant, &count) in counts.iter().enumerate() {
        let label = COMMAND_LABELS[discriminant];
        if label == "Reserved" {
            continue;
        }
        if count > 0 {
            seen.push(format!("{label}={count}"));
        } else {
            unseen.push(label);
        }
    }
    println!("commands delivered: {}", seen.join(" "));
    println!("commands never delivered: {}", unseen.join(" "));
}

/// Reply, rejection and resend counters plus per-action commits.
fn print_coverage(workload: &Workload) {
    let stats = workload.auditor.stats();
    println!(
        "coverage: replies_seen={} replies_unknown={} committed_rejections={} \
         samples_none={} resends={} denials={} transients={} evictions={}",
        stats.replies_seen,
        stats.replies_unknown,
        stats.committed_rejections,
        workload.samples_none(),
        workload.resends(),
        stats.denials,
        stats.transient_rejections,
        workload.evictions(),
    );
    for action in Action::iter() {
        let commits = stats.commits(action);
        let (refused, code) = stats.denials_per_action[action as usize];
        let (transients, transient_code) = stats.transient_rejections_per_action[action as usize];
        if commits > 0 || refused > 0 || transients > 0 {
            println!(
                "  {action:?}: {commits} commits, {refused} denied (last status {code}), \
                 {transients} transient (last code {transient_code})"
            );
        }
    }
}
