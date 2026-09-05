# Apache Iggy Simulator

A deterministic harness that runs an Iggy cluster inside a single thread. The replicas are the real `shard::IggyShard`, running the real message pump, VSR consensus, metadata state machine and partitions. Only the environment under them is swapped: clock, journal storage, superblock, bus and network are in-memory doubles, and every task runs on a cooperative executor whose poll order is drawn from the run's seed.

A run is a pure function of that seed. Failures print it, and `--seed <value>` replays the same task interleaving, the same dropped packets, the same crashes, down to the prepare timestamps recorded in the log. Three production consensus bugs have been found this way, each of them reproducible from one seed and a handful of flags.

The crate is `publish = false`: a tool to run against the workspace, not a dependency.

## Quick start

```sh
# 10k ticks of partition-plane traffic, 3 replicas, perfect network
cargo run -p simulator --bin workload-fuzz

# metadata plane against a hostile network with crashes and restarts
cargo run -p simulator --bin workload-fuzz -- \
  --seed 48 --ticks 4000 --plane metadata --faults heavy \
  --crash-prob 0.02 --restart-prob 0.08 --crash-primary

# the properties below, pinned as regression tests
cargo test -p simulator

# scripted demo: send, create and delete a stream, crash a follower, poll
cargo run -p simulator --bin simulator-ui
```

Nothing here needs Docker, a server binary, or a network stack. `core/integration` is where real processes are spawned.

## Real code, simulated environment

| Layer | What runs |
| --- | --- |
| Shard runtime, router, dispatch | `shard::IggyShard`, one `run_message_pump` task per shard |
| Consensus | `consensus::VsrConsensus`, one group for the metadata plane and one per partition namespace |
| Metadata, partitions | the `metadata` STM, single writer on shard 0 with reader mirrors on the peers; `partitions` on the hash-owning shard |
| Client requests | real wire messages built by `SimClient`; with `--shell`, the server's `on_client_request` path including login and session binding |
| Clock | `SimClock` over executor virtual time, epoch pinned at 2026-01-01 |
| Journal, storage | `SimJournal<MemStorage>`, held by the harness so a WAL outlives the replica that wrote it |
| Superblock | `SimSuperblock`; RAM has no torn writes, so it carries none of `PingPongSuperblock`'s framing or checksums |
| Bus and network | `SimOutbox` per replica, drained into `PacketSimulator` |
| Scheduling | `DetExecutor`, uniform seeded pick among ready tasks |
| Snapshots | the real `SnapshotCoordinator` writing real files, only under `Simulator::with_checkpoints` |

The `simulator` feature on `shard`, `metadata`, `partitions` and `server_common` exposes four seams that only this crate uses: `init_partition` (bypasses the reconciler), `seed_single_partition` (bypasses metadata consensus), `hold_borrow_across_await` (feeds the borrow detector a known-bad case), and a fixed-salt password hash so replicated user metadata replays. None has a production caller, and a `-p iggy-server` build excludes all four.

## Determinism

`SimSeeds::derive(seed)` splits the run's seed into one PRNG stream per consumer:

| Stream | Drives | Why it is separate |
| --- | --- | --- |
| `network` | delays, drops, replays, partitions, clogs | a workload change must not move the packet trace |
| `workload` | which action, which entity, which argument | it shared a stream with `network` once, which is why children are drawn from a parent rather than salted off the seed |
| `executor` | task-poll and timer-fire order | shaking out order dependence must not perturb traffic |
| `entry_shard` | which shard receives each inbound packet | one draw per delivered packet, the highest-rate stream |
| `faults` | crash and restart scheduling | both probabilities at zero draws nothing, so a fault-free run replays bit-identically |
| `swarm` | `--faults swarm`'s parameter draw | correlating the loss probability with the loss events would collapse the explored space to a diagonal |

Children are drawn from a parent PRNG in field order, so fields are appended to `SimSeeds`, never inserted: an insertion moves every child after it and re-locks every seeded baseline.

Time never flows on its own. `DetExecutor::advance_time` is the only thing that moves it, sleeps anchor their deadline at creation (matching `compio::time::sleep`, which the pump's re-arm relies on), and timers fire in `(deadline, seq)` order so equal deadlines resolve FIFO. `run_pumps` gives `run_until_stalled` a 100k poll budget; exhausting it means a task is spin-waking, and the panic names the seed and the schedule hash.

## One tick

`Simulator::step()` returns the client replies delivered during it, in four phases:

1. Fire the consensus tick timer (`CONSENSUS_TICK_INTERVAL`: view change, retransmits) and run every pump to quiescence.
2. Deliver ready packets into the shard routers. The receiving shard is a seeded draw and so usually not the owner, which makes the frame take the real router hop, as production does when the coordinator homes a peer connection on an arbitrary shard. Under `--shell` a client packet enters through `deliver_client_request` instead of raw `dispatch`.
3. Run the pumps again over the delivered frames and their loopback follow-ups, then drain each replica's outbox into the network.
4. Advance network time.

Everything a step produces is on the wire before network time moves, so a reply cannot chain into another delivery inside the same tick.

## The workload driver

`workload::run_with_faults` is the loop the fuzzer and most tests share. Per tick it advances the workload clock, steps the fault injector, then resends timed-out requests and handshakes before sampling anything new, since a timed-out request still holds its client's slot. It then samples at most one request per idle client, steps the simulator, classifies the replies, recovers evicted clients, and asserts the per-tick invariants.

Each workload action has a module under `workload/ops/`, 23 of them, one per `Action` variant, exposing the same seven items: `Input`, `Outcome`, `OUTCOMES`, `sample`, `build_message`, `classify_reply`, `predicted_effect`. Dispatch runs through the `op_dispatch!` macro, so an action without a module is a compile error rather than a silently unsampled op. Variant order in `Action` is part of the determinism contract: append, never insert or reorder.

A resend reuses the original encoded message verbatim. Rebuilding it would draw a fresh request id, and the metadata client table dedups on that id: a renumbered retry commits a second time instead of returning the cached reply. Resends move to the next replica, so a client whose primary died finds the new one. Each client holds one request in flight (`CLIENT_REQUEST_QUEUE_MAX`).

## What gets checked

| Check | When | Catches |
| --- | --- | --- |
| `workload::invariants` | every tick | `commit_offset` or consensus `view` regressing on a live `(replica, namespace)`, in-flight requests over the per-client bound |
| `workload::state_checker` | every tick | two replicas holding different history at an op they both committed, both directions of `(commit_a == commit_b) == (checksum_a == checksum_b)`, and a break in the `parent == checksum` hash link |
| `Simulator::assert_inboxes_drained` | every executor quiescence | a frame sitting in an inbox nobody was woken for, which the next `advance_time` would otherwise drain silently |
| `workload::oracle` | after the drain | a live replica ahead of the leader, disagreement on any committed op two replicas share, a namespace in committed metadata with no host or more than one primary, and on a serial run a predicted `Shadow` that differs from the metadata committed on the leader |

Agreement is asserted over the committed prefix, not over equal heads. A replica that missed the last commit broadcast, or rejoined a moment ago, is allowed to trail; what it may not do is hold different history at an op it did commit.

The oracle reports what it actually compared (`ops_compared`, `replicas_compared`, `namespaces_checked`) because a check that ran against an empty chain passes silently.

## Using it from a test

```rust
let seed = 0xC0FF_EE00;
let client_id: u128 = 1;
let network = PacketSimulatorOptions {
    node_count: 3,
    client_count: 1,
    seed,
    ..PacketSimulatorOptions::default()
};
let mut sim = Simulator::new(3, std::iter::once(client_id), network);
let client = SimClient::new(client_id);
let ns = IggyNamespace::new(1, 1, 0);
sim.init_partition(ns);
sim.register_client_with_primary(&client);

let mut options = WorkloadOptions::new(seed, 3, vec![ns]);
options.weights = ActionWeights::new(&[
    (Action::CreateStream, 50),
    (Action::DeleteStream, 25),
    (Action::SendMessages, 25),
]);
let mut wl = Workload::new(options);

let replies = workload::run(&mut sim, &mut wl, &[client], 2_000, u64::MAX);
assert!(replies > 0);
assert!(oracle::drive_to_quiesce(&mut sim, &mut wl, 5_000));
oracle::assert_converged(&sim, &mut wl);
```

`MemoryPool::init_pool` has to run first with pooling disabled, since `PooledBuffer::from` panics on an uninitialised pool.
For the dispatch path use `Simulator::with_shards_shell`, where `shell_login` and `seed_stream_topic_partition` replace `register_client_with_primary`. To assert what was injected rather than trusting the probabilities to have fired, pass a caller-owned `FaultInjector` to `run_with_faults`; `replica_crash` and `replica_restart` drive faults directly.

## workload-fuzz

```text
workload-fuzz: seed=1 ticks=500 clients=1 replicas=3 plane=Partition faults=None shell=false crash_prob=0 quiesce=true
network: loss=0 replay=0 delay=1..3 partition=None/Symmetric p_partition=0 p_unpartition=0 stability=0/0 clog=0 clog_ticks=0 link_capacity=64
ran 500 ticks; 50 replies; crashes=0 restarts=0 still down: 0
coverage: replies_seen=50 replies_unknown=0 committed_rejections=0 samples_none=0 resends=0 denials=2 transients=0 evictions=0
  SendMessages: 28 commits, 0 denied (last status 0), 0 transient (last code 0)
  StoreConsumerOffset: 16 commits, 0 denied (last status 0), 0 transient (last code 0)
  DeleteConsumerOffset: 4 commits, 2 denied (last status 3021), 0 transient (last code 0)
quiesced and converged (leader-relative; entity oracle: held; evictions=0; ops_compared=1 replicas_compared=2 namespaces_checked=1)
coverage: replies_seen=51 replies_unknown=0 committed_rejections=0 samples_none=0 resends=0 denials=2 transients=0 evictions=0
commands delivered: Request=52 Prepare=80 PrepareOk=78 Reply=52 Commit=56 RequestPrepares=3 RepairPrepare=4 RepairDone=3
commands never delivered: Ping Pong PingClient PongClient StartViewChange DoViewChange StartView Eviction ...
workload-fuzz: OK (seed=1)
```

The two banner lines are the run's identity: every network parameter is printed, not the interesting subset, because under `--faults swarm` those values are the run. Coverage prints twice, before the quiesce assert and after the drain, so a failed drain still reports what the run managed to do. The command coverage lines answer "did this path get exercised?" by counting at delivery, so a command listed as delivered was seen on the wire rather than inferred from the source.

An invalid network configuration exits 2 before the simulator starts. Every other failure is a panic, and the hook prints `reproduce with --seed N` ahead of it.

### Flags

| Group | Flags |
| --- | --- |
| Shape | `--replicas` (3), `--clients` (1), `--ticks` (10000), `--plane`, `--ack-quorum-ratio` (0.5), `--shell` |
| Faults | `--faults`, `--crash-prob`, `--restart-prob`, `--crash-primary`, `--min-survivors` (a commit quorum) |
| Network overrides | `--packet-loss-prob`, `--replay-prob`, `--one-way-delay-min`, `--one-way-delay-mean`, `--link-capacity`, `--partition-mode`, `--partition-symmetry`, `--partition-prob`, `--unpartition-prob`, `--partition-stability`, `--unpartition-stability`, `--clog-prob`, `--clog-duration-mean` |
| Checkpointing | `--journal-slots`, `--data-dir`, `--reuse-data-dir` |
| Quiesce | `--no-quiesce`, `--heal-before-quiesce` |
| Gates | `--min-commits` (1), `--min-ops-compared` (1), `--require-entity-oracle`, `--require-faults` |
| Durability study | `--restore-partition-frontier` |

`--plane` picks the op mix: `partition` (writes and consumer offsets), `metadata` (replicated metadata mutations), `mixed` (stream creates over a write-heavy base), `uniform` (every action equally likely, the widest per-tick coverage).

`--faults` picks a whole network profile and the individual network flags override single fields of it.
Severity costs progress, every lost frame paying a resend timeout: on one namespace with one client, a 3-replica cluster drains roughly 440 replies in 5000 ticks on a perfect network, 240 under `light`, 40 under `heavy`.
Budget ticks accordingly instead of reading a low reply count as a stall.
`--faults swarm` derives every network parameter from the seed, which is what a campaign wants: `none`, `light` and `heavy` are three fixed points, so a thousand seeds against `heavy` is the same network a thousand times.

The gates exist because a run that commits nothing compares empty against empty and reports success. `--min-commits` and `--min-ops-compared` default to 1 for that reason; `--require-entity-oracle` additionally fails a run whose entity oracle was disarmed by an eviction and never re-armed.

`--heal-before-quiesce` restarts every crashed replica and stops the network drawing faults before the drain. It is off by default: a drain that fails with a replica still down is how a wedge shows up, and healing first resolves the wedge instead of reporting it.

### Checkpoints

A checkpoint is forced by the metadata WAL running low on slots, so `--journal-slots N` is what makes one happen. Without it the journal is unbounded and WAL drain, `snapshot_op` movement, `RangeEvicted` and metadata state transfer are all unreachable however hard the cluster is driven. `N` must exceed `SnapshotCoordinator::CHECKPOINT_MARGIN`, or every commit checkpoints and the run measures that rather than the workload.

Snapshots are written to a real directory, per process, whose path is printed. It refuses an existing `--data-dir` unless `--reuse-data-dir` says otherwise, because a restart now recovers from `snapshot.bin` and the directory would be input to the new run.

## Reproducing and diagnosing

```sh
RUST_BACKTRACE=1 RUST_LOG=shard=debug,consensus=debug \
  cargo run -p simulator --bin workload-fuzz -- --seed 13 --plane metadata \
  --crash-prob 0.01 --restart-prob 0.05 --ticks 30000
```

Omitting `--seed` draws one and prints it on the banner, so an exploratory run stays replayable. The panic hook replaces the default one, so `RUST_BACKTRACE` is honoured explicitly and only when set; a campaign would otherwise be buried in backtraces. `RUST_LOG` selects the server's own tracing, which is the only record of a request dropped after logging, the shape that wedges a client's in-flight slot.

A failed drain prints every outstanding request with its action, target and attempt count, then one line per replica carrying its commit range, repair barrier, transfer state, primary claim and armed repair session, which usually names the failure without a debugger:

```text
replica 2: live | metadata ... commit=1407..1414 barrier=1359 transferring=false primary=false repair=..1407@0
```

## Tests

The suite lives at the end of `src/lib.rs`, with unit tests next to the modules they cover. Names follow `given_X_when_Y_should_Z` where there is a meaningful given; older behavioural names remain where there is not. Every test that touches replies initialises `MemoryPool` first, as above.

What the suite pins, by example: `workload_replay_is_deterministic` and `multi_shard_replay_is_deterministic` (the seed contract), `committed_metadata_agrees_across_replicas`, `given_advanced_view_when_metadata_replica_restarts_should_recover_view_from_superblock`, `given_superblock_write_fails_when_primary_crashes_should_withhold_votes_and_not_elect`, `checkpointing_cluster_serves_a_chunked_state_transfer`, `shell_detects_partition_borrow_held_across_await`.

Constructors: `Simulator::new` (one shard per replica), `with_shards` (metadata on shard 0, partitions hash-assigned across all shards), `with_shards_shell` (the same plus the real dispatch handlers), `with_checkpoints`. Multi-shard and shell-on-multi-shard are library-only; `workload-fuzz` always builds one shard per replica.

## Not modelled

- Storage faults, beyond two knobs on the superblock. `SimSuperblock::set_fail_writes` and `set_yield_writes` inject a persistent write fault and an fsync-wide suspension point; `MemStorage` under the journal never fails and never tears a write. Partition superblocks are storeless, which leaves partition view recovery untested.
- Segment files. Partition messages live in memory; they survive a restart only because the harness carries `RetainedPartitionState` across the rebuild.
- Partition-plane durability. Production's `load_partition` restores the view alone, so a restarted replica rejoins at op 0, invisible to quorum. `--restore-partition-frontier` looks past that at a system more durable than Iggy is.
- Packet corruption. Packets are delayed, dropped, duplicated, partitioned and clogged, never mangled.
- Multi-shard metadata. Shard 0 owns the only metadata consensus group.
- I/O of any kind except the checkpoint files: no `io_uring`, no sockets, no wall clock.

## Layout

```text
src/
├── lib.rs              Simulator: cluster construction, step(), crash/restart, partition seeding, tests
├── executor/           DetExecutor (seeded cooperative scheduler) + virtual clock
├── packet.rs           PacketSimulator: delay, loss, replay, partitions, clogs, link capacity
├── network.rs          Network: passthrough over PacketSimulator, submit/step/heal
├── bus.rs              SimOutbox: per-replica staging that consensus sends into
├── replica.rs          Replica type alias and new_shard() wiring, mirroring server bootstrap
├── deps.rs             SimClock, MemStorage, SimJournal, SimSuperblock
├── client.rs           SimClient: builds real wire requests
├── seeds.rs            SimSeeds: one PRNG stream per consumer
├── ready_queue.rs      min-heap with reservoir-sampled random ready removal
├── workload/           driver, op modules, shadow, auditor, invariants, state checker, oracle
└── bin/
    ├── workload-fuzz.rs    the fuzzer
    └── simulator-ui.rs     scripted demo run
```

## CI

A change under `core/simulator/**` marks the `rust-simulator` component in `.github/config/components.yml`, which scopes the Rust test tasks to it. The component is split out of `rust-cluster` so simulator-only changes do not trigger the foreign SDK suites. No fuzzing campaign runs in CI today; `workload-fuzz` is driven by hand, and a seed that fails should arrive as a test or as a finding write-up.

Commits touching this crate take the `simulator` scope: `fix(simulator): ...`.
