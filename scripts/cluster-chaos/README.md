# Cluster chaos harness

Reproduces multi-node failure cases on one machine: a 3-node VSR cluster in Docker, built from a published
`apache/iggy` image, takes produce load while nodes are killed, restarted, isolated, frozen or all lost at once.
Afterwards every node is healed and a checker reads the whole topic back and compares it with what the producers
were told was acknowledged.

Nothing here builds the server. Each node is a container with a fixed IP and its data in a named volume, so a
`kill` followed by `start` is a real process crash and a recovery from disk.

## Layout

Paths are relative to the repository root.

| Path | What it is |
| --- | --- |
| `scripts/cluster-chaos/cluster.sh` | Docker backend (below) |
| `scripts/cluster-chaos/run-scenario.sh` | Runs one scenario on a fresh cluster and writes a result JSON |
| `scripts/cluster-chaos/Dockerfile` | Image of the checker, built from this checkout |
| `scripts/cluster-chaos/k8s/kind-cluster.sh` | The same backend commands on a local kind cluster, using `helm/charts/iggy` in cluster mode |
| `core/tools/src/cluster-chaos-checker` | The checker (`setup`, `produce`, `verify`), on the in-repo `iggy` SDK |

A backend is a script with these commands, which `run-scenario.sh` calls through `CLUSTER`: `up`, `down`,
`addresses`, `checker NAME ARGS...`, and per node `kill`, `stop`, `start`, `isolate`, `heal`, `pause`, `unpause`,
`running` and `logs`.

## Quick start

Requirements: Docker, `jq`, and about 5 CPUs and 8 GiB of memory for the defaults (three nodes at 1 CPU and 2 GiB,
the checker at 2 CPUs and 2 GiB).

```bash
# From the repository root. The checker uses the SDK of this checkout, so build it from the
# same commit as the server image you test.
docker build -f scripts/cluster-chaos/Dockerfile -t iggy-cluster-chaos-checker .

scripts/cluster-chaos/run-scenario.sh baseline
scripts/cluster-chaos/run-scenario.sh crash-restart
```

`IMAGE` picks the server image (default `apache/iggy:edge`, built from `master`). Its source commit is in the
image labels:

```bash
docker inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' apache/iggy:edge
```

To test a local change, build a server image from the same checkout, either fully in Docker
(`docker build -f core/server/Dockerfile -t iggy-server:dev .`) or from prebuilt binaries with the
`runtime-prebuilt` target, as `bdd/docker-compose.cluster.yml` does, then run with `IMAGE=iggy-server:dev`.

Each run prints a verdict and writes `<results-dir>/<scenario>-<timestamp>.json` (default results directory:
`${TMPDIR:-/tmp}/cluster-chaos-results`). The exit status is 0 on a pass and 1 on a failure. On a failure, or when
the run stops early, the work directory is kept and printed: produce logs (`attempted-*.jsonl`, `acked-*.jsonl`),
checker output and every node's log.

## What is checked

`checker produce` runs several producers, each with its own session, writing events keyed by symbol (one key always
maps to one partition). Every event goes to `attempted-*.jsonl` before its first send and to `acked-*.jsonl` once a
send carrying it succeeded. A failed send is retried with the same events until it is acknowledged, so the
acknowledged log is exactly what the cluster promised to keep. A producer gives up, and the run fails, when one send
keeps failing for 180 s.

`checker verify` reads every partition from offset 0. It reads in rounds and stops only after three rounds in a row
found nothing new and had no failed poll, because right after a recovery neither an empty poll nor the partition end
in `GetTopic` proves the end of the log. If reads have not settled after 30 rounds, the run fails.

`verify` reads through one session, so it sees what the node serving that session returns. It does not compare the
replicas with each other.

A scenario fails on any of:

| Check | Meaning |
| --- | --- |
| `lost_acked` | an acknowledged event is not in the log |
| `read_incomplete` | reads did not settle: polls kept failing or data kept arriving |
| `unreadable_tail` | `GetTopic` lists offsets that no read returns |
| `offset_gaps` | a poll skipped offsets |
| `reordered` | an event is before an earlier event of the same key |
| `misplaced` | a key appears in two partitions |
| `phantoms` | an event nobody sent with this content, a damaged payload, or a user header that does not match the payload |
| producers gave up | a send kept failing for 180 s |
| node fatal errors | `panicked at` or `shard thread returned error` in a node log, or a node not running after the heal |

Reported but not failed:

| Field | Meaning |
| --- | --- |
| `duplicates` | events in the log more than once. The server has no deduplication, so a retry of a send whose reply was lost appends the events again. Expected after faults. |
| `unacked_but_present` | events whose send never got an acknowledgement but that were committed anyway |
| `empty_polls_mid_log` | polls that returned no messages at an offset below the partition's final read end, that is, data that was there but polled empty (with a sample of up to 20 in `empty_polls_mid_log_sample`) |
| `topic_stats_mismatches` | `GetTopic` answers that disagree with what was read. `verify` asks each node on its own session (`topic_stats.nodes`) for the topic's and each partition's `messages_count`, `size_bytes` and `current_offset`, next to the read end per partition and the total read |
| `state_transfer_refusals` | node log lines containing `needs state transfer` (the signature of #4438). They also appear transiently in passing runs |

## Scenarios

Defaults: 6 partitions, `replicated` durability, 2000 events/s from 6 producers in batches of 20 for 120 s.
Every value can be overridden through the environment (see below).

| Scenario | Faults during the load | Expected on a correct cluster |
| --- | --- | --- |
| `baseline` | none | pass |
| `control` | none; 20% of sends resend earlier acknowledged events | pass, and `duplicates` at least the number of resent events. Shows the verifier sees every copy |
| `crash-restart` | SIGKILL each node in turn (`KILL_GAP` 15 s apart), restart it from disk after `DOWN_SECONDS` (15 s) | pass |
| `rolling-restart` | graceful stop and start of each node in turn | pass |
| `isolate` | disconnect a node from the network (peers and clients) for 20 s, twice | pass |
| `pause` | freeze a node (cgroup freezer) for 20 s, twice | pass |
| `kill-storm` | four kill/restart cycles 12 s apart, 8 s down each | pass |
| `cascading-crash` | kill a node, kill a second one 6 s later (during the view change), restart both; three times. `persisted` | pass |
| `full-outage` | SIGKILL all three nodes at once, restart after 15 s. `persisted` | pass |
| `full-outage-replicated` | the same with `replicated` durability | data checks pass; `lost_acked` may be above 0 (by design, below) |
| `rolling-crash` | the #4438 reproduction: 1 partition, 30k events/s, `persisted`, a node SIGKILLed every 80 s and down 40 s | pass |
| `split-primaries` | `crash-restart`, then a `MEASURE_SECONDS` (60 s) produce phase with no faults | pass, with send p99 under `P99_LIMIT_MS` (1000) after the restarts |

`isolate` is Docker-only. The other scenarios also run on the kind backend, where a kill is a pod force-delete
(see below).

Resends (`RESEND_RATE`) put a second copy of an acknowledged event into the log, and that copy counts as present. Keep
them to `control`, where nothing fails, so they cannot hide a loss.

## Known issues and how to reproduce them

### Lagging replica stuck after rolling crash-restarts (#4438)

```bash
NODE_CPUS=2 NODE_MEMORY=4g NODE_SHARDS=2 CHECKER_CPUS=4 CHECKER_MEMORY=4g \
  scripts/cluster-chaos/run-scenario.sh rolling-crash
```

A restarted replica's durable state ends below the repair window its peers retain. It logs
`refusing commit floor: repaired window does not connect to recovered durable state (needs state transfer)`, never
completes the state transfer, and keeps serving its stale prefix. **Buggy:** the run fails with `lost_acked` and
`unreadable_tail` in the millions (2,283,600 of 4,570,200 acknowledged events unreadable in one run) and
`state_transfer_refusals` in the hundreds; restarting only the lagging node makes every event readable again.
**Expected:** pass. It is timing dependent: on `master` at `f6dfbdbb3` it failed 1 of 2 runs. The failing tests are
in #4439. Each run produces about 11 million events and needs roughly 1.5 GB for the produce logs in `TMPDIR`.

### Sends thrash once partition primaries diverge (#4436)

```bash
scripts/cluster-chaos/run-scenario.sh split-primaries
```

Each partition elects its own primary. After failovers the primaries can end up on different nodes, and the Rust SDK
then moves its single session on nearly every send, about 4 s each. **Buggy:** the measure phase after the restarts
shows multi-second p99 send latency and a fraction of the offered rate (#4436 has numbers from a Kubernetes
deployment: 11,984 messages/s at a 4,054 ms p99 on 6 partitions, against 45,830 messages/s on 1). **Expected:**
p99 in milliseconds. Whether the primaries split depends on timing: in Docker at the default load, every partition
group followed the killed node to the same new primary and the scenario passed. The deterministic reproduction is the
integration test in #4437.

### Acknowledged data lost on crash-restart with #4433 applied

Build a server image from that pull request and run
`DURABILITY=persisted scripts/cluster-chaos/run-scenario.sh crash-restart` against it. **Buggy:** every acknowledged
event is lost and every partition polls empty; in
[this comment](https://github.com/apache/iggy/pull/4433#issuecomment-6044243711) `master` lost 0 of 154,180
acknowledged events with the same schedule and `master` with #4433 lost all of them in both runs. **Expected:**
pass, with `lost_acked` 0.

### Stale `GetTopic` stats and empty polls after recovery

Both have been seen after staggered crash-restarts under load: `GetTopic` reported 0 messages and 0 B for a topic
whose partitions served about 6 million messages, and a poll returned no messages at an offset that held data, which a
later poll returned. The verifier tolerates both and reports them as `topic_stats_mismatches` (with every node's
answer in `topic_stats`) and `empty_polls_mid_log`. A consumer that stops at the first empty poll stops early.

### `replicated` durability loses unflushed acknowledged writes in a full outage (by design)

With `replicated`, an acknowledgement means a quorum holds the write, without a stable-storage barrier. Losing every
node at once loses what was not flushed yet: a `full-outage-replicated` run against `apache/iggy:edge` lost 38,980 of
239,480 acknowledged events, while `full-outage` (`persisted`) lost none.

Two of three nodes crashing within the flush window loses data the same way
(`DURABILITY=replicated scripts/cluster-chaos/run-scenario.sh cascading-crash`). In two runs it lost 51,860 of
196,460 and 66,120 of 143,520 acknowledged events. In both runs a restarted replica then stopped with
`partition N could not commit op M, which the cluster had already committed. The replica is divergent and was fenced`
and stayed down. Sends to some partitions were refused on the two remaining nodes until the producers gave up after
180 s. With `persisted` the same scenario passes.

## Environment

| Variable | Default | Used by |
| --- | --- | --- |
| `IMAGE` | `apache/iggy:edge` | server image |
| `CHECKER_IMAGE` | `iggy-cluster-chaos-checker` | checker image |
| `PARTITIONS` `DURABILITY` | 6, `replicated` | topic |
| `RATE` `PRODUCERS` `BATCH` `SECONDS_RUN` `RESEND_RATE` | 2000, 6, 20, 120, 0 | load |
| `KILL_GAP` `DOWN_SECONDS` `SECOND_KILL_DELAY` | 15, 15, 6 | kill timing |
| `MEASURE_SECONDS` `P99_LIMIT_MS` | 60, 1000 | `split-primaries` |
| `NODE_CPUS` `NODE_MEMORY` `NODE_SHARDS` `NODE_MEMORY_POOL` | 1, `2g`, 1, `512MiB` | per node |
| `CHECKER_CPUS` `CHECKER_MEMORY` | 2, `2g` | checker container (Docker only) |
| `RUST_LOG` | `info` | server log filter (Docker only) |
| `CHAOS_PREFIX` `CHAOS_SUBNET` | `iggy-chaos`, `172.31.250` | names and network, to run several clusters at once (Docker only) |
| `KIND_CLUSTER` `KIND_IMAGE` | `iggy-chaos`, `kindest/node:v1.35.0` | kind backend |
| `KEEP` `KEEP_WORK` | unset | keep the cluster up, keep the work directory on a pass |
| `CLUSTER` | `cluster.sh` | backend script |

`cluster.sh` also works on its own for manual experiments:

```bash
scripts/cluster-chaos/cluster.sh up
scripts/cluster-chaos/cluster.sh kill 1
scripts/cluster-chaos/cluster.sh start 1
scripts/cluster-chaos/cluster.sh logs 1 | grep -i 'view'
scripts/cluster-chaos/cluster.sh down
```

## Kubernetes (kind)

`k8s/kind-cluster.sh` runs the same cluster on a local kind cluster with one worker per Iggy node. Each node is a
release of `helm/charts/iggy` with `examples/cluster-3-node.yaml` and its roster pointed at the worker IPs, with
`hostNetwork` and a node-local volume, as the chart's cluster mode requires. The checker runs in a pod on the
control-plane node.

```bash
export CLUSTER=scripts/cluster-chaos/k8s/kind-cluster.sh
"$CLUSTER" create                      # kind cluster "iggy-chaos": 1 control plane + 3 workers
scripts/cluster-chaos/run-scenario.sh crash-restart
"$CLUSTER" destroy
```

It needs `kind`, `kubectl` and `helm`, and the server and checker images present locally (`up` loads both into the
kind nodes). The chart values it generates render with `helm template` and pass schema validation, but the backend
has not been run end to end yet. Differences from the Docker backend:

- `kill` scales the node's Deployment to zero and force-deletes its pod (`kubectl delete pod --force --grace-period
  0`); `start` scales it back to one. The kubelet still sends SIGTERM before it kills the container, so for an
  abrupt crash use the Docker backend.
- `pause` freezes the whole kind node container, kubelet included.
- `isolate` is not supported.
- `logs` has the current pod's log and, after a container restart, the previous one. A force-deleted pod's log is
  gone. `running` fails once a server container restarted on its own.
- Only the produce and verify summaries are copied back from the checker pod. The produce logs stay in the pod.
