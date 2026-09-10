# Poll completion experiments

`poll_completion.py` compares the current poll path with completion owned by the
partition. It runs one consumer against one shard over TCP, retains raw poll
observations, and compares independent pairs of complete runs. It does not test
the purge correctness contract; run the regression tests separately.

## Build and environment

Build baseline and candidate servers with the same Rust 1.98.0 toolchain, lockfile,
release profile, and features. Record source revisions and any compatibility patches.
Build one `iggy-bench` binary and one `iggy` CLI binary from the harness revision for
each platform. Reuse those exact client binaries against both servers. The runner
records binary hashes; keep Linux and native macOS results and comparisons separate.

The Linux experiment uses this pinned image:

```text
rust@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922
```

Build dependencies include `libhwloc-dev` and `libudev-dev`; the runner needs Python 3
and `taskset`. Put sources used for builds, binaries, results, and fixture data in a
Docker named volume. A bind mount of the repository is useful for copying source,
but keep the measured fixture on the volume. Example container setup from the repo:

```sh
docker volume create iggy-poll-lab
docker run --name iggy-poll-lab --cpuset-cpus=0-3 --memory=8g \
  --security-opt "seccomp=$PWD/performance_results/owner-poll-20260909/environment/io-uring-seccomp.json" \
  --mount type=volume,source=iggy-poll-lab,target=/work \
  --mount "type=bind,source=$PWD,target=/source,readonly" \
  -it rust@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 bash
```

Retain the actual seccomp profile with environment records. This profile permits the
required `io_uring` operations; replacing it with unrestricted seccomp changes the
recorded setup. The runner pins the Linux server to guest CPU 0 and clients to CPUs
1 and 2. The container permits CPUs 0 through 3 and has an 8 GiB ceiling. In this
session Docker's VM has 10 guest CPUs and approximately 8 GiB shared memory; the
container limit does not reserve that memory or dedicate host CPUs to the experiment.

For macOS, use native builds and a separate local results directory. The runner
uses one shard and two Tokio worker threads without CPU affinity. It reads process
CPU and RSS through `libproc`; Linux uses `/proc`. Native macOS exercises its native
I/O backend, so its results are not measurements of Linux `io_uring`.

## Workloads and pilots

| Case | Messages per poll | Polling | Additional work |
| --- | ---: | --- | --- |
| `resident` | 1 | Next | Journal remains resident |
| `disk-offset` | 1 | Offset | No automatic commit |
| `disk-next` | 1 | Next | Automatic commit |
| `disk-batch` | 100 | Next | Automatic commit |
| `disk-group` | 100 | Next | One consumer group |
| `disk-writes` | 100 | Next | Concurrent producer at a fixed target rate |
| `disk-fsync` | 100 | Next | Concurrent producer with fsync enforced |

Every message has a 256 byte payload. The runner warms the path on a separate
fixture, deletes it, and creates the measured fixture. The benchmark's own warmup
is disabled so it does not consume measured messages. Disk cases flush each produced batch through the topic's count threshold of one.
The server stays running, retaining the read indexes built during preload;
the OS file cache may remain hot. These cases establish use of the disk read
path, not physical storage latency. Verify routing with a separate diagnostic run.

Use absolute binary and result paths because child processes run from a fixture
directory. For example, inside the Linux container:

```sh
POLL_BIN_DIR=/work/bin/linux
POLL_RESULTS=/work/results/linux
mkdir -p "$POLL_RESULTS"
python3 /work/source/scripts/benchmarks/poll_completion.py run \
  --server "$POLL_BIN_DIR/baseline-server" \
  --client "$POLL_BIN_DIR/iggy-bench" --cli "$POLL_BIN_DIR/iggy" \
  --case disk-next --polls 10000 --label baseline \
  --output "$POLL_RESULTS/pilot-disk-next"
```

The example poll count is a starting point, not a calibrated duration. Use pilots
to check completion counts, empty responses, runtime, cache routing, and resource
headroom. Choose enough work to resolve the effects of interest and make the 50 ms
CPU sampling and 100 ms producer sampling useful. Repeat the baseline against itself
with the paired procedure below to estimate the noise floor before interpreting
small changes. Keep builds and unrelated workloads out of measured runs.

Calibrate the baseline producer separately for both background cases:

```sh
python3 /work/source/scripts/benchmarks/poll_completion.py run \
  --server "$POLL_BIN_DIR/baseline-server" \
  --client "$POLL_BIN_DIR/iggy-bench" --cli "$POLL_BIN_DIR/iggy" \
  --case disk-writes --calibrate-producer --polls 10000 --label baseline \
  --output "$POLL_RESULTS/calibration-disk-writes"
```

Repeat with `disk-fsync`. Choose and record the target rate from those observations;
use the same target for both versions within each case. An individual mixed pilot
also requires `--background-rate` in payload bytes per second.

## Freeze and compare

Freeze one workload JSON file per platform after the pilots. Keys are case names;
each value has `polls` and, for background cases, `background_rate`. This is a minimal
example for two cases, not the complete calibrated workload:

```json
{
  "resident": {"polls": 10000},
  "disk-next": {"polls": 10000}
}
```

For the full suite include all seven case keys and their calibrated counts and
rates. Keep the frozen file with results. `schedule` creates counterbalanced order
within each case; `matrix` executes it serially and refuses existing run directories.
For the two cases above:

```sh
python3 /work/source/scripts/benchmarks/poll_completion.py schedule \
  --cases resident disk-next --pairs 10 --seed 20260909 \
  --output "$POLL_RESULTS/schedule.json"
python3 /work/source/scripts/benchmarks/poll_completion.py matrix \
  --baseline "$POLL_BIN_DIR/baseline-server" \
  --candidate "$POLL_BIN_DIR/candidate-server" \
  --client "$POLL_BIN_DIR/iggy-bench" --cli "$POLL_BIN_DIR/iggy" \
  --schedule "$POLL_RESULTS/schedule.json" --workload "$POLL_RESULTS/workload.json" \
  --results "$POLL_RESULTS/runs"
python3 /work/source/scripts/benchmarks/poll_completion.py compare \
  --schedule "$POLL_RESULTS/schedule.json" --results "$POLL_RESULTS/runs" \
  --output "$POLL_RESULTS/comparison.json"
```

For the baseline comparison with itself, pass the baseline server as both server
arguments and use separate result directories. For macOS, repeat this workflow with
native binaries, native paths, and its own frozen workload and schedule.

The analysis pools successful nonempty raw polls before calculating each run's
percentiles. It bootstraps independent paired run ratios, not individual polls.
Throughput ratios are candidate divided by baseline, with higher values better;
latency ratios use the same direction, with lower values better. The current
comparison checks a throughput ratio of at least 0.97 and a p99 ratio of at most
1.05 with one-sided 95% bootstrap bounds and at least ten pairs. Report absolute
values, intervals, and the noise pilot alongside those conditional labels.

Background throughput is a separate workload guardrail. Report the frozen target,
achieved payload bytes per second for each version over the consumer window, and
the candidate to baseline ratio. If changed producer work could explain a poll
improvement, leave that conclusion inconclusive. Any numeric tolerance needs to be
chosen before confirmation from baseline variation and the intended workload;
the analysis does not silently impose a new threshold for this guardrail.

## Diagnostics and retained evidence

Build a separate diagnostic server with `--features poll-diagnostics`, and run it
with `RUST_LOG=iggy.shard.poll_diagnostics=debug`. Logs identify resident and disk
dispatch and the time disk completion waited for the shard. Use this to confirm the
mechanism and fixture routing. Disable the feature and diagnostic logging for the
performance comparison so tracing does not become part of the measured cost.

Each run retains commands, binary hashes, server logs, `polls.csv`,
`run-manifest.json`, producer samples, process samples, and a summary. CPU counters
are interpolated from 50 ms samples to the consumer measurement window. Producer
counters use approximately 100 ms samples over that same window. Those are coarse
resource guardrails; neither interpolation creates finer measurement precision.
Failed and interrupted runs retain their evidence. Fixture data is removed by
default; use `run --keep-fixture` when diagnosing a failure. Keep failed runs and
any exclusions visible rather than replacing them in place.

Validate analysis without running a server:

```sh
python3 -B -m unittest discover -s scripts/benchmarks -p test_poll_completion.py
```
