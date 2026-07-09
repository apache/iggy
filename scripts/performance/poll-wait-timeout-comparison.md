# Poll Wait Timeout Benchmark Comparison

This runbook compares immediate `PollMessages` polling with deferred polling.

## Goal

Compare immediate polling with deferred polling on sparse, busy-loop, consumer-group, and saturated workloads. The script records `iggy-bench` throughput and latency, plus OS-level CPU/RSS samples and network byte deltas for each run.

## What To Compare

- Immediate polling: `--poll-wait-timeout 0s`
- Deferred polling: `--poll-wait-timeout 10ms`, `100ms`, `1s`
- Busy-loop comparison: `0s` vs `100ms`
- Transports: run `tcp` first, then repeat with `websocket`
- Sparse workloads: low producer rate, small batches
- Saturated control: large batches, no rate limit
- Resource signals: `iggy-server` CPU/RSS, `iggy-bench` CPU/RSS, network bytes before/after each run

## Run

Quick TCP smoke, about 2-3 minutes on a warm release build:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --quick --transport tcp
```

Full TCP run:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --transport tcp
```

Full WebSocket run:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --transport websocket
```

Use existing release binaries when you already built them elsewhere:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh \
  --quick \
  --skip-build \
  --bench-cmd /Users/aruns/Developer/iggy/target/release/iggy-bench \
  --server-cmd /Users/aruns/Developer/iggy/target/release/iggy-server \
  --identifier aruns-mbp-pr3605
```

Change OS sample interval:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --sample-interval 0.5
```

Disable OS sampling if you only want `iggy-bench` throughput and latency:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --no-resource-sampling
```

Print commands without running:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --dry-run
```

## Workloads

The script runs:

- `sparse_single`: one producer and one consumer, `1` message per batch, `10KB/s` rate limit
- `busy_loop_single`: one producer and one consumer, `1` message per batch, `5KB/s` rate limit, `0s` vs `100ms`
- `sparse_consumer_group`: one producer and four consumers over four partitions, `1` message per batch, `10KB/s` rate limit
- `saturated_control`: one producer and one consumer, `1000` messages per batch, no rate limit, `0s` vs `100ms`

## Resource Logs

Each run writes a benchmark log and, when resource sampling is enabled, a matching resource CSV:

```text
performance_results/poll_wait_timeout_comparison/<suite>_wait_<timeout>.log
performance_results/poll_wait_timeout_comparison/<suite>_wait_<timeout>_resources.csv
```

The CSV format is:

```text
timestamp,role,pid,cpu_percent,rss_kb
```

The benchmark log appends:

- average and max CPU for `server` and `bench`
- max RSS for `server` and `bench`
- network byte delta for the run

## Interpretation Caveats

- Local TCP and WebSocket runs usually travel over loopback, so network bytes are useful for relative comparison, not external NIC throughput.
- OS CPU sampling is interval-based and process-level; use it to compare `0s` vs deferred polling trends, not as a microbenchmark-grade profiler.
- Busy-loop value should show up most clearly in CPU and network/request churn, especially when producer rate is sparse.
- Saturated control protects against regression when messages are already readable; deferred polling should stay close to immediate polling for throughput and p99 latency.
- Repeat quick smoke before sharing numbers; use full TCP/WebSocket runs for final PR discussion data.

## Success Criteria

- Deferred sparse runs should not lose messages.
- Sparse workloads should keep expected low-rate throughput while showing bounded wait-path latency.
- Busy-loop deferred runs should reduce CPU/network churn relative to `0s` immediate polling.
- Saturated control with non-zero timeout should be close to immediate polling for throughput and p99 latency.
- WebSocket comparison should behave consistently with TCP.
- Resource claims should cite the generated CSV/log data and avoid overclaiming precision.

## Discord Update Template

```text
I extended the #3605 benchmark runner to cover regular polling vs deferred polling in busy-loop/sparse workloads, plus OS-level CPU/RSS samples and network byte deltas per run. I’ll run TCP first, then repeat WebSocket, and share the 0s vs 100ms comparison with throughput, p99, CPU, RSS, and network deltas.
```
