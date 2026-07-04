# Poll Wait Timeout Benchmark Comparison

This runbook compares immediate `PollMessages` polling with deferred polling.

## Goal

Compare immediate polling with deferred polling on sparse and saturated workloads. The script records `iggy-bench` throughput and latency; use OS-level sampling or future poll-count metrics for direct CPU/request-churn numbers.

## What To Compare

- Immediate polling: `--poll-wait-timeout 0s`
- Deferred polling: `--poll-wait-timeout 10ms`, `100ms`, `1s`
- Transports: start with `tcp`; optionally repeat with `websocket`
- Sparse workloads: low producer rate, small batches
- Saturated control: large batches, no rate limit

## Run

Quick local smoke, about 2-3 minutes on a warm release build:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --quick
```

Full local run:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh
```

WebSocket-facing comparison:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --transport websocket
```

Print commands without running:

```bash
scripts/performance/run-poll-wait-timeout-comparison.sh --dry-run
```

## Workloads

The script runs:

- `sparse_single`: one producer and one consumer, `1` message per batch, `10KB/s` rate limit
- `sparse_consumer_group`: one producer and four consumers over four partitions, `1` message per batch, `10KB/s` rate limit
- `saturated_control`: one producer and one consumer, `1000` messages per batch, no rate limit

## Success Criteria

- Deferred sparse runs should not lose messages.
- Sparse workloads should keep expected low-rate throughput while showing bounded wait-path latency.
- Direct CPU/request-churn claims should be backed by OS-level sampling or explicit poll-count metrics.
- Saturated control with non-zero timeout should be close to immediate polling for throughput and p99 latency.
- WebSocket comparison should behave consistently with TCP.

## Discord Update Template

```text
I added a focused benchmark comparison runner for #3605:
scripts/performance/run-poll-wait-timeout-comparison.sh

It compares --poll-wait-timeout 0s vs 10ms/100ms/1s on sparse single-consumer, sparse consumer-group, and saturated control workloads. I’ll share the result table after the TCP quick/full run, and can repeat on WebSocket if useful.
```
