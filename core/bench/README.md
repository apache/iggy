# Apache Iggy Bench CLI

The interactive Bench CLI allows you to perform various benchmarking on the Apache Iggy server.

Iggy Bench CLI can be installed with `cargo install iggy-bench` and then simply accessed by typing `iggy-bench` in your terminal.

![CLI](../../assets/bench.png)

## Poll measurements

The low level consumer API supports two optional global selectors:

| Selector | Values | Effect |
| --- | --- | --- |
| `--polling-kind` | `offset`, `next` | Explicit offsets disable automatic commit; Next enables it. |
| `--latency-kind` | `poll`, `origin-timestamp` | Time the SDK poll call or the age of the first returned message. |

Without these selectors, pinned consumers use explicit offsets, groups use Next,
consumer benchmarks measure the poll call, and benchmarks with concurrent producers
measure message age. Consumer rate amplification remains independent of the latency
selector. Explicit selectors require the low level API and a benchmark with consumers.

For example, after populating `bench-stream-1` and `topic-1`:

```sh
cargo run --release --bin iggy-bench -- \
  --messages-per-batch 1 --message-batches 10000 --warmup-time 0s \
  --polling-kind next --latency-kind poll \
  pinned-consumer --streams 1 --consumers 1 tcp --nodelay \
  output --output-dir performance_results
```

When output is enabled, three additional files appear beside `bench.log`:

- `polls.csv` retains every SDK poll call after warmup, including empty responses,
  errors, structured SDK timeouts, and an interrupted call. Columns are `actor_id`,
  `sequence`, `elapsed_us`, `poll_latency_us`, `origin_latency_us`, `partition_id`,
  `messages`, `payload_bytes`, `outcome`, and `error_code`. Missing values are empty
  CSV fields. Outcomes are `messages`, `empty`, `error`, `timeout`, and `cancelled`.
- `run-manifest.json` records resolved selectors, workload options, run status,
  outcome counters, and actor measurement start times in Unix microseconds. Actor
  durations and CSV elapsed times use a monotonic clock relative to that actor's
  measurement start. Authentication secrets are omitted.
- `producer-samples.csv` retains cumulative producer counters about every 100 ms
  after successful sends, plus initial and final samples. Its columns are `actor_id`,
  `elapsed_us`, `completed_batches`, `completed_messages`, and `payload_bytes`.
  Producer start times and final counters also appear in the manifest, including
  when a background producer is interrupted.

Poll samples are buffered in each actor and written after measurement. Capturing
samples adds memory use proportional to the number of polls, including empty polls.
Ctrl-C stops and joins actors before exporting partial results. Failed and interrupted
runs retain their artifacts. Explicit selector values distinguish output directory
names and report identifiers; the report JSON schema is unchanged.

Poll timings include the SDK and transport, including retries inside the SDK call.
They exclude later benchmark accounting. These options do not force a resident or
disk read path; prepare the fixture and verify its read path separately. Aggregate
report percentiles still average actor percentiles. Pool `polls.csv` observations
before calculating percentiles when a combined distribution is required.
