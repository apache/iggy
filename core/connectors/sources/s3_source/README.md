# S3 source connector

Reads uncompressed objects from Amazon S3 or an S3-compatible service into Iggy.
Each nonempty delimiter-separated record becomes one raw message. The source
never writes, deletes, tags, or moves S3 objects.

## Configuration

See [config.toml](config.toml) and the
[runtime example](../../runtime/example_config/connectors/s3_source.toml).
Configure the destination stream with `schema = "raw"` and no payload-changing
transforms to preserve bytes.

| Plugin field | Default | Meaning |
| --- | --- | --- |
| `bucket` | Required | General-purpose bucket to read |
| `prefix` | Omitted | Match full keys beginning with this prefix, including nested paths |
| `region` | AWS discovery | Optional region override |
| `endpoint` | AWS default | Optional S3-compatible service URL |
| `path_style` | True with an explicit endpoint, otherwise false | Addressing style |
| `poll_interval` | `"5s"` | Minimum delay before each poll |
| `custom_delimiter` | `"\n"` | Exact literal bytes, 1–64 UTF-8 bytes |
| `max_record_bytes` | 1 MiB | Maximum payload size of one record |
| `max_batch_bytes` | 4 MiB | Maximum total record payload bytes per poll |
| `max_batch_messages` | 1,000 | Maximum records per poll |
| `verbose_logging` | False | Log batch details at info instead of debug |

Unknown plugin fields are rejected to catch configuration typos. Explicit region
and endpoint values must be nonempty. An empty prefix is treated as omitted;
nonempty prefixes are preserved exactly, including whitespace.

Credentials come from the AWS SDK's default provider chain: environment,
shared AWS configuration, workload roles, and the other providers supported by
the SDK. There are no access-key fields in plugin configuration. For local
Floci tests, the runtime process receives `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`; production deployments should use workload roles
where available.

The identity needs `s3:ListBucket` for the bucket and `s3:GetObject` for selected
keys, plus any permissions required by their encryption. Without a restored
active object, startup checks listing and issues a conditional one-byte ranged GET
for the first listed object. Empty objects use a full zero-byte GET. The probe
consumes its bounded response without advancing progress. A failed probe
prevents startup. An empty prefix cannot prove read access, and a successful
probe does not prove access to every key.

With a restored active object, startup validates the checkpoint and defers S3
access to polling. Missing objects and access failures then use the polling retry
backoff, allowing recovery without another connector restart.

## Record semantics

The default delimiter is exactly LF. With LF, `a\r\n` produces payload
`a\r`; explicitly configure `"\r\n"` to strip CRLF. There is no newline
normalization, trimming, UTF-8 decoding, decompression, or JSON validation.

Empty records are omitted, but whitespace-only records, BOMs, malformed JSON,
and invalid UTF-8 bytes are preserved. The last nonempty record is emitted at
clean EOF even without a trailing delimiter. Delimiter matching is not aware
of quotes or escapes: producers must choose a delimiter absent from payloads.

## Finite scan and restart

The source lists one key at a time in lexicographical order using
`ListObjectsV2`, the configured prefix, and `StartAfter`. Completion ACK
advances the in-memory cursor. Once an empty listing is reached, the source
stays exhausted for that instance's lifetime. It does not periodically rescan.
Listings request URL-encoded keys and decode them once before use, so special
characters do not change object identity or break XML responses.
Idle polls keep the ACK/NACK handshake but omit unchanged checkpoints to avoid
repeated state writes. Empty objects and skipped empty records still checkpoint
any new progress.

Each object requires a separate LIST round trip, and polls process at most one
object. For objects that fit in one batch, throughput is bounded by one object
per poll interval plus LIST, GET, and delivery/checkpoint latency. The default
five-second interval therefore allows fewer than 0.2 objects per second.
Lowering the interval helps small-object scans, but sequential request latency
still limits throughput. Larger listing pages and parallel object reads are not
implemented.

Use a stable, finite prefix. Keys inserted or replaced at or before the cursor
can be missed during the same walk. S3 directory buckets are unsupported
because they do not provide the required lexicographical ordering and
`StartAfter` contract. Continuous ingestion would require a separate
notification/queue-based design.

Only the optional active object's key, ETag, size, and acknowledged byte offset
are persisted. Completed keys, the scan cursor, and exhaustion are not saved.
Restart resumes an active object first. With no active object, restart begins
a new walk and **can replay all completed objects**.

Keep bucket, prefix, and delimiter unchanged for a connector key. To change
them, use a new connector key or deliberately remove its old checkpoint.

## Delivery and object changes

Each poll opens a conditional streaming GET with `If-Match`, using a byte
range for a nonzero resume offset. It validates response ETag, length, and
range. Read-ahead is discarded after a bounded batch; the next GET starts at
the acknowledged record boundary, not the fetched byte count.

The runtime sends messages, saves candidate state, and then ACKs. Only ACK
commits progress. NACK, cancellation, and read failures preserve committed
progress. Clean EOF and the expected response length are required for
completion, including empty objects and exactly full final batches.

An ETag precondition failure refreshes that key's metadata with HEAD and
restarts its replacement at byte zero. It never applies an old offset to new
contents. Previously delivered records from the old version remain in Iggy.
Refreshed metadata is cached in memory across failed reads and NACKs, while the
acknowledged checkpoint stays unchanged. ACK clears that cache and commits the
replacement's progress. A restart can repeat the metadata refresh.
A deleted or unreadable active object blocks progress rather than being skipped.

Delivery is **at least once**. A send can succeed before checkpoint saving or
ACK fails. Stable message IDs are the first 16 bytes of BLAKE3 over
length-prefixed endpoint, bucket, key, ETag, and the original record-start
offset, interpreted as little-endian `u128`. They let consumers deduplicate
replays; Iggy does not deduplicate messages by this ID.

## Limits and failure recovery

The reader feeds at most 64 KiB at a time into the framer. Retained framing
memory is bounded by the record limit plus delimiter look-ahead and one input
slice, in addition to the output batch and SDK transport buffering.
Framing emits records directly to the batch. After the first rejected record,
it validates the rest of the slice without allocating more record payloads.

An internal 16 MiB scan allowance bounds empty-record work per poll. If the
first record is incomplete, reading continues until it ends or exceeds the
record limit. Checkpoint-only progress includes deliberately omitted empty
records.

An oversized record discards the unreturned batch and latches a failure in
memory. Further polls sleep and report the error without another S3 request.
Correct the limits and restart to retry from the saved checkpoint. Records are
never truncated, split into messages, or silently skipped.

Record size must fit both the batch and Iggy's 64,000,000-byte payload ceiling.
Validation also accounts for per-message headers and request-envelope headroom
against Iggy's absolute request ceiling. **Deployment-specific server, topic,
and transport limits can be smaller and are not discoverable by this plugin.**
Size batches conservatively for those limits; `max_batch_bytes` measures
payload only, not an encoded request. The standard 4 MiB default assumes an
appropriately configured binary transport, not the default 2 MB HTTP limit.

S3 requests have bounded timeouts. Fetch failures, including access-denied and
missing-object responses, increase the retry delay exponentially up to 30
seconds. Each poll waits for the greater of this delay and `poll_interval`,
in addition to the SDK's request retry behavior. A successful fetch resets the
delay. Failed reads never advance progress or skip the active object, so access
repairs or object restoration can recover without a connector restart.
Errors report the SDK failure category, HTTP status when available, and a
bounded service code. They omit raw SDK error details and service messages
because these can contain request details or credentials.
The existing connector SDK handles NACK backoff and stops after five consecutive
NACKs. A poll error itself is logged by the SDK but does not change runtime
connector status to Error. Oversized-record failures are logged once by this
plugin; the SDK may still log each subsequent returned error.

## Verification

Unit and HTTP-contract tests:

```sh
cargo test -p iggy_connector_s3_source
```

An optional microbenchmark compares eager and bounded emission for 64 KiB
slices of tiny records with a one-record batch limit:

```sh
cargo test -p iggy_connector_s3_source given_small_records_when_benchmarked -- --ignored --nocapture
```

It reports elapsed times without a timing assertion. Use the same build profile
for comparisons; it measures framing, not end-to-end S3 throughput.

The HTTP-contract tests exercise the real AWS client against controlled local
responses; they do not replace backend integration coverage.

Floci integration tests reuse the shared `FlociContainer` fixture:

```sh
cargo build --bin iggy-server --bin iggy-connectors
cargo build -p iggy_connector_s3_source -p iggy_connector_s3_sink
cargo test -p integration -- connectors::s3::
```

Docker is required. Test credentials and objects are confined to the fixture.
