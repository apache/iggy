# Client compatibility matrix

Real Kafka clients, each run against the gateway and against Apache Kafka
(`apache/kafka:4.3.1`, the Kafka oracle), with what each observed compared fact
by fact. `client_matrix_tests.rs` drives it; this directory holds one probe or
configuration per client. Issue: apache/iggy#4531. The latest recorded
results are in [MATRIX.md](MATRIX.md).

## Tiers

| Tier | Clients | When |
| --- | --- | --- |
| Smoke | kcat 1.7.1 (librdkafka 1.7), confluent-kafka-python 2.16.0 (librdkafka 2.16), franz-go 1.22.1, Java client 3.9.0 and 4.3.1 | Every pull request, in the Rust test job |
| Nightly | Vector 0.59.0, Fluent Bit 5.0.11, RisingWave 3.0.5, Flink 2.2.1 with `flink-sql-connector-kafka` 5.0.0-2.2, Kafka Connect with Debezium 3.6.2 (PostgreSQL 17.10) | The scheduled run and manual dispatches (`KAFKA_CLIENT_MATRIX_FULL=1`) |

Every image is pinned by tag and digest in `client_matrix_tests.rs`
(`*_IMAGE` constants are pulled on every run, `*_NIGHTLY` ones only by the
nightly job). Libraries come from their package managers at pinned versions:
confluent-kafka-python from PyPI, franz-go through `franz/go.mod` and
`go.sum`, the Flink connector from Maven Central (checked against its SHA-256).
The Java probe is compiled with the host JDK against the `kafka-clients` jar
of the image it runs in.

## The scenario

Smoke clients run one probe each (`kcat/probe.sh`, `python/probe.py`,
`franz/main.go`, `java/ClientMatrixProbe.java`), at the client's defaults
(the Java producer and franz-go are idempotent, librdkafka is not):

1. **produce**: 12 records `k000`..`k011`, values `v000`..`v011`, headers
   `idx=<n>` and `src=matrix`, onto a topic of 3 partitions.
2. **consume**: a group consumer reads them back.
3. **commit**: it commits synchronously and closes (kcat: commits on close).
4. **resume**: 12 more records; a new consumer in the same group must read
   exactly those, and nothing from the first batch.

Nightly clients run their own steps through their own configuration:

| Client | Steps |
| --- | --- |
| Vector, Fluent Bit | `sink` (stdin or a tailed file to the Kafka sink), `source` (group consumer), `commit` (offsets read back with the Python probe), `resume` (restart, second batch) |
| Flink | `sink`: a bounded `INSERT INTO sink SELECT * FROM source`, KafkaSource in, `AT_LEAST_ONCE` KafkaSink out |
| RisingWave | `source` (`CREATE SOURCE` and a materialized view), `sink` (`CREATE SINK`), `stream` (a second batch flows through) |
| Kafka Connect + Debezium | `start` (distributed worker creates and reads its internal topics), `snapshot`, `stream` (rows inserted after start), `restart` (a new worker resumes from the stored offsets) |

## The probe contract

A probe prints one event per line on stdout; anything else is ignored.

```text
ACK <phase> <partition|-> <offset|->           a record the broker acknowledged
NACK <phase> <reason>                           a record or call that failed
REC <phase> <partition> <offset> <key> <value> <name=value,...|->
COMMITTED <phase> <partition> <offset>          a committed offset (-1: none)
DONE                                            the probe reached its end
```

Keys, values and headers are single words (the Python probe percent-escapes
`%`, space and newline). `tests/common/matrix.rs` turns events into facts:
counts, per-partition offset ranges, committed offsets, how many records came
back intact (key, value and headers as written), duplicates, order, and the
first index a resumed consumer read. Nothing a broker chooses (node ids,
timestamps, producer ids) is a fact.

`python/probe.py` doubles as the matrix's reader and writer for the nightly
clients: `produce`, `read` (every partition from the start to its end, no
group) and `committed` (a group's offsets).

## Judging

A fact that differs between the gateway and Kafka fails the run, unless
[`allowed_differences.toml`](allowed_differences.toml) declares it with the
issue that owns it; a declared difference that no longer occurs
fails too. If a client's run on Kafka is not what a working client gives (all
records back, intact, resumed exactly), the row is **broken**: the probe or the
setup is wrong, not the gateway.

The matrix prints one row per client and one column per step: `pass`, `fail,
known (#issue)`, `**FAIL**` (undeclared), or `n/a`. CI writes it to the job
summary and uploads it as the `client-matrix` artifact.

## Running it locally

```bash
cargo build -p server --bin iggy-server
# Podman: KAFKA_ORACLE_CONTAINER_CLI=podman, or a `docker` shim on PATH.
# The smoke images are enough for a pull-request run; the _NIGHTLY ones are
# only needed for the full matrix.
for image in $(grep -rhoP '^pub const [A-Z0-9_]+_(?:IMAGE|NIGHTLY): &str = "\K[^"]+' gateways/kafka/tests/client_matrix_tests.rs) apache/kafka:4.3.1; do docker pull "$image"; done
KAFKA_CLIENT_MATRIX_DIR=$PWD/target/client-matrix \
  cargo nextest run -p iggy-gateway-kafka --test client_matrix_tests
# The nightly clients as well:
KAFKA_CLIENT_MATRIX_FULL=1 cargo nextest run -p iggy-gateway-kafka --test client_matrix_tests
```

Needs Linux (`--network host` reaching the host loopback), GNU `timeout`, a
JDK (`javac`), `curl` and `sha256sum`. What the matrix builds (Python
packages, the franz-go binary, Java classes, the Flink jar) is cached under
Cargo's target directory (`tmp/client-matrix`).
