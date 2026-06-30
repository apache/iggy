# Iggy integration tests under the cluster `vsr` feature

Status of the `integration` test suite (`core/integration/tests/`) run against
the next-gen clustered server (`--features vsr`, 3-node VSR cluster), release,
`--test-threads=1` (io_uring OOM otherwise).

## Scope: what even runs under vsr

The integration binary has **878 tests** without `vsr` (legacy server). With
`--features vsr` only **302** compile/run — the rest are `cfg(not vsr)`-gated or
HTTP-only (server-ng exposes no HTTP listener) and **cannot run under vsr**.

| Top module | non-vsr | vsr-runnable | excluded from vsr |
| --- | ---: | ---: | --- |
| `server` | 285 | **183** | 102 (HTTP transport variants + `cfg(not vsr)` fns) |
| `data_integrity` | 108 | **96** | 12 (HTTP variants) |
| `sdk` | 14 | **12** | 2 (HTTP variants) |
| `storage` | 11 | **11** | 0 |
| `cli` | 174 | 0 | whole module `cfg(not vsr)` (no HTTP/CLI path) |
| `cluster` | 96 | 0 | whole module `cfg(not vsr)` |
| `connectors` | 135 | 0 | whole module `cfg(not vsr)` (connectors runtime) |
| `mcp` | 41 | 0 | whole module `cfg(not vsr)` |
| `config_provider` | 9 | 0 | whole module `cfg(not vsr)` |
| `state` | 5 | 0 | whole module `cfg(not vsr)` |
| **Total** | **878** | **302** | **576** |

Not covered here: `cargo test -p server` / `-p server-ng` crate unit tests — a
separate surface with no vsr cluster mode.

## Result of the vsr-runnable sweep: 302 / 302 pass

| Module | Pass | Fail |
| --- | ---: | ---: |
| `server` | 183 | 0 |
| `data_integrity` | 96 | 0 |
| `sdk` | 12 | 0 |
| `storage` | 11 | 0 |
| **Total** | **302** | **0** |

`server` submodule detail: specific 2/2, scenarios 3/3, purge_delete 6/6,
message_cleanup 7/7, general 12/12 (incl. `authentication`), cg_vsr 21/21
(incl. `duplicate_name_create`), message_retrieval 72/72, concurrent_addition 60/60.

Run as a single test-binary invocation per module (proper per-test cluster
lifecycle). The earlier 5 failures were burst transients; rapid back-to-back
isolated runs can still surface a separate harness flake — "Timed out waiting
for VSR replica mesh to form" — which is cluster-startup contention, not a code
defect.

## Previously failing (5) — fixed by the transient-reply refactor

All five failed with **`Disconnected`** under concurrent / burst load on
**tcp / websocket** (never QUIC):

| Test | Old failure |
| --- | --- |
| `server::concurrent_addition::matrix::tcp_user_cold_barrier_off_expects` | assertion: expected AlreadyExists, got Disconnected |
| `server::concurrent_addition::matrix::websocket_topic_hot_barrier_off_expects` | assertion: 19/20 ok, `Err(Disconnected)` |
| `server::concurrent_addition::matrix::websocket_user_cold_barrier_off_expects` | assertion: got Disconnected |
| `server::concurrent_addition::matrix::websocket_user_hot_barrier_off_expects` | client setup: "login failed: Disconnected" |
| `data_integrity::verify_consumer_group_partition_assignment::should_not_reshuffle_partitions_when_new_member_joins` (websocket) | `unwrap()`: "login failed: Disconnected" |

### Old root cause

Under burst load the metadata primary transiently can't commit a request (or
accept a login); the server stayed **silent** on the transient and relied on the
SDK read-timeout to replay. The **tcp/websocket** lockstep stream can't safely
resend after a silent timeout (a late reply would desync framing) → it must
reconnect → reconnect loses the VSR session → can't idempotent-replay → gated off
→ surfaces `Disconnected`. QUIC alone survived (per-request streams let it resend
on the same connection).

### Fix: explicit `TransientNotCommitted` reply frame (replaces silence)

The server now replies with an explicit `TransientNotCommitted` (IggyError code
57) result-code frame instead of staying silent. The complete frame keeps the
lockstep stream in framing sync, so the SDK replays the **same request id on the
same connection** (no reconnect, session intact) — idempotent via `ClientTable`
dedup. Applied to **both** the metadata request path and the login/register path.

- common: `IggyError::TransientNotCommitted = 57`.
- consensus `metadata_helpers.rs`: `PreflightOutcome::Drop` split into `NotReady`
  (in-flight / not-caught-up — transient) vs `Drop` (stale/gap/newer-session —
  client bug, still silent). `plane_helpers.rs`: `build_transient_reply`.
- metadata `submit_request_in_process`: transients (not-caught-up, pipeline-full,
  in-flight, view-change cancel) reply with the transient frame instead of `Err`.
- server-ng `responses.rs`: login success reply is now result-framed
  (`[count=0][user_id][session]`); `auth.rs` `surface_login_failure` sends the
  transient frame on a non-terminal (transient) login.
- sdk `tcp`/`websocket`/`quic` `send_raw`: encode the header once, replay the same
  request on `TransientNotCommitted` (incl. login), bounded by
  `RESPONSE_READ_TIMEOUT` (30s), paced by `NOT_READY_RETRY_INTERVAL` (50ms).

Verified: build + clippy `-D warnings` + crate unit tests clean; full
vsr-runnable sweep 302/302, 0 regressions (`authentication` and `sdk` login
paths green, so the login-reply result-framing decodes correctly).
