# Apache Iggy Agent Guidelines

**Apache Iggy (Incubating)** is a persistent message streaming platform
written in Rust, capable of processing millions of messages per second
at ultra-low latency. It uses a thread-per-core shared-nothing
architecture with `io_uring` and `compio` for low-level I/O. Supported
transports: QUIC, WebSocket, TCP (custom binary protocol), and HTTP
(REST). Client SDKs ship in Rust, .NET, Java, Python, Go, C++, and
Node.js. A connectors subsystem ingests from and egresses to external
systems via dynamically loaded plugins.

> The skill set under `.claude/skills/` is currently scoped to the
> connectors subsystem (`core/connectors/`). Other areas follow the
> universal principles in this file. Deeper per-area skills will be
> added as needed.

## Quick reference

```bash
# Verification order is enforced by CI (.github/actions/rust/pre-merge).
# Skipping any step means a broken PR.
cargo fmt --all                                                       # format (step 1)
cargo sort --no-format --workspace                                    # sort Cargo.toml deps (step 2)
                                                                      # --no-format is required: without it,
                                                                      # multi-line feature arrays get flattened
                                                                      # and taplo then re-formats them, churning
                                                                      # the workspace.
cargo clippy --all-features --all-targets -- -D warnings              # lint (step 3, CI uses --all-features)
cargo test -p <crate>                                                 # tests (step 4)

cargo machete --with-metadata                                         # unused-dep detector (CI)
cargo check --all --all-features                                      # full-workspace check
cargo doc --no-deps --all-features --quiet                            # doctests + docs

# Non-Rust lint scripts (mirror CI). Pass --fix to auto-fix where supported.
./scripts/ci/taplo.sh [--check|--fix]                                 # TOML formatting
./scripts/ci/markdownlint.sh [--fix]                                  # Markdown lint
./scripts/ci/shellcheck.sh [--fix]                                    # Shell lint
./scripts/ci/license-headers.sh [--check|--fix]                       # Apache 2.0 headers
./scripts/ci/trailing-whitespace.sh [--fix]                           # trailing whitespace
./scripts/ci/trailing-newline.sh [--fix]                              # final newline
./scripts/ci/sync-rust-version.sh [--check|--fix]                     # sync rust-toolchain.toml across SDKs
./scripts/ci/python-sdk-version-sync.sh [--check|--fix]               # Python SDK version sync
./scripts/ci/uv-lock-check.sh [--check|--fix]                         # uv.lock freshness
./scripts/ci/binary-artifacts.sh --check                              # no committed binaries
./scripts/ci/third-party-licenses.sh --validate --manifest <path>     # ASF third-party license enumeration
typos                                                                  # spelling (configured in .typos.toml)

# Run things
cargo run --bin iggy-server                                           # server
cargo run --bin iggy -- <subcommand>                                  # CLI
IGGY_CONNECTORS_CONFIG_PATH=... cargo run --bin iggy-connectors       # connectors runtime
./scripts/run-bdd-tests.sh                                            # cross-SDK BDD suite
./scripts/run-benches.sh                                              # benchmark suite
```

## Pre-commit hooks

`.pre-commit-config.yaml` wires all of the lint scripts above plus
`cargo fmt`, `cargo sort --no-format --workspace`, and (on push)
`cargo clippy --all-features --all-targets -- -D warnings`. Install
once and the hooks run on every commit:

```bash
# Classic pre-commit (Python):
pre-commit install
pre-commit run --all-files

# Or prek (Rust reimplementation, drop-in faster replacement):
prek install
prek run --all-files
```

Both read the same `.pre-commit-config.yaml`. CI re-runs the same hooks,
so locally-installed hooks catch issues before push.

## Structure

```text
iggy/
├── core/
│   ├── server/           Iggy server binary
│   ├── server-ng/        Next-gen server rewrite (Viewstamped Replication, WIP)
│   ├── sdk/              Rust client SDK
│   ├── cli/              iggy CLI
│   ├── connectors/       Connectors runtime + SDK + sinks/sources
│   ├── common/           Shared types (IggyDuration, IggyTimestamp, ...)
│   ├── binary_protocol/  Wire format (stable)
│   ├── configs/          Config plumbing + ConfigEnv derive
│   ├── ai/mcp/           MCP server for AI integration
│   ├── bench/            Benchmark suite
│   ├── integration/      Cross-crate integration tests (real iggy-server + testcontainers backends)
│   ├── simulator/        Deterministic simulator
│   ├── tools/            Operational tools
│   ├── harness_derive/   #[iggy_harness] proc macro
│   ├── consensus/        Raft consensus (Miri-checked, unsafe-heavy iobuf)
│   ├── journal/          WAL / journal layer
│   ├── message_bus/      In-process bus
│   ├── shard/            Shard runtime
│   └── metadata, partitions, server_common, configs_derive, clock
├── foreign/              Java / .NET / Python / Go / C++ / Node.js bindings
├── bdd/                  Cross-language BDD tests (Gherkin)
├── examples/             Per-SDK runnable examples
├── helm/                 Helm charts
├── web/                  Web UI
└── scripts/              CI + release + dev scripts
```

## Where to look

| Task                     | Location                           | Notes                                                |
| ------------------------ | ---------------------------------- | ---------------------------------------------------- |
| Wire protocol / commands | `core/binary_protocol/`            | Stable binary format                                 |
| Server internals         | `core/server/src/`                 | TCP/QUIC/HTTP/WS handlers                            |
| Next-gen server          | `core/server-ng/`                  | Viewstamped Replication rewrite (WIP)                |
| Client SDK               | `core/sdk/src/`                    | Async Rust client                                    |
| Connectors runtime       | `core/connectors/runtime/`         | FFI plugin host -> skill `connector-runtime`         |
| Connectors plugins       | `core/connectors/{sinks,sources}/` | One crate per plugin                                 |
| Connectors SDK           | `core/connectors/sdk/`             | Sink/Source traits, FFI macros                       |
| Integration tests        | `core/integration/tests/`          | Real `iggy-server` processes via `TestHarness`       |
| Cross-SDK BDD            | `bdd/`                             | Gherkin features against every SDK                   |
| Benchmark suite          | `core/bench/`                      | Producer/consumer micro and end-to-end               |

## Skills (deep dives)

Currently scoped to the **connectors subsystem**. Load
`connectors-overview` first for any change under `core/connectors/`.

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md) -
  entry point and index. Universal rules, exemplars, efficiency patterns.
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md) -
  FFI, plugin lifecycle, manager, state, metrics, benchmark mode,
  logging format.
- [connector-sdk](.claude/skills/connector-sdk/SKILL.md) -
  Sink/Source traits, decoders/encoders, transforms, retry helpers.
- [connector-sink](.claude/skills/connector-sink/SKILL.md) /
  [connector-source](.claude/skills/connector-source/SKILL.md) -
  per-plugin authoring guides.
- [connector-transform](.claude/skills/connector-transform/SKILL.md) -
  add/delete/update/filter fields, format conversion.
- [connector-testing](.claude/skills/connector-testing/SKILL.md) -
  BDD naming, `#[iggy_harness]`, testcontainers fixtures.

For other subsystems, fall back to the principles below. No
subsystem-specific CLAUDE.md or AGENTS.md is shipped beyond this file.

## Tooling we rely on

CI (`.github/workflows/pre-merge.yml`, `.github/actions/rust/pre-merge/`)
installs and uses these. Match locally to reproduce CI behavior.

| Tool                                          | Purpose                                  | Where invoked                                    |
| --------------------------------------------- | ---------------------------------------- | ------------------------------------------------ |
| `cargo fmt`                                   | Rust formatter                           | pre-commit, CI `fmt` task                        |
| `cargo sort`                                  | Sort `[dependencies]` blocks             | pre-commit (`--no-format`), CI                   |
| `cargo clippy`                                | Lint with `--all-features --all-targets` | pre-push, CI `clippy` task                       |
| `cargo machete`                               | Unused-dep detector                      | CI `machete` task                                |
| `cargo nextest` + `cargo-rail`                | DAG-scoped test execution                | CI `test-*` tasks                                |
| `cargo llvm-cov`                              | Coverage                                 | CI `test-*` + `coverage-baseline.yml`            |
| `cargo http-registry`                         | Local alt-registry for publish probes    | CI `verify-publish` task                         |
| Miri                                          | UB detection on unsafe-heavy crates      | CI `miri` (binary_protocol + consensus)          |
| `taplo`                                       | TOML formatter / checker                 | pre-commit, CI                                   |
| `markdownlint-cli` (`markdownlint`)           | Markdown linter                          | pre-commit, CI                                   |
| `shellcheck`                                  | Shell linter                             | pre-commit, CI                                   |
| `typos`                                       | Spelling                                 | pre-commit                                       |
| `cargo about` + `license-checker-rseidelsohn` | Third-party license enumeration          | `scripts/ci/third-party-licenses.sh`             |
| Apache RAT / `license-headers.sh`             | Apache 2.0 source-header check           | pre-commit, CI                                   |
| `uv`                                          | Python lockfile / env                    | `scripts/ci/uv-lock-check.sh`                    |
| `ruff`                                        | Python formatter + linter                | pre-commit (`foreign/python`, `bdd`, `examples`) |
| `golangci-lint`                               | Go linter                                | pre-push (`foreign/go`)                          |

## Core principles

Apply to every change in this repo.

1. **Apache 2.0 header on every new source file.** `.rs` uses
   `/* ... */`, `Cargo.toml` and shell use `# ...`. Copy verbatim from
   any sibling file. Enforced by `./scripts/ci/license-headers.sh` and
   by the pre-commit hook.
2. **Avoid LLM-slop tells: em dashes, gratuitous semicolons, hedging
   narrative, trailing summaries.** Not strict bans (sometimes the right
   tool) but in this codebase they almost always mark generated text.
   Default to hyphens, commas, or rewriting. Applies to code, comments,
   commits, PR descriptions, and docs.
3. **Verification order is mandatory:** `cargo fmt --all` ->
   `cargo sort --no-format --workspace` -> `cargo clippy --all-features
   --all-targets -- -D warnings` -> `cargo test`. Plus
   `./scripts/ci/taplo.sh --check`. CI enforces all of them.
4. **Always pass `--no-format` to `cargo sort`.** Without it, multi-line
   feature arrays in `Cargo.toml` get flattened, which then conflicts
   with `taplo`. The pre-commit hook already uses the correct flag.
5. **Idiomatic Rust over free helpers.** `FromStr` not `parse_foo`,
   `Display` not `format_foo`, `From`/`TryFrom` not
   `to_foo_from_bar`. Structured `Err` enums, not `String`.
6. **Imports at the top of every file.** No inline `use` inside
   functions or blocks. Group: std, external crates, then `crate::`.
7. **Precise names.** No `b`, `p`, `t`, `m`. Use `batch`, `payload`,
   `timestamp`, `message`. Match literal API field names as log
   labels (`offset=`, `current_offset=`), never invent.
8. **Comments explain WHY, not WHAT.** Default to no comment. Add
   one only when a hidden constraint, invariant, or workaround would
   surprise a future reader. Never reference the current task or PR.
9. **Never `cargo install`** or any package-manager mutation without
   explicit authorization. Use the toolchain pinned in
   `rust-toolchain.toml`. CI installs additional cargo tools on demand
   per task.
10. **No `unwrap()` / `expect()` on Results from external I/O** outside
    tests. Acceptable in tests, in `const` init, and where the
    invariant is locally provable.
11. **`tokio::sync::Mutex` (not `std::sync::Mutex`)** wherever a lock
    is held across `.await`. Hold locks briefly: lock, clone what
    you need, drop the guard, then do I/O.
12. **Code reads top to bottom like a book.** Public consts and
    public functions first, helpers below, deeper helpers below
    those. A reader scrolling top-down should hit each function
    before any function it calls.
13. **Zero clone in hot loops.** Per-message work runs millions of
    times. Use `&self`, `&str`, `&[T]`. Prefer `Vec::with_capacity(n)`
    when the size is known. For connector `Payload::Json`,
    `try_to_bytes(&self)` serializes without cloning the tree.
14. **Forward-compatible config.** New TOML fields use
    `#[serde(default)]` or `Option<T>` so existing configs continue
    to load.

## Connectors-specific invariants

Applied to any change under `core/connectors/`. The connectors skills
expand on each of these.

- **Every silent drop bumps a metric.** Intentional transform filter
  (`Ok(None)`) -> `messages_filtered`. Anything else -> `errors`.
  Pre-built `SinkLabels`/`SourceLabels` caches are threaded through
  the hot path so metric calls avoid per-batch `String` allocations.
- **Stage timings are always observed.** The
  `iggy_connector_stage_duration_seconds` histogram populates
  regardless of the per-connector `benchmark` flag. The flag only
  gates the optional per-batch tracing event under the
  `iggy_connectors::benchmark` target.
- **FFI pointers are call-scoped.** Plugins must copy config and
  state bytes if they need them past the call. The SDK macros do this
  correctly. Do not bypass them.
- **Serialization split:** FFI payloads use `postcard`, connector
  configs use `serde_json`, connector state uses `rmp_serde`
  (MessagePack). Stability vs editability vs compactness, in that
  order. Do not mix.

## Testing

- **Unit tests** at the bottom of `src/lib.rs` (or per-module).
- **In-crate integration tests** under `<crate>/tests/`.
- **Cross-crate integration** under `core/integration/tests/`. The
  harness (`core/integration/src/harness/`) spawns **real**
  `iggy-server` processes (`ServerHandle`), the connectors runtime
  (`ConnectorsRuntimeHandle`), the MCP server (`McpHandle`), and
  client connections (`ClientHandle`). The `#[iggy_harness]` proc
  macro (`core/harness_derive/`) wires fixtures and orchestrates
  lifecycle. Suites cover `cli/`, `cluster/`, `config_provider/`,
  `connectors/`, `data_integrity/`, `mcp/`, `sdk/`, `server/`
  (scenarios, retention, message cleanup, concurrent access,
  consumer groups), `state/`, and `storage/`. Inside `connectors/`,
  external backends (Postgres, Elasticsearch, Mongo, Quickwit, etc.)
  are spun up with `testcontainers-modules`. Test artifacts and
  logs land in `test_logs/` for post-run inspection.
- **Cross-SDK BDD** under `bdd/`. Gherkin features executed against
  every language SDK. Driver: `scripts/run-bdd-tests.sh`.

BDD test naming: `given_X_when_Y_should_Z` (3-part). Promote to
4-part `given_X_when_Y_then_Z_should_W` only when there is a distinct
intermediate "then" state worth naming. Be consistent within a file.

Run a single integration test:

```bash
cargo test -p integration -- connectors::runtime::benchmark::given_logging_format_json
```

## Pitfalls a first-time agent will hit

- **Docker is required for most integration tests.** Connector
  backends (Postgres, Elasticsearch, Mongo, Quickwit, InfluxDB, ...)
  spin up via `testcontainers-modules`. Without a running Docker
  daemon, the affected suites fail at container startup, not at
  assertion time.
- **`cargo sort` without `--no-format` will churn the workspace.**
  Plain `cargo sort --workspace` flattens multi-line feature arrays
  in `Cargo.toml`, which `taplo` then re-expands. Always pass
  `--no-format`. The pre-commit hook already does.
- **The Rust toolchain is pinned to a specific version** in
  `rust-toolchain.toml`. If your system `cargo` is older, builds
  fail on transitive deps that require the pinned `rustc`. Prefer
  rustup so the pin auto-resolves.
- **`test_logs/` grows quickly.** Each integration test snapshots
  `local_data` and server logs on exit. Wipe it between major
  refactors to avoid disk pressure and stale-state confusion.
- **Miri jobs cover `core/binary_protocol/` and `core/consensus/`.**
  Any change touching `unsafe` in those crates needs extra review.
  Miri does not work on crates that pull `compio` (`io_uring`
  syscalls cannot be emulated), so do not try to expand Miri
  coverage there.
- **The integration crate has no `--test` target.** All tests live
  in a single `mod.rs` binary. Filter by test path:
  `cargo test -p integration -- connectors::runtime::benchmark`.
- **Connector plugins are dlopened by path.** If you build with a
  non-default target dir or in release mode, update the `path` field
  in each connector TOML or the runtime will not find the `.so` /
  `.dylib`.
- **Secrets in plugin configs must use `SecretString`.** Plain
  `String` fields containing credentials get logged on `Debug` and
  serialized in plaintext in `/stats`. See the connectors skills for
  the exact `secrecy` + `serde_secret::serialize_secret` pattern.

## Local state directories

Running the server, connectors runtime, or the test suite creates
several gitignored directories at the repo root. They are safe to
delete to reset local state:

- `local_data/` - default server data dir (`messages_path`, etc.).
- `local_state/` - connectors runtime state (`source_*.state` files).
- `test_logs/` - per-test logs and `local_data` snapshots written by
  the integration harness on test exit.
- `performance_results/` - benchmark output (`./scripts/run-benches.sh`).
- `target/` - cargo build output.

If a test hang or crash leaves a stale `local_data` inside `test_logs/`,
delete the offending subdirectory. The harness regenerates everything
on the next run.

## Commit and PR conventions

- Conventional commits with scope: `feat(connectors): ...`,
  `fix(server): ...`, `docs(sdk): ...`. Subject <= 72 chars.
  Enforced by `.github/workflows/pr-title.yml` (allowed types and
  scopes listed there).
- Body answers "why was this needed?" before "what did you do?". No
  bullet lists of files changed. Git already shows that.
- Co-authored-by trailer when applicable.

## Releases

- Versions are kept in sync across the workspace and all SDKs by
  `scripts/extract-version.sh --check` (pre-commit) and
  `scripts/ci/sync-rust-version.sh` /
  `scripts/ci/python-sdk-version-sync.sh`.
- The Rust crate publish chain is verified by
  `scripts/verify-crates-publish.sh` against a local
  `cargo-http-registry` alt-registry.
- Third-party license enumeration for each binary artifact is
  produced by `scripts/ci/third-party-licenses.sh` (ASF release policy).

## Discussion and support

- Design discussions: <https://github.com/apache/iggy/discussions>
- Community chat: <https://discord.gg/apache-iggy>
- Documentation: <https://iggy.apache.org/docs/>
