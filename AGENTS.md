# Apache Iggy Agent Guidelines

**Apache Iggy (Incubating)** is a persistent message streaming platform
in Rust. Thread-per-core shared-nothing, `io_uring` + `compio`.
Transports: QUIC, WebSocket, TCP (custom binary), HTTP (REST). SDKs:
Rust, .NET, Java, Python, Go, C++, Node.js. A connectors subsystem
ingests from / egresses to external systems via dlopened plugins.

> Skills under `.claude/skills/` are currently scoped to the
> **connectors** subsystem (`core/connectors/`). Load
> [connectors-overview](.claude/skills/connectors-overview/SKILL.md)
> first for any change there. Other subsystems follow the repo-wide
> principles in this file.

## Contents

- [Apache Iggy Agent Guidelines](#apache-iggy-agent-guidelines)
  - [Contents](#contents)
  - [STOP and ask the user before](#stop-and-ask-the-user-before)
  - [Quick reference](#quick-reference)
  - [Structure](#structure)
  - [Where to look](#where-to-look)
  - [Skills](#skills)
  - [Repo-wide principles](#repo-wide-principles)
  - [Tooling](#tooling)
  - [Testing](#testing)
  - [Pitfalls](#pitfalls)
  - [Local state directories](#local-state-directories)
  - [Commits, PRs, releases](#commits-prs-releases)
  - [Discussion and support](#discussion-and-support)

## STOP and ask the user before

- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI
  signature in `core/connectors/sdk/src/{sink,source}.rs` (breaks
  every pre-built plugin .so).
- Modifying `core/binary_protocol/` wire-format types tagged
  `#[repr(C)]` (breaks every running client).
- Touching the state-save path (`core/connectors/runtime/src/state.rs`)
  in a way that changes the atomic-rename protocol (corruption risk).
- Renaming default consumer groups, plugin path resolution rules, or
  any other operator-facing identifier.
- Force-pushing to `master`, amending shipped commits, or skipping
  hooks with `--no-verify` / `--no-gpg-sign`.

## Quick reference

```bash
# Verification order is enforced by CI (.github/actions/rust/pre-merge).
cargo fmt --all                                            # step 1
cargo sort --no-format --workspace                         # step 2 (--no-format mandatory)
cargo clippy --all-features --all-targets -- -D warnings   # step 3
cargo test -p <crate>                                      # step 4

# Other CI checks (mirror in pre-commit; pass --fix where supported)
./scripts/ci/taplo.sh             # TOML
./scripts/ci/markdownlint.sh      # Markdown
./scripts/ci/shellcheck.sh        # Shell
./scripts/ci/license-headers.sh   # Apache headers
./scripts/ci/trailing-whitespace.sh
./scripts/ci/trailing-newline.sh
./scripts/ci/sync-rustc-version.sh
./scripts/ci/python-sdk-version-sync.sh
./scripts/ci/uv-lock-check.sh
./scripts/ci/binary-artifacts.sh --check
./scripts/ci/third-party-licenses.sh --validate --manifest <path>

# Run things
cargo run --bin iggy-server
cargo run --bin iggy -- <subcommand>
IGGY_CONNECTORS_CONFIG_PATH=... cargo run --bin iggy-connectors
./scripts/run-bdd-tests.sh
./scripts/run-benches.sh
```

Pre-commit hooks wire the same scripts. Install with `pre-commit
install` (Python) or `prek install` (Rust drop-in) - both read
`.pre-commit-config.yaml`.

## Structure

```text
iggy/
├── core/
│   ├── server/           Iggy server binary
│   ├── server-ng/        Next-gen server (Viewstamped Replication, WIP)
│   ├── sdk/              Rust client SDK
│   ├── cli/              iggy CLI
│   ├── connectors/       Connectors runtime + SDK + sinks/sources
│   ├── common/           Shared types (IggyDuration, IggyTimestamp, ...)
│   ├── binary_protocol/  Wire format (stable)
│   ├── configs/          Config plumbing + ConfigEnv derive
│   ├── ai/mcp/           MCP server
│   ├── bench/            Benchmark suite
│   ├── integration/      Cross-crate integration tests
│   ├── simulator/        Deterministic simulator
│   ├── consensus/        Raft (Miri-checked unsafe iobuf)
│   ├── journal/          WAL / journal
│   ├── message_bus/      In-process bus
│   ├── shard/            Shard runtime
│   ├── harness_derive/   #[iggy_harness] proc macro
│   └── metadata, partitions, server_common, configs_derive, clock, tools
├── foreign/              Java / .NET / Python / Go / C++ / Node.js SDKs
├── bdd/                  Cross-language BDD (Gherkin)
├── examples/             Per-SDK runnable examples
├── helm/                 Helm charts
├── web/                  Web UI
└── scripts/              CI + release + dev scripts
```

## Where to look

| Task                  | Location                                 |
| --------------------- | ---------------------------------------- |
| Wire protocol         | `core/binary_protocol/`                  |
| Server                | `core/server/src/`                       |
| Next-gen server (WIP) | `core/server-ng/`                        |
| Rust client SDK       | `core/sdk/src/`                          |
| Connectors            | `core/connectors/` -> connector-* skills |
| Integration tests     | `core/integration/tests/`                |
| Cross-SDK BDD         | `bdd/`                                   |
| Benchmark suite       | `core/bench/`                            |

## Skills

Connectors-scoped. Each `SKILL.md` has YAML frontmatter (name,
description). Load `connectors-overview` first as router.

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md) - router + universal connector rules
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md) - FFI host, lifecycle, state, metrics
- [connector-sdk](.claude/skills/connector-sdk/SKILL.md) - traits, decoders/encoders, retry
- [connector-sink](.claude/skills/connector-sink/SKILL.md) - sink plugin authoring
- [connector-source](.claude/skills/connector-source/SKILL.md) - source plugin authoring
- [connector-transform](.claude/skills/connector-transform/SKILL.md) - transform authoring
- [connector-testing](.claude/skills/connector-testing/SKILL.md) - unit + integration test patterns

## Repo-wide principles

1. **Apache 2.0 header on every new source file:** Follow the comment style configured in `licenserc.toml`; common examples: `.rs` uses `// ...`, `Cargo.toml` and shell use `# ...`.
2. **Verification order:** `fmt --all` -> `sort --no-format --workspace` -> `clippy --all-features --all-targets -- -D warnings` -> `test`. Plus `taplo.sh --check`. CI enforces all.
3. **Always pass `--no-format` to `cargo sort`.** Without it multi-line feature arrays flatten and `taplo` re-expands them, churning the workspace.
4. **Avoid LLM-slop tells: em dashes, gratuitous semicolons, hedging narrative, trailing summaries.** Defaults, not absolute bans. Same rule for code, comments, commits, PR descriptions.
5. **Idiomatic Rust over free helpers.** `FromStr` not `parse_foo`, `Display` not `format_foo`, `From`/`TryFrom` not `to_foo_from_bar`. Structured `Err` enums, not `String`.
6. **Imports at the top.** No inline `use` inside functions or blocks. Group: std, external crates, then `crate::`.
7. **Precise names.** No `b`, `p`, `t`, `m`. Match literal API field names as log labels.
8. **Comments explain WHY, not WHAT.** Default to no comment. Add one only when a hidden constraint or workaround would surprise a future reader. Never reference the current task / PR.
9. **Never `cargo install`** without authorization. Toolchain pinned in `rust-toolchain.toml`. CI installs additional cargo tools on demand.
10. **No `unwrap()` / `expect()` on Results from external I/O** outside tests.
11. **`tokio::sync::Mutex` (not `std::sync::Mutex`)** wherever a lock is held across `.await`. Hold locks briefly: lock, clone, drop guard, then do I/O.
12. **Code reads top to bottom.** Public consts and public fns first, helpers below, deeper helpers below those.
13. **Zero clone in hot loops.** `&self`, `&str`, `&[T]`. `Vec::with_capacity(n)` when size is known.
14. **Forward-compatible config.** New TOML fields use `#[serde(default)]` or `Option<T>`.

## Tooling

| Tool                                          | Where                                       |
| --------------------------------------------- | ------------------------------------------- |
| `cargo fmt`, `cargo sort`, `cargo clippy`     | pre-commit + CI                             |
| `cargo machete`                               | CI `machete` task                           |
| `cargo nextest` + `cargo-rail`                | CI `test-*` DAG-scoped                      |
| `cargo llvm-cov`                              | CI coverage                                 |
| `cargo http-registry`                         | CI `verify-publish`                         |
| Miri                                          | CI `miri` (`binary_protocol` + `consensus`) |
| `taplo`, `markdownlint-cli`, `shellcheck`     | pre-commit + CI                             |
| `typos`                                       | pre-commit                                  |
| `cargo about` + `license-checker-rseidelsohn` | `scripts/ci/third-party-licenses.sh`        |
| HawkEye / `license-headers.sh`                | pre-commit + CI                             |
| `uv`, `ruff`                                  | pre-commit (Python)                         |
| `golangci-lint`                               | pre-push (Go)                               |

## Testing

- **Unit:** `#[cfg(test)] mod tests` at EOF of `src/lib.rs` (or per-module).
- **In-crate integration:** `<crate>/tests/`.
- **Cross-crate (real infra):** `core/integration/tests/`. Harness in
  `core/integration/src/harness/` spawns real `iggy-server`
  (`ServerHandle`), connectors runtime (`ConnectorsRuntimeHandle`),
  MCP server (`McpHandle`), client (`ClientHandle`). `#[iggy_harness]`
  proc macro wires fixtures. Suites: `cli/`, `cluster/`,
  `config_provider/`, `connectors/`, `data_integrity/`, `mcp/`, `sdk/`,
  `server/`, `state/`, `storage/`. External backends inside
  `connectors/` use `testcontainers-modules`. Per-test logs land in
  `test_logs/`.
- **Cross-SDK BDD:** `bdd/`. Gherkin features against every SDK. Driver: `scripts/run-bdd-tests.sh`.

BDD test naming: `given_X_when_Y_should_Z` (3-part). Promote to 4-part `given_X_when_Y_then_Z_should_W` only with a distinct "then" intermediate. Be consistent inside a file.

Filter a single integration test by path:

```bash
cargo test -p integration -- connectors::runtime::benchmark::given_logging_format_json
```

## Pitfalls

- **Docker is required for most integration tests.** `testcontainers-modules` spawns containers per test.
- **`cargo sort` without `--no-format` will churn the workspace.** Use the pre-commit hook.
- **Rust toolchain is pinned in `rust-toolchain.toml`.** Older system `cargo` fails on transitive deps. prefer rustup so the pin auto-resolves.
- **`test_logs/` grows quickly.** Wipe between major refactors.
- **Miri only covers `binary_protocol` + `consensus`.** Cannot emulate `io_uring` syscalls. do not try to expand Miri to crates that pull `compio`.
- **Integration crate has no `--test` target.** Filter by test path inside the single `mod.rs` binary.

## Local state directories

Created by server, connectors runtime, or tests. Gitignored. Safe to delete to reset:

- `local_data/` - server data
- `local_state/` - connectors runtime state files
- `test_logs/` - per-test logs + `local_data` snapshots
- `performance_results/` - benchmark output
- `target/` - cargo build

## Commits, PRs, releases

- Conventional commits with scope: `feat(connectors): ...`. Subject <= 72 chars. Allowed types/scopes enforced by `.github/workflows/pr-title.yml`.
- Body: why before what. No bullet lists of files changed.
- Co-authored-by trailer when applicable.
- Workspace + SDK versions sync via `scripts/extract-version.sh` and `scripts/ci/{sync-rust,python-sdk}-version{,-sync}.sh`.
- Crate publish chain verified by `scripts/verify-crates-publish.sh` against `cargo-http-registry`.
- Third-party license enumeration: `scripts/ci/third-party-licenses.sh` (ASF policy).

## Discussion and support

- Design: <https://github.com/apache/iggy/discussions>
- Chat: <https://discord.gg/apache-iggy>
- Docs: <https://iggy.apache.org/docs/>

## Infra quick-ref (yucemonitoring vcluster)

- iggy HTTP: `http://10.20.4.31:31920`. Auth: `POST /users/login {"username":"iggy","password":"iggy"}`.
- Node SSH: `maya@<ip>`, password `test123`. kernel 6.8.0-124. kubeconfig: `~/.kube/yucemonitoring.config`, ns `yucemonitoring`, `--insecure-skip-tls-verify`.
- Jenkins: `maya-anomaly-platform`, `type=iggy`. Trivy CVE scan always fails (false positive) -- check `BUILT_IMAGE=` in console log; if present, deploy anyway.
- QW REST: port-forward `svc/maya-quickwit-s3 7280:7280`. QW **0.8.2** -- only `unix_timestamp` (seconds) as datetime input_format; nanos/millis require 0.9.
- iggy->QW sink: `iggy-{metrics,flows,logs,apigw}` indices (mode:dynamic, no retention). Consumer groups: `qw_sink_{metrics,flows,logs,apigw}`. If connector loops on `InvalidOffset`, delete the CG via iggy HTTP API and restart.
- Segment cleaner: `IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=true` -- already enabled in production.
- Connectors in image: `otlp_source` (JSON/proto modes), `otlp_sink` (PR #3529), `quickwit_sink`.

## READY FOR HANDOVER (2026-06-22, session 15)

**Current state:** Five PRs open on `apache/iggy` (all S-waiting-on-review): #3516 (OTLP source -- awaiting re-review from ryerraguntla), #3517 (COOP_TASKRUN), #3523 (quickwit_sink), #3525 (InvalidOffset SDK -- 3 inline threads answered, awaiting atharvalade re-review), #3529 (otlp_sink -- 22 inline threads answered, 9ba479047 committed, /ready posted). Working tree clean.

**Next steps:** (1) Wait for reviews on all 5 PRs; (2) Investigate `PollingKind::First` TCP bug deeper (see TOBEDECIDED.md -- root cause narrowed to `validate_checksums_and_offsets` or segment offset mismatch; reproduce deterministically).

**Open decisions:** See TOBEDECIDED.md -- TCP first() bug (investigation findings recorded, not yet root-caused).
