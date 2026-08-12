# Cross-SDK BDD Testing

This directory contains cross-SDK Behavior-Driven Development (BDD) tests for Apache Iggy, designed to ensure consistency across different language SDKs.

## Structure

```bash
bdd/
├── scenarios/                  # Shared Gherkin feature files
│   ├── basic_messaging.feature
│   └── leader_redirection.feature
├── rust/                       # Rust SDK BDD implementation
│   ├── Dockerfile              # Rust BDD test container
│   ├── tests/
│   └── Cargo.toml
├── python/                     # Python SDK BDD implementation
│   ├── Dockerfile              # Python BDD test container
│   ├── tests/
│   ├── pyproject.toml
│   └── uv.lock
├── go/                         # Go SDK BDD implementation
│   ├── Dockerfile              # Go BDD test container
│   ├── tests/
│   ├── go.mod
│   └── go.sum
├── node/                       # Node SDK BDD implementation
│   └── Dockerfile              # Node BDD test container
├── csharp/                     # csharp SDK BDD implementation
│   └── Dockerfile              # csharp BDD test container
├── java/                       # Java SDK BDD implementation
│   ├── Dockerfile              # Java BDD test container
│   ├── src/test/
│   └── build.gradle.kts
├── php/                        # PHP SDK BDD implementation
│   ├── Dockerfile              # PHP BDD test container
│   ├── phpunit.xml.dist
│   └── tests/
├── docker-compose.yml          # Base: SDK test clients (always included)
├── docker-compose.server.yml   # Single iggy-server test setup
├── docker-compose.cluster.yml  # Leader + follower test setup
├── docker-compose.coverage.yml # Coverage collection overlay
├── docker-compose.vsr.yml      # server-ng (VSR) overlay, Rust SDK only
├── Dockerfile                  # Debug build of Iggy server
└── README.md
```

## Usage

### Quick Start

```bash
# Usage: ../scripts/run-bdd-tests.sh [--coverage] <sdk> [feature]
#   sdk:     rust | python | php | go | go-race | node | csharp | java | all | clean  (default: all)
#   feature: basic_messaging | leader_redirection | all  (default: all)

# Run all features for all SDKs
../scripts/run-bdd-tests.sh all

# Run specific SDK tests (all features)
../scripts/run-bdd-tests.sh rust
../scripts/run-bdd-tests.sh python
../scripts/run-bdd-tests.sh go
../scripts/run-bdd-tests.sh node
../scripts/run-bdd-tests.sh csharp
../scripts/run-bdd-tests.sh java
../scripts/run-bdd-tests.sh php

# Run only basic_messaging feature for Rust SDK
../scripts/run-bdd-tests.sh rust basic_messaging

# Run only leader_redirection
../scripts/run-bdd-tests.sh all leader_redirection

# Run against iggy-server-ng with VSR (Rust SDK only; other SDKs do not
# speak the VSR wire protocol yet). Build the vsr binaries first:
#   cargo build --bin iggy-server-ng --bin iggy --features vsr
# NOTE: `target/debug/iggy` is shared between lanes and the vsr flavour
# speaks a different wire protocol. When switching back to the legacy
# lane, rebuild without `--features vsr` first, or the in-container
# healthcheck ping cannot talk to the legacy server.
../scripts/run-bdd-tests.sh --vsr rust

# Clean up Docker resources
../scripts/run-bdd-tests.sh clean
```

### Requirements

- Docker and Docker Compose
- The tests build the latest Iggy server from source in debug mode for faster compilation

### How it Works

1. **Server Containers**: `docker-compose.server.yml` runs a single Iggy server; `docker-compose.cluster.yml` adds a leader + follower pair for cluster scenarios
2. **SDK Containers**: Each SDK has its own container with the appropriate runtime and dependencies
3. **Shared Features**: All SDKs test against the same `.feature` files for consistency
4. **Health Checks**: Containers wait for the server to be healthy before running tests

### Adding New SDKs

To add a new SDK (e.g., Node.js):

1. Create `node/` directory
2. Add `node/Dockerfile` with appropriate runtime and dependencies
3. Create `node/tests/` directory with BDD implementation
4. Add `node-bdd` service to `docker-compose.yml`
5. Update `../scripts/run-bdd-tests.sh` script
6. Update [components.yml](https://github.com/apache/iggy/blob/master/.github/config/components.yml) file to include the new SDK files

### CI/CD Integration

GitHub Actions workflow: [ci-test-bdd.yml](https://github.com/apache/iggy/blob/master/.github/workflows/ci-test-bdd.yml)

## Development

### For Rust SDK

The Rust implementation is located in `bdd/rust/` and linked via Docker volumes.

### For Python SDK

The Python implementation is in `bdd/python/tests/` and needs to be updated as the Python SDK API evolves.

### For Go SDK

The Go implementation is located in `bdd/go/tests/`.

### For Node SDK

The node.js BDD test are run by cucumber-js, bdd test code is located at [foreign/node/src/bdd](../foreign/node/src/bdd/)

### For csharp SDK

The csharp implementation is located at [foreign/csharp/Iggy_SDK.Tests.BDD](../foreign/csharp/Iggy_SDK.Tests.BDD)

### For Java SDK

The Java implementation is located in `java/src/test/`

### For PHP SDK

The PHP implementation is located in `bdd/php/tests/` and runs through PHPUnit.

### Adding New Scenarios

Add new `.feature` files to the `bdd/scenarios/` directory and implement the corresponding step definitions in each SDK's test directory.
