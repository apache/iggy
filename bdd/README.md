# Cross-SDK BDD Testing

This directory contains cross-SDK Behavior-Driven Development (BDD) tests for Apache Iggy, designed to ensure consistency across different language SDKs.

## Structure

```bash
bdd/
├── scenarios/                  # Shared Gherkin feature files
│   └── basic_messaging.feature
├── rust/                       # Rust SDK BDD implementation
│   ├── Dockerfile              # Rust BDD test container
│   ├── tests/
│   └── Cargo.toml
├── python/                     # Python SDK BDD implementation
│   ├── Dockerfile              # Python BDD test container
│   ├── tests/
│   └── requirements.txt
├── node/                       # Node SDK BDD implementation
│   └── Dockerfile              # Node BDD test container
├── docker-compose.yml          # Orchestrates server + SDK containers
├── Dockerfile                  # Debug build of Iggy server
└── README.md
```

## Usage

### Quick Start

```bash
# Run all SDK tests
../scripts/run-bdd-tests.sh all

# Run specific SDK tests
../scripts/run-bdd-tests.sh rust
../scripts/run-bdd-tests.sh python
../scripts/run-bdd-tests.sh go
../scripts/run-bdd-tests.sh node

# Clean up Docker resources
../scripts/run-bdd-tests.sh clean
```

### Requirements

- Docker and Docker Compose
- The tests build the latest Iggy server from source in debug mode for faster compilation

### How it Works

1. **Server Container**: Builds and runs the latest Iggy server in debug mode
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
6. Update [changed-files-config.json](https://github.com/apache/iggy/blob/master/.github/changed-files-config.json) file to include the new SDK files

### CI/CD Integration

GitHub Actions workflow: [ci-test-bdd.yml](https://github.com/apache/iggy/blob/master/.github/workflows/ci-test-bdd.yml)

## Development

### For Rust SDK

The Rust implementation is located in `core/bdd/` and linked via Docker volumes.

### For Python SDK

The Python implementation is in `bdd/python/tests/` and needs to be updated as the Python SDK API evolves.

### For Node SDK

The node.js BDD test are run by cucumber-js, bdd test code is located at [foreign/node/src/bdd](../foreign/node/src/bdd/)

### Adding New Scenarios

Add new `.feature` files to the `bdd/scenarios/` directory and implement the corresponding step definitions in each SDK's test directory.
