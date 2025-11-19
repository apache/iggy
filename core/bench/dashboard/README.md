# Apache Iggy Benchmarks Dashboard

A modern, high-performance benchmark results dashboard for Iggy, built with Rust. This application provides a responsive web interface for visualizing and analyzing benchmark results.

## Features

- 📊 Interactive performance trend visualization
  - Support for multiple benchmark types:
    - Send (Producer) benchmarks
    - Poll (Consumer) benchmarks
    - Send & Poll (Combined) benchmarks
    - Consumer Group Poll benchmarks
  - Separate visualization for producer and consumer metrics
  - Comprehensive latency metrics (Average, P95, P99, P999)
  - Trend and throughput charts across all benchmarks, hardware and versions
- 🔍 Filter benchmarks by hardware and version
- 📱 Responsive design that works on desktop and mobile
- 🚀 High-performance Rust backend
- ⚡ Fast, modern web frontend built with Yew

![Dashboard](../../../assets/benchmarking_platform.png)

## Project Structure

The project is organized as a Rust workspace with four main components:

- `frontend/`: Yew-based web application
  - Modern UI with interactive charts
  - Type-safe API integration
  - Error handling and loading states

- `server/`: Actix-web REST API server
  - Efficient file system operations
  - Configurable through command-line arguments
  - Built-in security features

- `shared/`: Common code between components
  - Type definitions
  - Serialization logic
  - Shared constants

## API Endpoints

The server provides the following REST API endpoints:

### Health Check

- `GET /health`
  - Check server health status
  - Response: `{"status": "healthy"}`

### Hardware Information

- `GET /api/hardware`
  - List all available hardware configurations
  - Response: Array of hardware configurations

### Git References

- `GET /api/gitrefs/{hardware}`
  - List git references for specific hardware
  - Parameters:
    - `hardware`: Hardware configuration identifier
  - Response: Array of git reference strings

### Benchmarks

- `GET /api/benchmarks/{gitref}`
  - List all benchmarks for a specific git reference
  - Parameters:
    - `gitref`: Git reference identifier
  - Response: Array of benchmark summaries

- `GET /api/benchmarks/{hardware}/{gitref}`
  - List benchmarks for specific hardware and git reference
  - Parameters:
    - `hardware`: Hardware configuration identifier
    - `gitref`: Git reference identifier
  - Response: Array of benchmark summaries

### Benchmark Reports

- `GET /api/benchmark/full/{unique_id}`
  - Get full benchmark report
  - Parameters:
    - `unique_id`: UUID of the benchmark
  - Response: Complete benchmark report JSON

- `GET /api/benchmark/light/{unique_id}`
  - Get lightweight benchmark report
  - Parameters:
    - `unique_id`: UUID of the benchmark
  - Response: Simplified benchmark report JSON

### Trend Analysis

- `GET /api/benchmark/trend/{hardware}/{params_identifier}`
  - Get benchmark trend data
  - Parameters:
    - `hardware`: Hardware configuration identifier
    - `params_identifier`: Benchmark parameters identifier
  - Response: Array of benchmark data points for trend analysis

### Test Artifacts

- `GET /api/benchmark/{unique_id}/artifacts`
  - Download test artifacts for a benchmark
  - Parameters:
    - `unique_id`: UUID of the benchmark
  - Response: ZIP archive containing test artifacts

All endpoints return JSON responses (except artifacts which returns a ZIP file) and use standard HTTP status codes:

- 200: Success
- 404: Resource not found
- 500: Server error

## Prerequisites

- Rust toolchain (latest stable)

  ```bash
  rustup default stable
  ```

- WebAssembly target

  ```bash
  rustup target add wasm32-unknown-unknown
  ```

- Trunk (for frontend development)

  ```bash
  cargo install trunk
  ```

## Development Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   ```

2. Run the development script:

   ```bash
   ./scripts/dashboard/run_dev.sh
   ```

   This will start both the frontend development server and the backend server.

## Running the Application

### Production Mode

1. Build the release version:

   ```bash
   ./scripts/dashboard/build_release.sh
   ```

2. Start the server:

   ```bash
   ./target/release/iggy-bench-dashboard-server --host 127.0.0.1 --port 8061
   ```

### Development Mode

Run the development script to start both the frontend and backend servers:

```bash
./scripts/run_dev.sh
```

This will start:

- Frontend development server on port 8060
- Backend API server on port 8061

Access the development version at <http://localhost:8060>

## Running with Docker

The [docker image](https://hub.docker.com/r/apache/iggy-bench-dashboard) is available, and can be fetched via `docker pull apache/iggy-bench-dashboard`.

### Building the Image

Call this from the root of the repository:

```bash
docker build -t iggy-bench-dashboard -f core/bench/dashboard/server/Dockerfile .
```

### Running the Container

First, ensure your performance results directory exists and has proper permissions:

  ```bash
  mkdir -p performance_results
  chmod 755 performance_results
  ```

Basic usage (recommended):

  ```bash
  docker run -p 8061:8061 \
     -v "$(pwd)/performance_results:/data/performance_results" \
     --user "$(id -u):$(id -g)" \
     apache/iggy-bench-dashboard
  ```

With custom configuration:

  ```bash
  docker run -p 8061:8061 \
        -v "$(pwd)/performance_results:/data/performance_results" \
        --user "$(id -u):$(id -g)" \
        -e HOST=0.0.0.0 \
        -e PORT=8061 \
        -e RESULTS_DIR=/data/performance_results \
        apache/iggy-bench-dashboard
  ```

Using a named volume:

  ```bash
  # Create a named volume
  docker volume create iggy-results

  # Run with named volume
  docker run -p 8061:8061 \
     -v iggy-results:/data/performance_results \
     apache/iggy-bench-dashboard
  ```

## Configuration

### Docker Configuration

#### Environment Variables

| Variable     | Default                   | Description                       |
| ------------ | ------------------------- | --------------------------------- |
| HOST         | 0.0.0.0                   | Server host address               |
| PORT         | 8061                      | Server port                       |
| RESULTS_DIR  | /data/performance_results | Directory for performance results |

#### Volume Permissions

The container is configured to run as a non-root user for security. When mounting a local directory, you should:

1. Use the `--user` flag with your local user ID to ensure proper file permissions
2. Make sure your local directory has the correct permissions (755)
3. If using a named volume, the container will handle permissions automatically

### Application Configuration

#### Server Settings

The server can be configured using command-line arguments:

```bash
iggy-bench-dashboard-server [OPTIONS]

Options:
      --host <HOST>                  Server host address [default: 127.0.0.1]
      --port <PORT>                  Server port [default: 8061]
      --results-dir <RESULTS_DIR>    Directory containing performance results [default: ./performance_results]
      --log-level <LOG_LEVEL>        Log level (trace, debug, info, warn, error) [default: info]
      --cors-origins <CORS_ORIGINS>  Allowed CORS origins (comma-separated) [default: *]
  -h, --help                         Print help
  -V, --version                      Print version
```

### Environment Variables for Development

For development, you can also use environment variables:

- `RUST_LOG`: Control log level and filters
- `RUST_BACKTRACE`: Enable backtraces (1 = enabled, full = full backtraces)

## License

Apache-2.0
