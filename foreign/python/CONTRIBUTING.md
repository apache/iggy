# Contributing to Apache Iggy

This repository provides a Python library powered by Rust using `pyo3`. It also utilizes Docker for server deployment.

## Quick Start

### 1. Run the Server with Docker:

Ensure you have Docker installed on your system. Then, execute the following command:

```
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 iggyrs/iggy:0.4.21
```


This command runs the server and maps the specified ports to your local machine.

### 2. Install loguru:
Loguru is advanced library used for logging information about procceses. Install it with:

```
pip install loguru
```

### 3. Install Maturin:

Maturin is used for building Rust binaries for Python. Install it with:

```
pip install maturin
```

### 4. Build and Install the pyo3 Library:

Navigate to your library's root directory and execute:

```
python -m venv .venv && source .venv/bin/activate
maturin develop
```


This will build the Rust library and make it available for Python.

### 5. Running the Examples:

Go to [python_examples/README.md](python_examples/README.md) for instructions on running the examples.

