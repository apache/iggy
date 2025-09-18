#!/bin/bash

# Setup script for testing Flink Data Producer with Iggy
# This script ensures Iggy is running and ready for testing

set -e

echo "=== Iggy Setup for Flink Data Producer ==="
echo ""

# Check if Iggy is running
check_iggy() {
    echo -n "Checking if Iggy server is running..."
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo " ✓ (HTTP on port 8080)"
        return 0
    elif nc -z localhost 8090 2>/dev/null; then
        echo " ✓ (TCP on port 8090)"
        return 0
    else
        echo " ✗"
        return 1
    fi
}

# Start Iggy if not running
start_iggy() {
    echo "Starting Iggy server..."

    # Check if we're in the right directory
    if [ ! -f "../../Cargo.toml" ]; then
        echo "Error: Please run this script from the examples/flink-data-producer directory"
        exit 1
    fi

    # Try to start Iggy server
    echo "Building and starting Iggy server..."
    cd ../..

    # Check if iggy-server binary exists
    if [ -f "target/release/iggy_server" ]; then
        echo "Starting Iggy server from existing binary..."
        ./target/release/iggy_server &
    else
        echo "Building Iggy server first..."
        cargo build --package server --release
        ./target/release/iggy_server &
    fi

    IGGY_PID=$!
    echo "Iggy server started with PID: $IGGY_PID"

    # Wait for Iggy to be ready
    echo -n "Waiting for Iggy to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/health > /dev/null 2>&1; then
            echo " ✓"
            return 0
        fi
        sleep 1
        echo -n "."
    done

    echo " ✗"
    echo "Error: Iggy server failed to start"
    kill $IGGY_PID 2>/dev/null || true
    exit 1
}

# Main script
if check_iggy; then
    echo "Iggy server is already running"
else
    echo "Iggy server is not running"
    read -p "Do you want to start Iggy server? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        start_iggy
    else
        echo ""
        echo "Please start Iggy server manually with one of these commands:"
        echo ""
        echo "  # From the iggy root directory:"
        echo "  cargo run --package server"
        echo ""
        echo "  # Or if already built:"
        echo "  ./target/release/iggy_server"
        echo ""
        echo "Then run the producer with:"
        echo "  cargo run --package flink-data-producer"
        exit 1
    fi
fi

echo ""
echo "=== Iggy is ready! ==="
echo ""
echo "You can now run the Flink Data Producer with:"
echo ""
echo "  # Default settings (TCP, 1000 messages):"
echo "  cargo run --package flink-data-producer"
echo ""
echo "  # Continuous mode:"
echo "  cargo run --package flink-data-producer -- --continuous"
echo ""
echo "  # Custom settings:"
echo "  cargo run --package flink-data-producer -- \\
      --stream-id my-stream \\
      --topic-id my-topic \\
      --num-messages 5000 \\
      --batch-size 100"
echo ""
echo "  # Get help:"
echo "  cargo run --package flink-data-producer -- --help"
echo ""