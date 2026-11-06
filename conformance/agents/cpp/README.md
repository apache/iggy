# C++ conformance agent

This is the agent of the C++ SDK (`foreign/cpp`) for the cross-SDK conformance suite. It reads JSON requests on
stdin, calls the SDK and writes one JSON answer per request on stdout. [The suite README](../../README.md) describes
the protocol.

## How it uses the SDK

The agent calls the generated cxx bridge, `iggy::ffi`, which `foreign/cpp/src/lib.rs` declares.
`IggyBlockingClient` in `iggy.hpp` has no `SendMessages`, `Ping` or raw requests. It also cannot share its
connection with the bridge. So each client of a test is one `iggy::ffi::Client`.

The bridge gives error codes through `ErrorInfo` for writes, logins, polls, readers, consumers, raw requests and
metadata changes. Other calls give the error message only: `get_stream`, `get_topic`, `get_consumer_group`,
`get_user`, `describe_options`, `connect`, `disconnect` and `ping`. These reads also fail for a missing resource.
The agent answers such an error with code 0, which no `IggyError` has, and the message of the SDK.

## Build

The build needs CMake 3.24 or newer, a C++17 compiler and the Rust toolchain of `rust-toolchain.toml`. CMake runs
cargo on a copy of the SDK manifest in the build tree, so the build writes nothing to `foreign/cpp`. CMake also
downloads nlohmann_json 3.12.0, the release that the Bazel tests of the SDK use.

```bash
cmake -S conformance/agents/cpp -B conformance/agents/cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build conformance/agents/cpp/build -j
```

## Run

```bash
CONFORMANCE_AGENT="$PWD/conformance/agents/cpp/build/iggy-agent" ./scripts/run-conformance-tests.sh cpp
```

The agent announces the TCP transport and every capability except `connection_info`. The SDK has no call for it.
