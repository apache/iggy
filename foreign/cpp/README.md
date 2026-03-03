# Iggy C++ Client

Currently, the bazel build system relies on the system-provided cargo toolchain, so concurrent runs can race and lead to data corruption if executed remotely

Build commands

```bash
// Build binary
bazel build //:iggy-cpp

// Unit tests
bazel test //:unit

// Low level integration tests (requires running server)
bazel test //:low-level-e2e
```
