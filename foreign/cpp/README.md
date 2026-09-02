<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Iggy C++ Client

C++ client for [Apache Iggy](https://iggy.apache.org) message streaming.

Currently, the bazel build system relies on the system-provided cargo toolchain, so concurrent runs can race and lead to data corruption if executed remotely

Build commands

```bash
// Build binary
bazel build //:iggy-cpp

// Unit tests
bazel test //:unit

// Low level integration tests (requires running server)
bazel test //:e2e
```
