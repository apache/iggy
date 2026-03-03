# Iggy C++ Client

Currently, the bazel build system relies on the system-provided cargo toolchain, so concurrent runs can race and lead to data corruption if executed remotely

Running the example:

```bash
bazel build //:iggy-cpp
bazel test //:iggy-cpp-test
```
