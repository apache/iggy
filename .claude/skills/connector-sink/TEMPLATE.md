# Sink plugin template

The template is a compiling, tested crate: `core/connectors/sinks/sink_template/`.
There is no second copy of its code here, so nothing in this file can drift from
it.

## Steps

1. Copy `core/connectors/sinks/sink_template/` to
   `core/connectors/sinks/<name>_sink/` and rename the package
   (`iggy_connector_<name>_sink`).
2. Pick exactly one backend path in its
   [README](../../../core/connectors/sinks/sink_template/README.md#choose-your-backend-path):
   Path A for an HTTP backend, Path B for a database, queue or object store.
   Both keep the circuit breaker and the time-budgeted delivery loop.
3. Fill in `push_batch()`, the only required `TODO(ConnectorDeveloper)`.
4. Add a workspace member, a row in `core/connectors/sinks/README.md`, and a
   sample under `core/connectors/runtime/example_config/connectors/`.
5. Read [SKILL.md](SKILL.md) and pre-flight with
   [connector-pr-review](../connector-pr-review/SKILL.md) before `/ready`.

## Where the rules live

- Delivery contract, at-most-once and at-least-once:
  [crate README](../../../core/connectors/sinks/sink_template/README.md#how-a-batch-is-delivered).
- Required tests: [connector-testing](../connector-testing/SKILL.md).
- Hard rules: the [authoring guide](../../../core/connectors/docs/authoring-sinks-and-sources.md).
