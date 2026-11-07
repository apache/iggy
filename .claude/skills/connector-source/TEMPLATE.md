# Source plugin template

The template is a compiling, tested crate: `core/connectors/sources/source_template/`.
There is no second copy of its code here, so nothing in this file can drift from
it.

## Steps

1. Copy `core/connectors/sources/source_template/` to
   `core/connectors/sources/<name>_source/` and rename the package
   (`iggy_connector_<name>_source`).
2. Pick exactly one backend path in its
   [README](../../../core/connectors/sources/source_template/README.md#choose-your-backend-path):
   Path A for an HTTP backend, Path B for a database, queue or filesystem.
   Both keep the circuit breaker, the sleep-first `poll()` and the Ack/Nack
   cursor handshake.
3. Fill in `fetch_records()`, the only required `TODO(ConnectorDeveloper)`.
4. Add a workspace member, a row in `core/connectors/sources/README.md`, and a
   sample under `core/connectors/runtime/example_config/connectors/`.
5. Read [SKILL.md](SKILL.md) and pre-flight with
   [connector-pr-review](../connector-pr-review/SKILL.md) before `/ready`.

## Where the rules live

- Delivery semantics and the bad-record rule:
  [crate README](../../../core/connectors/sources/source_template/README.md#delivery-semantics).
- Required tests: [connector-testing](../connector-testing/SKILL.md).
- Hard rules: the [authoring guide](../../../core/connectors/docs/authoring-sinks-and-sources.md).
