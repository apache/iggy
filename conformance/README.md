# Cross-SDK conformance suite

This directory holds one corpus of YAML test files that every Iggy SDK runs.
A single Rust program, `iggy-conformance`, loads the files, sends each call to an SDK agent and judges every answer.
An SDK agent is a small program that turns JSON commands into SDK calls.
The agent never decides whether a test passes, so no SDK can run a weaker version of a test.

The suite follows the MongoDB unified test format, with one change.
MongoDB gives each driver its own runner. Here one orchestrator owns the parser, the matcher and the result forms.

## Run the suite

Start a server, then run the corpus with the built-in Rust agent:

```bash
cargo build --bin iggy-server --bin iggy --bin iggy-conformance
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy target/debug/iggy-server
target/debug/iggy-conformance run --server 127.0.0.1:8090
```

To run another SDK, give the command that starts its agent:

```bash
target/debug/iggy-conformance run --server 127.0.0.1:8090 --agent "conformance/agents/go/iggy-agent"
```

Other options:

| Option | Effect |
| --- | --- |
| `--no-quorum <address>` | The address of a cluster node that cannot reach quorum. Tests with `topology: no_quorum` need it. |
| `--filter <text>` | Runs only the tests whose id contains the text. |
| `--junit <file>`, `--json <file>` | Writes the results as JUnit XML or as a JSON report. |
| `--skips conformance/skips.toml` | Fails the run on a skip that the list does not allow. |

The other commands are:

- `iggy-conformance check` loads every file and reports the first mistake, without a server.
- `iggy-conformance record --filter <text>` runs tests with the Rust agent and prints every answer as a JSON line.
- `iggy-conformance matrix a.json b.json` joins the JSON reports of several SDKs into one Markdown table.
- `iggy-conformance agent` is the Rust agent. If `--agent` is `rust`, the orchestrator starts it.

## Layout

```text
conformance/
  tests/<area>/*.yaml   the corpus
  skips.toml            the skips that each SDK may have, each with an issue
  agents/<sdk>/         the agents of the SDKs other than Rust
core/conformance/       the orchestrator and the Rust agent
```

## Test file format

A test file is YAML. Its keys are fixed, and an unknown key fails the load.

| Key | Meaning |
| --- | --- |
| `schema_version` | The format version, `"1.0"`. The runner refuses another major version and skips a newer minor version. |
| `description` | What the file tests. |
| `run_on` | Optional. A list of requirement sets. If one set matches, the file runs. |
| `initial_data` | Optional. Calls that the orchestrator makes with its own client before each test. |
| `tests` | The tests. |

Each test has these keys:

| Key | Meaning |
| --- | --- |
| `description` | A sentence that is unique in the file. The test id is `<file path>#<description>`. |
| `run_on` | Optional. Requirement sets, as at the file level. |
| `create_entities` | The clients that the agent creates before the first operation. |
| `operations` | The calls, in order. The first failed call ends the test. |
| `outcome` | Optional. Calls that the orchestrator makes with its own client after the operations, each with `expect_result`. |

Before each test, the orchestrator deletes every stream and every user except root.

### Requirements

A requirement set has these fields. A field that a set does not name always matches.

| Field | Meaning |
| --- | --- |
| `topology` | `single`, `no_quorum` or `cluster`. The run must have that deployment. |
| `transports` | The transports on which the test can run, such as `[tcp]`. |
| `capabilities` | The agent capabilities that the test needs, such as `[reader]`. |

When no set matches, the test is a skip and the report gives the reason.

### Entities

```yaml
create_entities:
  - client: {id: client0}
  - client: {id: client1, node: no_quorum, login: none}
```

| Client field | Meaning |
| --- | --- |
| `id` | The name that operations use. `orchestrator` and `proxy` are reserved. |
| `login` | `root` (the default), `none`, or `{username, password}`. |
| `node` | `default` (the single node), `no_quorum`, `leader` or `follower`. |
| `through_proxy` | `true` connects the client through a fault proxy of its own. |

A call such as `build_reader` creates an entity of another kind. Its `save_result_as` names the new entity.

### Operations

```yaml
- object: client0
  name: get_stream
  arguments: {stream_id: {$$ref: stream_id}}
  expect_result: {name: orders}
```

| Key | Meaning |
| --- | --- |
| `object` | An entity id, `orchestrator` or `proxy`. |
| `name` | The method. For an entity, it is the Rust SDK method name. |
| `arguments` | The arguments, named after the Rust parameters. |
| `expect_result` | The expected result. Without it, the call must still succeed. |
| `expect_error` | The expected error: `{name: unauthenticated}` or `{code: 40}`, and optionally `operation_id`. |
| `save_result_as` | The id of the entity that the call creates. |
| `background` | Starts the call as a task with this name. An `await` collects its answer later. |
| `within`, `not_before` | Time bounds such as `2s` or `200ms`. For an `await`, they count from the start of the task. |

An error name is the snake_case name of the `IggyError` variant, as in the shared codec fixtures.
The orchestrator maps the name to its code.

The `orchestrator` object has these calls:

| Call | Arguments | Effect |
| --- | --- | --- |
| `wait` | `duration_ms` | Sleeps. |
| `await` | `task` | Collects the answer of a task and matches it with `expect_result` or `expect_error`. |
| `cancel` | `task` | Cancels a task, as an application drops a call that it no longer wants. |
| `close` | `entity` | Closes an entity. |

### Matching

- Every field of an expected object must match. The actual result can have more fields.
- Lists match in order and in length.
- `1` and `1.0` are the same number.
- A one-key object whose key starts with `$$` is an operator.

| Operator | Matches |
| --- | --- |
| `{$$exists: false}` | An absent or null field. `true` needs a field that is not null. |
| `{$$type: integer}` | A value of the type. The types are `null`, `boolean`, `number`, `integer`, `string`, `array` and `object`. |
| `{$$any: [a, b]}` | A value that matches one of the expectations. |
| `{$$not: a}` | A value that does not match the expectation. |
| `{$$len: 3}` | A list, object or text of that length. The length can be an expectation, such as `{$$gte: 1}`. |
| `{$$gte: 1}`, `{$$lte: 9}` | A number at or above, or at or below, the bound. |
| `{$$unordered: [a, b]}` | A list of the same length in any order. |
| `{$$contains: [a]}` | A list that has a distinct item for each expectation. |
| `{$$capture: name}` | Any value. The test keeps it under the name. |
| `{$$ref: name}` | The captured value. In `arguments`, the captured value replaces the operator. |

Each test starts with two captures: `root_username` and `root_password` of the run.

## Methods

The orchestrator checks the arguments of each call before it sends the call.
An identifier is a number for an id or text for a name.

| Method | Entity | Arguments | Result |
| --- | --- | --- | --- |
| `connect`, `disconnect`, `ping`, `logout_user` | client | none | null |
| `login_user` | client | `username`, `password` | identity |
| `get_stream` | client | `stream_id` | stream details or null |
| `get_streams` | client | none | list of streams |
| `create_stream` | client | `name`, `encryption_key_hex` | stream details |
| `update_stream` | client | `stream_id`, `name`, `reset` | null |
| `delete_stream` | client | `stream_id` | null |
| `get_topic` | client | `stream_id`, `topic_id` | topic details or null |
| `get_topics` | client | `stream_id` | list of topics |
| `create_topic` | client | `stream_id`, `name`, `partitions_count`, `message_expiry`, `max_topic_size` | topic details |
| `update_topic` | client | `stream_id`, `topic_id`, `name`, `message_expiry`, `max_topic_size`, `reset` | null |
| `delete_topic` | client | `stream_id`, `topic_id` | null |
| `send_messages` | client | `stream_id`, `topic_id`, `partitioning`, `messages` | null |
| `poll_messages` | client | `stream_id`, `topic_id`, `partition_id`, `consumer`, `strategy`, `count`, `commit`, `max_wait_ms`, `min_count` | polled messages |
| `create_consumer_group` | client | `stream_id`, `topic_id`, `name` | consumer group |
| `get_consumer_group` | client | `stream_id`, `topic_id`, `group_id` | consumer group or null |
| `delete_consumer_group` | client | `stream_id`, `topic_id`, `group_id` | null |
| `build_reader` | client | `stream_id`, `topic_id`, `consumer`, `partitions`, `start`, `max_wait_ms`, `commit` | null, and a reader entity |
| `resolve_operation` | client | `operation_id`, `deadline_ms` | null |
| `abandon_operation` | client | `operation_id` | null |
| `next` | reader | none | polled messages |

The argument forms are:

| Argument | Forms |
| --- | --- |
| `message_expiry` | `never`, `server_default`, or microseconds |
| `max_topic_size` | `unlimited`, `server_default`, or bytes |
| `partitioning` | `balanced`, `{partition_id: 0}` or `{messages_key: text}` |
| `messages` | A list of `{id, payload}` or `{id, payload_hex}`, with optional `headers: {key: {kind, value}}`. `id` is decimal text. |
| `consumer` | `{consumer: 1}` or `{group: readers}`. Without it the SDK default consumer applies. |
| `strategy`, `start` | `first`, `last`, `next`, `{offset: 0}` or `{timestamp: microseconds}` |
| `commit` | `manual` (the default), `on_read` or `after_processing` |
| `reset` | Option keys to reset to their server defaults, such as `[max_topic_size]` |

`resolve_operation`, `abandon_operation` and the reader methods need the agent capabilities
`resolve_operation`, `abandon_operation` and `reader`.

## Canonical results

Every agent returns results in one JSON form. The orchestrator decodes each result into its form before it compares
values. A missing or unknown field fails the test, also for a field that the test does not name.
The forms are the types in `core/conformance/src/canonical.rs`:

- Field names follow the Rust SDK types.
- Sizes are byte counts in `size_bytes`, and timestamps are microseconds.
- A message id is decimal text, because no JSON number holds a u128 exactly.
- A message has `payload_hex`. If the payload is valid UTF-8, the message also has `payload` as text.
- An option map uses the HTTP JSON form of `core/common/fixtures/resource_options.json`:
  `{key: {value, kind, explicit}}`, with the value as reversible text. User headers use the same text form.

| Form | Fields |
| --- | --- |
| identity | `user_id` |
| stream | `id`, `name`, `created_at`, `size_bytes`, `messages_count`, `topics_count`, `options` |
| stream details | the stream fields and `topics`, a list of topics |
| topic | `id`, `name`, `created_at`, `size_bytes`, `messages_count`, `partitions_count`, `options` |
| topic details | the topic fields and `partitions` |
| partition | `id`, `created_at`, `segments_count`, `current_offset`, `size_bytes`, `messages_count` |
| polled messages | `partition_id`, `current_offset`, `count`, `messages` |
| message | `offset`, `id`, `timestamp`, `origin_timestamp`, `checksum`, `payload`, `payload_hex`, `headers` |
| consumer group | `id`, `name`, `partitions_count`, `members_count`, `members` (`id`, `partitions_count`, `partitions`) |

## Agent protocol

The orchestrator starts the agent as a child process.
Each side writes one JSON object per line. The agent writes its logs to stderr only.

The agent starts with a hello:

```json
{"hello": {"sdk": "go", "sdk_version": "0.12.0", "protocol": 1, "transports": ["tcp"], "capabilities": ["reader"]}}
```

Then the orchestrator writes requests, each with an `id`:

```text
{"id":1,"op":"create","kind":"client","entity":"client0","args":{"transport":"tcp","address":"127.0.0.1:8090","login":{"username":"iggy","password":"iggy"}}}
{"id":2,"op":"call","entity":"client0","method":"get_stream","args":{"stream_id":1}}
{"id":3,"op":"call","entity":"client0","method":"build_reader","args":{"stream_id":"orders","topic_id":"events"},"save_as":"reader0"}
{"id":4,"op":"start","task":"poll0","entity":"client0","method":"poll_messages","args":{...}}
{"id":5,"op":"await","task":"poll0"}
{"id":6,"op":"cancel","task":"poll0"}
{"id":7,"op":"close","entity":"reader0"}
{"id":8,"op":"reset"}
```

The agent answers each request with exactly one of these lines:

```text
{"id":2,"result":{...}}
{"id":2,"error":{"code":40,"message":"Unauthenticated"}}
{"id":2,"error":{"code":14006,"operation_id":7}}
{"id":2,"unsupported":"the SDK has no partition reader"}
{"id":2,"failure":"cannot decode the arguments"}
```

`login` is null for a client that stays signed out.
`start` answers at once with a null result. `await` answers at the end of the task.

Rules for every agent:

1. Answer requests concurrently, and tag each answer with the `id` of its request.
2. Return an SDK error as its numeric code. For the errors 14006 to 14010, add `operation_id`, and for 14009 add
   `partition_id`.
3. Answer `unsupported` for a method, an argument value, a transport or an entity kind that the SDK does not have.
   The orchestrator reports the test as a skip with that reason.
4. Answer `failure` for a fault of the agent itself. The orchestrator reports the test as an error.
5. On `reset`, cancel every task and close every entity.
6. Never judge a result. Return what the SDK returned.

## Skips

`conformance/skips.toml` lists the skips that each SDK is allowed:

```toml
[[skip]]
sdk = "swift"
test = "faults/lost_send_reply.yaml#The replay of a send that the server committed stores its messages once."
issue = "https://github.com/apache/iggy/issues/NNNN"
```

With `--skips`, a skip that the list does not name fails the run.
A listed test that passes also fails the run, so the list can only shrink.

## Keep the suite honest

A suite only enforces what its runner asserts. These rules keep the assertions in one place:

1. The matcher and the result forms live only in the orchestrator.
2. People write the expected values. `record` output is a starting point. Review it before you commit it, or a bug of
   the Rust SDK becomes the contract.
3. Changes under `conformance/tests/` and `core/conformance/` need a maintainer review.
   Then a coding agent that ports an SDK change cannot weaken the tests in the same pull request.
