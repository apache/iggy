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
(cd conformance/agents/go && go build -o iggy-agent .)
target/debug/iggy-conformance run --server 127.0.0.1:8090 --agent conformance/agents/go/iggy-agent
```

`scripts/run-conformance-tests.sh <sdk>` does all of this the way CI does. It starts a single node, a node that
cannot reach quorum and a two-node cluster on spare local ports, builds the agent, runs the corpus with the skip list
and writes the reports to `reports/`. Its header lists the environment variables, for example `CONFORMANCE_HOST` to
run two suites side by side on two loopback addresses.

The script builds and runs the agents of `rust`, `go`, `python`, `csharp`, `java`, `node`, `cpp`, `php` and `swift`.
Each agent directory under `conformance/agents/` holds its own build files. The PHP agent, and the Swift agent on a
machine without Swift, build and run in Docker images.

Other options:

| Option | Effect |
| --- | --- |
| `--no-quorum <address>` | The address of a cluster node that cannot reach quorum. Tests with `topology: no_quorum` need it. |
| `--leader <address>`, `--follower <address>` | The addresses of the two nodes of a cluster. Tests with `topology: cluster` need them. |
| `--cluster-proxy-host <host>` | The host that the cluster nodes advertise. The orchestrator proxies the port of each node there. |
| `--filter <text>` | Runs only the tests whose id contains the text. |
| `--junit <file>`, `--json <file>` | Writes the results as JUnit XML or as a JSON report. |
| `--skips conformance/skips.toml` | Fails the run on a skip or a divergence that the list does not allow. |

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
| `expect_frames` | Optional. The frames that the fault proxy recorded for a client. See [Fault proxy](#fault-proxy). |
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
| `id` | The name that operations use. `orchestrator`, `proxy` and `admin` are reserved. |
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
| `object` | An entity id, `orchestrator`, `proxy` or `admin`. |
| `name` | The method. For an entity, it is the Rust SDK method name. |
| `arguments` | The arguments, named after the Rust parameters. |
| `expect_result` | The expected result. Without it, the call must still succeed. |
| `expect_error` | The expected error: `{name: unauthenticated}` or `{code: 40}`, and optionally `operation_id`. |
| `save_result_as` | The id of the entity that the call creates. |
| `background` | Starts the call as a task with this name. An `await` collects its answer later. |
| `within`, `not_before` | Time bounds such as `2s` or `200ms`. For an `await`, they count from the start of the task. |

An operation on `admin` calls a method with the orchestrator's own client, which is the Rust SDK. Use it for a setup
step that the test does not check, so that a divergence of the SDK under test fails only the tests that check that
call.

An error name is the snake_case name of the `IggyError` variant, as in the shared codec fixtures.
The orchestrator maps the name to its code.

The `orchestrator` object has these calls:

| Call | Arguments | Effect |
| --- | --- | --- |
| `wait` | `duration_ms` | Sleeps. |
| `await` | `task` | Collects the answer of a task and matches it with `expect_result` or `expect_error`. |
| `cancel` | `task` | Cancels a task, as an application drops a call that it no longer wants. |
| `close` | `entity` | Closes an entity. |

### Fault proxy

A client with `through_proxy: true` connects through a proxy listener of its own, so every frame on the listener
belongs to that client. The proxy reads the 256-byte consensus header of each frame, forwards the bytes unchanged and
records the frame. The proxy covers plain TCP only, so a test that uses it declares `transports: [tcp]`.

The `proxy` object has these calls:

| Call | Arguments | Effect |
| --- | --- | --- |
| `fail_frames` | `client`, `match`, `action`, `skip`, `times`, `delay_ms` | Applies the action to the matching frames. |
| `refuse_connections` | `client`, `duration_ms` | Closes each new connection to the listener for the given time. |
| `clear_rules` | `client` | Removes the rules of the client. |

`match` selects frames by `direction` (`request` or `reply`), `command` and `code`.
`skip` lets the first matching frames through, and `times` limits the frames that the rule changes. The default is
one frame.

| Action | Effect | What it models |
| --- | --- | --- |
| `drop_request` | The server never sees the request. The connection stays open. | A primary that ignores a request |
| `drop_reply` | The server applies the request, and the reply never arrives. | A reply lost after the commit |
| `close_before_forward` | The proxy closes the connection instead of forwarding. | A network failure before the server |
| `close_after_forward` | The proxy forwards the frame, then closes the connection. | A failure with an unknown outcome |
| `delay` | The proxy holds the frame for `delay_ms`. | A slow network or a slow primary |

`expect_frames` matches the recorded frames of a client as a list:

```yaml
expect_frames:
  - client: client0
    commands: [consumer_group.join]
    direction: request
    frames:
      - {request: {$$capture: lost}, outcome: dropped}
      - {request: {$$ref: lost}, outcome: forwarded}
```

`commands` and `direction` select frames, and pings are left out unless `include_pings` is `true`.

In a run with `--cluster-proxy-host`, the cluster nodes advertise the proxy host. The orchestrator listens there on the
port of each node for the whole run, so every client of the cluster goes through the proxy, also after a redirect.
These listeners are shared by the clients of a test. The proxy calls and `expect_frames` name them `@leader` and
`@follower` in place of a client.
Each frame has these fields:

| Field | Meaning |
| --- | --- |
| `connection` | The proxy's number of the TCP connection, from 1. |
| `direction` | `request` or `reply`. |
| `command` | The protocol command name of `iggy_binary_protocol::codes::command_name`, such as `message.send`. A frame without a request has the consensus command, such as `ping_client`. |
| `code` | The command code. A reply has the code of its request. |
| `request`, `client`, `session` | The request number, the client id as text, and the session. A reply repeats the request number. |
| `status` | The status of a reply. Zero means success. |
| `size`, `at_ms` | The frame size, and the milliseconds since the listener started. |
| `outcome` | `forwarded`, `dropped`, `delayed`, `closed_before` or `closed_after`. |

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

Each test starts with captures of the run: `root_username`, `root_password`, `server_address`, and in a run with a
cluster `leader_address` and `follower_address`.

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
| `build_consumer` | client | `stream_id`, `topic_id`, `group`, `max_wait_ms`, `count`, `commit` | null, and a consumer entity |
| `resolve_operation` | client | `operation_id`, `deadline_ms` | null |
| `abandon_operation` | client | `operation_id` | null |
| `send_binary_request` | client | `code`, `payload_hex` | raw reply |
| `send_binary_request_to_replica` | client | `code`, `payload_hex`, `replica` | raw reply |
| `describe_options` | client | `scope` | list of option specs |
| `create_user` | client | `username`, `password`, `status`, `permissions` | user details |
| `get_user` | client | `user_id` | user details or null |
| `update_user` | client | `user_id`, `username`, `status` | null |
| `delete_user` | client | `user_id` | null |
| `get_metadata_changes` | client | `start`, `groups`, `limit` | metadata changes |
| `get_connection_info` | client | none | connection info |
| `next` | reader | none | polled messages |
| `next` | consumer | none | received message |

The argument forms are:

| Argument | Forms |
| --- | --- |
| `message_expiry` | `never`, `server_default`, or microseconds |
| `max_topic_size` | `unlimited`, `server_default`, or bytes |
| `partitioning` | `balanced`, `{partition_id: 0}` or `{messages_key: text}` |
| `messages` | A list of `{id, payload}` or `{id, payload_hex}`, with optional `headers: {key: {kind, value}}`. `id` is decimal text. |
| `consumer` | `{consumer: 1}` or `{group: readers}`. Without it the consumer is `{consumer: 0}`, the default of the Rust SDK. |
| `strategy`, `start` | `first`, `last`, `next`, `{offset: 0}` or `{timestamp: microseconds}` |
| `commit` | `manual` (the default), `on_read` or `after_processing` |
| `reset` | Option keys to reset to their server defaults, such as `[max_topic_size]` |
| `group` | The name of a consumer group that exists. `build_consumer` joins it. |
| `scope` | `stream`, `topic` or `user` |
| `status` | `active` (the default) or `inactive` |
| `permissions` | `{global: {manage_servers, read_servers, manage_users, read_users, manage_streams, read_streams, manage_topics, read_topics, poll_messages, send_messages}}`, each flag false by default. Without it the user has no permissions. |
| `start` | `earliest`, `latest`, `{after: cursor}` or `{current: page token or null}`, with the cursor and the page token in their text forms |
| `groups` | Metadata change groups such as `[streams]`. Empty means every group. |

Some methods need an agent capability. The orchestrator skips a test that needs a capability that the agent does
not announce:

| Capability | Methods |
| --- | --- |
| `reader` | `build_reader`, and `next` on a reader |
| `consumer` | `build_consumer`, and `next` on a consumer |
| `resolve_operation`, `abandon_operation` | The method of the same name |
| `raw_command`, `raw_command_replica` | `send_binary_request`, `send_binary_request_to_replica` |
| `describe_options`, `metadata_changes`, `connection_info` | `describe_options`, `get_metadata_changes`, `get_connection_info` |

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
| raw reply | `payload_hex` |
| option spec | `key`, `kind`, `updatable`, `resettable`, `secret`, `read_only`, `critical` |
| user details | `id`, `username`, `status`, `created_at`, `permissions` (`global` only, or null), `options` |
| received message | `partition_id`, `message` |
| connection info | `server_address` |
| metadata changes | The HTTP JSON form that `core/common/fixtures/metadata_changes.json` calls `expected` |

A metadata changes answer must decode into the Rust `MetadataChanges` type and encode back to the same JSON. Its
records keep their fields at the top level, such as `kind`, `stream_id` and `requested_op`, as in the fixture.

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
   `partition_id`. If the SDK reports an error that has no Iggy code, return code 0 with its message. No `IggyError`
   has code 0, so the test fails, and the skip list can name the divergence.
3. Answer `unsupported` for a method, an argument value, a transport or an entity kind that the SDK does not have.
   The orchestrator reports the test as a skip with that reason.
4. Answer `failure` for a fault of the agent itself. The orchestrator reports the test as an error.
5. On `reset`, cancel every task and close every entity.
6. Never judge a result. Return what the SDK returned.

## Skips and divergences

`conformance/skips.toml` lists the skips that each SDK is allowed, and its known divergences.
A divergence is a test that fails because the SDK differs from the contract.

```toml
[[skip]]
sdk = "swift"
test = "faults/lost_send_reply.yaml#The replay of a send that the server committed stores its messages once."
issue = "https://github.com/apache/iggy/issues/NNNN"

[[divergence]]
sdk = "go"
test = "streams/stream_crud.yaml#Delete a stream."
issue = "https://github.com/apache/iggy/issues/NNNN"
reason = "GetStream returns an error for a missing stream"
```

With `--skips`, a skip that the list does not name fails the run, and a listed divergence does not.
A listed test that passes also fails the run, so the list can only shrink.
Test ids are relative to `--tests`, so a run of part of the corpus uses `--filter`, not another `--tests` directory.

## Keep the suite honest

A suite only enforces what its runner asserts. These rules keep the assertions in one place:

1. The matcher and the result forms live only in the orchestrator.
2. People write the expected values. `record` output is a starting point. Review it before you commit it, or a bug of
   the Rust SDK becomes the contract.
3. Changes under `conformance/tests/` and `core/conformance/` need a maintainer review.
   Then a coding agent that ports an SDK change cannot weaken the tests in the same pull request.
