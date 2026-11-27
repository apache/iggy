- Feature Name: `sdk_conformance`
- Start Date: 2026-11-06
- RFC PR: [apache/iggy#4915](https://github.com/apache/iggy/pull/4915)
- Discussion: [apache/iggy#0000](https://github.com/apache/iggy/discussions/0000)
- Iggy Issue: [apache/iggy#0000](https://github.com/apache/iggy/issues/0000)

## Summary

Add one corpus of YAML tests that every Iggy SDK runs, and one Rust program, `iggy-conformance`, that runs them.
This orchestrator parses each file, sends each call to an agent and judges the answer.
An agent is a small program in the language of one SDK that turns JSON commands into SDK calls and returns the result.
It never decides whether a test passes.
Every answer must decode into one canonical JSON form, so a test means the same thing in every SDK.
A TCP proxy inside the orchestrator injects faults, such as a lost reply, and records each frame (one request or reply
on the wire).
One test can then assert behavior that the public API hides, in all nine SDKs.
An example is that a replay keeps the request number of the lost request.
A skip list names each test that an SDK cannot pass yet, with an issue, and the list can only shrink.
The design adapts the MongoDB unified test format, but one orchestrator owns the parser and the matcher instead of one
runner per driver.
The Gherkin features in `bdd/` move to the corpus one at a time.
A prototype ran 49 tests against agents for all nine SDKs and found 36 failures where an SDK answers differently from
the Rust SDK.

## Motivation

An Iggy application can use any of nine SDKs: Rust, Go, Java, C#, Python, Node.js, C++, PHP and Swift.
Users expect the same call to give the same answer in each.
Today a shared test makes sure of that for only part of the API, and for no behavior after a network fault.

### SDKs give different answers to the same call

The [prototype](#prototype) found these differences, among others:

| SDK | Difference |
| --- | --- |
| Go | `GetStream` returns error 1009 (`stream_id_not_found`) for a missing stream. Rust and Java return no stream. |
| Python | `create_topic` and `create_consumer_group` return `None`, where the Rust SDK returns the new resource. |
| C++ | Some read calls, such as `GetStreams`, drop the Iggy code of an error, so an application cannot tell `unauthenticated` (code 40) from other errors. |
| Node.js | `stream.get` returns a stream without its list of topics. |
| Swift | A write with an unknown outcome fails with error 57, not 14006, and no call resolves it. A committing deferred poll uses a connection of its own. |
| PHP | `createStream` and `createTopic` return nothing. A received message has no `origin_timestamp` and no headers. |

An application that moves to another SDK finds these differences at run time.
The Node.js agent also showed three faults that no test catches yet.
`sendBinaryRequest` returns an empty payload for a one-byte reply, timestamps lose their sub-millisecond part, and
consumer groups have no member list.

### Hidden behavior has tests only in Rust

Go, Java, C#, Node.js and Swift implement the binary protocol themselves, with the 256-byte consensus header of
`core/binary_protocol/src/consensus/header.rs`.
They also implement a client contract that the public API does not show.
The contract works on lanes. A lane admits one write at a time, in order.
A client has one lane for metadata writes, such as a consumer group join, and one lane per partition.
Offset operations and committing polls share a separate progress gate per partition.
When the network loses the reply to a send, the contract works like this:

1. The call fails with `operation_uncertain` (code 14006) and names the request number of the lost request.
2. A second call on the same lane fails with `lane_blocked` (code 14007) and sends nothing.
3. `resolve_operation` replays the request with the same request number, so the server can drop a duplicate.

A consumer group join follows a different rule since apache/iggy#4803.
The SDK abandons an uncertain join at once, and the next call sends a new join.

The Rust SDK tests this contract against an in-process fake server, `core/sdk/src/links/fake_cluster.rs`, but no
shared test reaches the five native SDKs.
The server has no failpoints (switches that fail a chosen request).
The only BDD scenario with a lost connection calls the SDK's own `disconnect()`.
A bug in this contract can store a message twice.

The three fault tests of the prototype run this contract against a real server through the proxy.
They pass on Rust, Go, Java, C#, Node.js, Python and C++, and they show the Swift gaps in the table above.

### The Gherkin suite costs nine implementations of every step

`bdd/scenarios/` holds 9 feature files with 40 scenarios and about 130 distinct step phrases.
Every SDK implements every phrase in its own step code.
On 2026-11-05 that step code had 22,903 lines across the nine SDKs, from 1,425 in Node.js to 7,136 in Go.
A scenario that reuses existing phrases costs nothing, but most new features add phrases, and each new phrase costs
nine implementations.

Coverage drifts, because each SDK chooses its own features:

- `stream_crud` runs only in Rust and Java.
- `leader_redirection` runs in Rust, Java, Go and C#.
- PHP runs 6 of the 9 features. Python, C++, Node.js and Swift run 7.

Each SDK keeps its feature list in a different place.
Rust, Python, Java, C++ and PHP use volume mounts in `bdd/docker-compose.yml`, and Node.js uses `COPY` lines in
`bdd/node/Dockerfile`.
C# uses entries in its test project, and Go and Swift use runner code.
No report shows which SDK skips which scenario.
Each step implementation also picks its own strictness, so two SDKs can pass one scenario with different assertions.

### Use cases

1. A maintainer adds a cross-SDK test as one YAML file. If the test uses existing methods, no SDK needs new code.
2. A contributor ports a Rust SDK change to another SDK, and the tests that Rust passes decide whether the port is
   correct.
   This also works for a coding agent, because the tests and the orchestrator are not part of the port.
3. A user who chooses an SDK reads one matrix of what each SDK passes, skips and fails, with an issue for each gap.
4. A reviewer of a reconnect or replay change in a native SDK sees the frames that the SDK sent, not only the result.

## Guide-level explanation

### Terms

| Term | Meaning |
| --- | --- |
| corpus | The YAML test files under `conformance/tests/`. |
| orchestrator | `iggy-conformance`, the Rust program that runs the corpus and judges every answer. |
| agent | A small program per SDK. It turns JSON commands into SDK calls and returns what the SDK returned. |
| canonical form | The one JSON form of a result type that every agent returns. |
| fault proxy | A TCP proxy in the orchestrator, between an SDK client and the server. It injects faults and records frames. |
| skip | A test that an SDK cannot run, because the SDK lacks a method or a capability. |
| divergence | A test that an SDK runs and fails, because the SDK differs from the contract. |
| skip list | `conformance/skips.toml`. It lists the allowed skips and the known divergences of each SDK, each with an issue. |

### A converted test

`conformance/tests/streams/stream_crud.yaml` restates `bdd/scenarios/stream_crud.feature`.
This is one of its five tests:

```yaml
schema_version: "1.0"
description: An SDK creates, reads, lists, renames and deletes a stream by its numeric id.

tests:
  - description: Delete a stream.
    create_entities:
      - client: {id: client0}
    operations:
      - object: client0
        name: create_stream
        arguments: {name: stream-crud-delete}
        expect_result: {id: {$$capture: stream_id}}
      - object: client0
        name: delete_stream
        arguments: {stream_id: {$$ref: stream_id}}
      - object: client0
        name: get_stream
        arguments: {stream_id: {$$ref: stream_id}}
        expect_result: null
    outcome:
      - name: get_streams
        expect_result: []
```

`client0` is a client of the SDK under test, signed in as root by default.
`$$capture` stores the id of the new stream, and `$$ref` uses it in later calls.
`outcome` runs after the agent resets, with the orchestrator's own client, so the final state does not depend on the
SDK under test.

The Go SDK fails the last operation:

```text
FAIL streams/stream_crud.yaml#Delete a stream.: operation 2 (client0.get_stream): unexpected error 1009
(stream_id_not_found): stream with id: 0 was not found.
```

The skip list names the failure until the Go SDK changes or the discussion of this RFC changes the expected answer:

```toml
[[divergence]]
sdk = "go"
test = "streams/stream_crud.yaml#Delete a stream."
operation = 2
answer = {error = {code = 1009}}
issue = "https://github.com/apache/iggy/issues/NNNN"
reason = "GetStream returns ErrStreamIdNotFound (1009) for a missing stream, where Rust and Java return no stream"
```

The run accepts error 1009 at operation 2, but any other failure of the test still fails the run.
Once the Go SDK passes the test, the run fails until someone removes the entry.

### A fault test

`conformance/tests/faults/lost_send_reply.yaml` ports two Rust tests of `core/sdk/src/driver.rs` from a fake server to a
real server:
`given_a_silent_partition_primary_when_the_deadline_passes_should_retain_the_sent_operation` and
`given_a_retained_op_when_resolved_after_commit_should_return_ok_and_free_the_lane`.

```yaml
schema_version: "1.0"
description: >-
  A send whose reply the network loses ends as uncertain and blocks its lane until the caller
  resolves it. The resolution replays the send, and the server stores the messages once.
run_on:
  - transports: [tcp]
initial_data:
  - name: create_stream
    arguments: {name: orders}
  - name: create_topic
    arguments: {stream_id: orders, name: events, partitions_count: 1}

tests:
  - description: The replay of a send that the server committed stores its messages once.
    create_entities:
      - client: {id: client0, through_proxy: true}
    operations:
      - object: proxy
        name: fail_frames
        arguments:
          client: client0
          match: {command: message.send}
          action: drop_reply
      - object: client0
        name: send_messages
        arguments:
          stream_id: orders
          topic_id: events
          partitioning: {partition_id: 0}
          messages: [{id: "1", payload: once}]
        expect_error: {name: operation_uncertain, operation_id: {$$capture: lost}}
      - object: client0
        name: send_messages
        arguments:
          stream_id: orders
          topic_id: events
          partitioning: {partition_id: 0}
          messages: [{id: "2", payload: blocked}]
        expect_error: {name: lane_blocked, operation_id: {$$ref: lost}}
      - object: client0
        name: resolve_operation
        arguments: {operation_id: {$$ref: lost}}
      - object: client0
        name: send_messages
        arguments:
          stream_id: orders
          topic_id: events
          partitioning: {partition_id: 0}
          messages: [{id: "3", payload: after}]
      - object: client0
        name: poll_messages
        arguments:
          stream_id: orders
          topic_id: events
          partition_id: 0
          strategy: first
          count: 10
        expect_result:
          count: 2
          messages:
            - {offset: 0, id: "1", payload: once}
            - {offset: 1, id: "3", payload: after}
    expect_frames:
      - client: client0
        commands: [message.send]
        frames:
          - {direction: request, request: {$$ref: lost}, outcome: forwarded}
          - {direction: reply, request: {$$ref: lost}, outcome: dropped, committed: true}
          - {direction: request, request: {$$ref: lost}, outcome: forwarded}
          - {direction: reply, request: {$$ref: lost}, outcome: forwarded}
          - {direction: request, request: {$$not: {$$ref: lost}}, outcome: forwarded}
          - {direction: reply, outcome: forwarded}
```

The proxy forwards the first send and drops its reply, so the server stores the message, but the SDK never learns it.
The test asserts four facts that the public API does not show:

- The error names the request number of the lost send, and the dropped reply has `committed: true`.
- The blocked send sends no frame, because `expect_frames` lists every send request.
- The resolution replays the send with the same request number, and the partition holds the message once.
- The send after the resolution gets a new request number, so the lane is free.

The file does not require `resolve_operation`, so an SDK without that call runs the first two sends and skips at the
replay.
Swift fails the first send with error 57, not 14006, so the skip list names a divergence.

### A cluster test

`scripts/run-conformance-tests.sh` starts a two-node cluster whose nodes advertise a proxy host to the proxy.
The orchestrator listens on that host, on the port of each node, for the whole run, and dials the nodes from it.
Every SDK client then stays behind the proxy, also after the SDK follows a redirect to the leader.
`conformance/tests/cluster/leader_redirection.yaml` restates `bdd/scenarios/leader_redirection.feature`.
This is one of its four tests:

```yaml
tests:
  - description: A client of the follower moves to the leader.
    run_on:
      - topology: cluster
    create_entities:
      - client: {id: client0, node: follower}
    operations:
      - object: client0
        name: get_connection_info
        eventually: 10s
        expect_result: {server_address: {$$ref: leader_address}}
      - object: client0
        name: create_stream
        arguments: {name: test-stream}
        expect_result: {name: test-stream}
    outcome:
      - name: get_streams
        expect_result: [{name: test-stream}]
```

`eventually` repeats the read every 100 milliseconds until its answer matches, for at most 10 seconds.
The SDK can report the address of the follower until it follows the redirect.

### Run the suite

```bash
cargo build --bin iggy-server --bin iggy --bin iggy-conformance
scripts/run-conformance-tests.sh go
```

The script starts a single node, a node that cannot reach quorum and a two-node cluster on spare local ports.
It builds the Go agent, runs the corpus with the skip list, and writes JUnit XML and JSON reports to `reports/`.
`iggy-conformance matrix` joins the JSON reports of several SDKs into one Markdown table.
These rows come from the prototype:

| Test | cpp (tcp) | csharp (tcp) | go (tcp) | java (tcp) | node (tcp) | php (tcp) | python (tcp) | rust (tcp) | swift (tcp) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `faults/uncertain_join.yaml#The build fails as uncertain, and the next build joins again with a new request number.` | pass | pass | pass | pass | pass | pass | pass | pass | FAIL |
| `faults/lost_poll_connection.yaml#The partition stays blocked until the abandon, and the poll after it reads on.` | pass | pass | pass | pass | pass | FAIL | pass | pass | FAIL |
| `messaging/basic_messaging.yaml#Create a stream and a topic, send ten messages and poll them from offset 0.` | pass | pass | pass | pass | pass | skip | FAIL | pass | pass |

A known divergence shows as `FAIL` in the matrix, and the skip list gives its reason.

### For SDK maintainers

Each SDK gets one agent under `conformance/agents/<sdk>/`, with its own build files.
The agent has one dispatch entry per method, so it grows with the API, not with the number of tests.
In the prototype, an agent has from 1,336 lines (Node.js) to 2,866 lines (C++), build files included.
When the corpus gains a method, each agent adds an entry or answers `unsupported`, which the report shows as a skip.
An agent never judges a result. If the SDK returns something unexpected, the agent returns it as it is.

If an SDK cannot pass a test, add a skip-list entry with an issue.
The rules in [Skips and divergences](#skips-and-divergences) keep the list down to real gaps.

### For contributors who add a test

1. Find the area of the test under `conformance/tests/`, or add a new area.
2. Write the test with existing methods. If it needs a new method, add the method to the orchestrator and the Rust
   agent first.
3. Run `iggy-conformance check` to load every file without a server.
4. Run `iggy-conformance record --filter <text>` to see the answers of the Rust SDK.
5. Write the expected values from the contract, not from the `record` output.
6. Run the test on every SDK. For each SDK that cannot pass it, add a skip or a divergence entry with an issue.

### For reviewers

A change under `conformance/tests/`, `conformance/skips.toml` or `core/conformance/` needs a maintainer review.
An expected value changes only with a reason in the pull request.
CI refuses a pull request that changes an SDK and also changes an existing test or adds a skip-list entry.

## Reference-level explanation

### Facts about master this design depends on

- The result types in `iggy_common` derive `Serialize`, and the HTTP API returns them as JSON.
  An example is `Json<StreamDetails>` in `core/server/src/http/handlers.rs`.
- Every frame starts with the 256-byte consensus header.
  A request header gives the command, the request number, the client and the session, but both sign-in commands
  share the operation `Register`.
  A reply header gives the request number and the client, but not the command or the session.
  With status zero, a reply to a metadata or consumer-offset operation can still hold a rejection in its result section
  (`Operation::is_result_framed`).
- `iggy_binary_protocol::codes::command_name` names each command code, for example `message.send`.
- Each cluster node advertises the address in `IGGY_CLUSTER_NODES_<n>_ADVERTISED_ADDRESS`, and an SDK follows a
  redirect to the advertised address of the leader.
  A node can pick the address by the network of the client, with `advertised_addresses` and `client_cidr`.
  A cluster that advertises the proxy host only to the proxy keeps every SDK client behind the proxy.
  The admin client stays direct, and the server needs no change.
- `core/common/fixtures/` holds the shared codec fixtures for resource options, metadata changes and deferred polling.
  All eight foreign SDKs copy them, and the canonical forms reuse their HTTP JSON form.
- `IggyError` has the retained-operation errors 14006 to 14010, and the native SDKs implement them.
  Swift does not retain writes, so a write with an unknown outcome fails with error 57 (`transient_not_committed`).
  It raises 14010 for a committing read, and `abandonOperation` frees that read.
- Python, C++ and PHP wrap the Rust SDK through pyo3, cxx and ext-php-rs.
  For them, the suite tests the binding: types, error codes and async behavior.
  Go, Java, C#, Node.js and Swift implement the protocol themselves, so the suite tests their whole client contract.

The design changes no wire format, no on-disk format and no server code.

### Components

```text
 conformance/tests/**/*.yaml   conformance/skips.toml
              |                         |
              v                         v
 +----------------------------------------------+  admin client (Rust SDK)  +-------------+
 | iggy-conformance                             |-------------------------->|             |
 |   format types, matcher, canonical forms     |                           | iggy-server |
 |   reports and matrix                         |  frames, unchanged        |             |
 |   fault proxy  <-------------+               |-------------------------->|             |
 +------------------------------|---------------+                           +-------------+
        | JSON lines on         | SDK traffic over TCP
        | stdin and stdout      |
        v                       |
 +------------------------------+---------------+
 | agent: Go, Java, C#, Python, ...             |
 |   the SDK under test                         |
 +----------------------------------------------+
```

The orchestrator does these jobs:

- It loads each test file into Rust types that reject unknown keys.
- It runs `initial_data` before each test and `outcome` after it with its own Rust SDK client.
  This admin client connects to the server directly, not through the proxy, because the nodes advertise the proxy
  host only to the proxy.
- It starts the agent as a child process and sends it the operations of each test.
- It decodes the arguments of each call into strict types first, so a mistake in a test file never reaches an agent.
- It decodes each answer into its canonical form and compares it with the expectation.
- It runs the fault proxy and writes JUnit XML and a JSON report for each run.

The binary has five commands:

| Command | Effect |
| --- | --- |
| `run` | Runs the corpus against one agent. |
| `check` | Loads every file and reports the first mistake, without a server. |
| `record` | Runs tests with the Rust agent and prints every answer as a JSON line. |
| `matrix` | Joins the JSON reports of several SDKs into one Markdown table. |
| `agent` | Runs the Rust agent. `run --agent rust` starts it as the agent. |

`run` takes these options, among others:

| Option | Effect |
| --- | --- |
| `--server <address>` | The address of the single node. |
| `--agent <command>` | The command that starts the agent, or `rust` for the built-in Rust agent. |
| `--no-quorum <address>` | The address of a cluster node that cannot reach quorum. Tests with `topology: no_quorum` need it. |
| `--leader <address>`, `--follower <address>` | The addresses of the two nodes of a cluster. Tests with `topology: cluster` need them. |
| `--cluster-proxy-host <host>` | The host that the cluster nodes advertise. The orchestrator proxies the port of each node there. |
| `--filter <text>` | Runs only the tests whose id contains the text. |
| `--junit <file>`, `--json <file>` | Writes the results as JUnit XML or as a JSON report. |
| `--skips <file>` | Applies the skip list. See [Skips and divergences](#skips-and-divergences). |
| `--call-timeout`, `--hello-timeout` | The time limits in [Isolation, time limits and failures](#isolation-time-limits-and-failures). |

The layout:

```text
conformance/
  README.md             the format, the methods, the canonical forms and the agent protocol
  tests/<area>/*.yaml   the corpus
  skips.toml            the allowed skips and the known divergences, each with an issue
  agents/<sdk>/         the agents of the eight SDKs other than Rust, each with its build files
core/conformance/       the crate iggy-conformance (publish = false): the orchestrator and the Rust agent
scripts/run-conformance-tests.sh
```

### Test file format

A test file is YAML.
The types in `core/conformance/src/format.rs` reject unknown keys, so a misspelled key fails the load, not the test.

| File key | Meaning |
| --- | --- |
| `schema_version` | The format version, `"1.0"`. See [Versioning](#versioning). |
| `description` | What the file tests. |
| `run_on` | Optional. A list of requirement sets. If one set matches, the file runs. |
| `initial_data` | Optional. Calls that the orchestrator makes with its own client before each test. |
| `tests` | The tests. |

| Test key | Meaning |
| --- | --- |
| `description` | A sentence that is unique in the file. The test id is `<file path>#<description>`. |
| `run_on` | Optional. Requirement sets, as at the file level. |
| `create_entities` | The clients that the agent creates before the first operation. |
| `operations` | The calls, in order. The first failed call ends the test. |
| `expect_frames` | Optional. The frames that the fault proxy recorded. See [Fault proxy](#fault-proxy). |
| `outcome` | Optional. Calls that the orchestrator makes with its own client after the agent resets, each with `expect_result`. |

#### Requirements

A field that a requirement set leaves out always matches.

| Field | Meaning |
| --- | --- |
| `topology` | `single`, `no_quorum` or `cluster`. The run must have that deployment. |
| `transports` | The transports on which the test can run, such as `[tcp]`. |
| `capabilities` | The agent capabilities that the test needs, such as `[reader]`. |

If no set matches, the test is a skip, and the report gives the reason.

#### Entities

A client entity has these fields:

| Field | Meaning |
| --- | --- |
| `id` | The name that operations use. `orchestrator`, `proxy` and `admin` are reserved. |
| `login` | `root` (the default), `none`, or `{username, password}`. |
| `node` | `default` (the single node), `no_quorum`, `leader` or `follower`. |
| `through_proxy` | `true` connects the client through a fault proxy listener of its own. `check` refuses it with a `node` other than `default`, because a cluster client moves to the addresses that the nodes advertise. |

A call such as `build_reader` creates an entity of another kind, and its `save_result_as` names the new entity.

#### Operations

| Key | Meaning |
| --- | --- |
| `object` | An entity id, or `admin`, `orchestrator` or `proxy`. |
| `name` | The method. For an entity, it is the Rust SDK method name, but `build_reader` and `build_consumer` stand for the builders of `reader` and `consumer_group`. |
| `arguments` | The Rust parameters, with the fields of an options struct inline. The builders also take `stream_id` and `topic_id`, and `build_consumer` takes `group` for the group name. Bytes take a `_hex` suffix, milliseconds take a `_ms` suffix, and `op` is `operation_id`. |
| `expect_result` | The expected result. Without it, the call must still succeed. |
| `expect_error` | The expected error, as `{name: unauthenticated}` or `{code: 40}`, and optionally `operation_id`. |
| `save_result_as` | The id of the entity that the call creates. |
| `background` | Starts the call as a task with this name. An `await` collects its answer later. |
| `within`, `not_before` | Time bounds such as `2s` or `200ms`. For an `await`, they count from the start of the task. |
| `eventually` | Repeats a read every 100 milliseconds until the answer matches, for at most this time. It needs an expectation, and it takes no time bounds and no saved entity. `check` refuses it on a write, and a poll that stores an offset counts as a write. |

An error name is the snake_case name of the `IggyError` variant, as in the shared codec fixtures.
The orchestrator maps each name to its code.

The `admin` object calls a method with the orchestrator's own client, for a setup step that the test does not assert.
A divergence of the SDK under test then fails only the tests that assert that call.

The `orchestrator` object has four calls:

| Call | Arguments | Effect |
| --- | --- | --- |
| `wait` | `duration_ms` | Sleeps. |
| `await` | `task` | Collects the answer of a task and matches it with `expect_result` or `expect_error`. |
| `cancel` | `task` | Cancels a task, as an application drops a call that it no longer wants. |
| `close` | `entity` | Closes an entity. |

The calls of the `proxy` object are in [Fault proxy](#fault-proxy).

#### Matching

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

Each test starts with the captures `root_username`, `root_password` and `server_address`.
A run with a cluster adds `leader_address` and `follower_address`.

### Methods

The prototype defines 33 client methods, plus `next` on a reader and on a consumer.

| Group | Methods | Capability |
| --- | --- | --- |
| Session | `connect`, `disconnect`, `ping`, `login_user`, `logout_user`, `get_connection_info` | `connection_info` for `get_connection_info` |
| Streams | `get_stream`, `get_streams`, `create_stream`, `update_stream`, `delete_stream` | none |
| Topics | `get_topic`, `get_topics`, `create_topic`, `update_topic`, `delete_topic` | none |
| Messages | `send_messages`, `poll_messages` | none |
| Consumer groups | `create_consumer_group`, `get_consumer_group`, `delete_consumer_group` | none |
| Readers and consumers | `build_reader`, `build_consumer`, and `next` on the entity that each one creates | `reader`, `consumer` |
| Retained operations | `resolve_operation`, `abandon_operation` | the name of the method |
| Raw commands | `send_binary_request`, `send_binary_request_to_replica` | `raw_command`, `raw_command_replica` |
| Users | `create_user`, `get_user`, `update_user`, `delete_user` | none |
| Options | `describe_options` | `describe_options` |
| Metadata changes | `get_metadata_changes` | `metadata_changes` |

These argument rules apply to every method:

- An identifier is a number for an id or text for a name.
- A message id is decimal text, because no JSON number holds a u128 exactly.
- A payload is UTF-8 text in `payload`, or raw bytes in `payload_hex`.
- A duration or a size can also have a named value, such as `never` or `server_default` for `message_expiry`.

[`conformance/README.md`](https://github.com/apache/iggy/blob/conformance-suite/conformance/README.md) on the
prototype branch lists the arguments and the result of each method.

### Canonical results

Every agent returns results in one JSON form.
The orchestrator decodes each result into the types in `core/conformance/src/canonical.rs` before it compares values.
A missing or unknown field fails the test, also for a field that the test does not name.
So a new canonical field lands in the orchestrator and in every agent in one pull request.

The forms follow these rules:

- Field names follow the Rust SDK types.
- Sizes are byte counts in `size_bytes`, and timestamps are microseconds.
- Every u64 and u128 value is decimal text, such as an offset or a `checksum`.
- A message has `payload_hex`. If the payload is valid UTF-8, the message also has `payload` as text.
- An option map uses the HTTP JSON form of `core/common/fixtures/resource_options.json`.
  The form is `{key: {value, kind, explicit}}`, with the value as reversible text. User headers use the same text form.
- A metadata changes answer uses the HTTP JSON form that `core/common/fixtures/metadata_changes.json` calls `expected`.
  It must decode into the Rust `MetadataChanges` type and encode back to the same JSON.

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
| received message | `partition_id`, `message` |
| raw reply | `payload_hex` |
| option spec | `key`, `kind`, `updatable`, `resettable`, `secret`, `read_only`, `critical` |
| user details | `id`, `username`, `status`, `created_at`, `permissions` (`global` only, or null), `options` |
| connection info | `server_address` |
| metadata changes | the HTTP JSON form of `core/common/fixtures/metadata_changes.json` |

### Agent protocol

The orchestrator starts the agent as a child process.
Each side writes one JSON object per line on stdin and stdout, and the agent writes its logs to stderr only.
The agent starts with a hello:

```json
{"hello": {"sdk": "go", "sdk_version": "0.12.0", "protocol": 1, "transports": ["tcp"], "capabilities": ["reader"]}}
```

The orchestrator refuses an agent whose hello names another protocol version.
Then it writes requests, each with an `id`:

```text
{"id":1,"op":"create","kind":"client","entity":"client0","args":{"transport":"tcp","address":"127.0.0.1:8090","login":{"username":"iggy","password":"iggy"}}}
{"id":2,"op":"call","entity":"client0","method":"get_stream","args":{"stream_id":1}}
{"id":3,"op":"call","entity":"client0","method":"build_reader","args":{"stream_id":"orders","topic_id":"events"},"save_as":"reader0"}
{"id":4,"op":"start","task":"poll0","entity":"client0","method":"poll_messages","args":{"stream_id":"orders","topic_id":"events","partition_id":0,"strategy":{"offset":"9007199254740993"},"count":10}}
{"id":5,"op":"await","task":"poll0"}
{"id":6,"op":"cancel","task":"poll0"}
{"id":7,"op":"close","entity":"reader0"}
{"id":8,"op":"reset"}
```

The agent answers each request with exactly one of these lines:

```text
{"id":2,"result":{...}}
{"id":2,"error":{"code":40,"message":"Unauthenticated"}}
{"id":2,"error":{"code":14006,"operation_id":"7"}}
{"id":2,"unsupported":"the SDK has no partition reader"}
{"id":2,"failure":"cannot decode the arguments"}
```

`login` is null for a client that stays signed out.
`start` answers at once with a null result, and `await` answers at the end of the task.

Both sides write every u64 and u128 value as decimal text, such as the offset of `poll0` and the `operation_id`
of 14006.
The orchestrator turns this text into numbers and back, so tests and captures keep a u64 as a number.

Rules for every agent:

1. Answer requests concurrently, and tag each answer with the `id` of its request.
   Write each answer as one whole line, so that two answers never mix.
2. Return an SDK error as its numeric code.
   For the errors 14006 to 14010, add `operation_id`, and for 14009 add `partition_id`.
   The `operation_id` of 14010 is a local recovery id, so it never matches a frame `request`.
   If the SDK reports an error that has no Iggy code, return code 0 with its message.
   No `IggyError` has code 0, so the test fails, and the skip list can name the divergence.
3. Answer `unsupported` for a method, an argument value, a transport or an entity kind that the SDK does not have.
   The orchestrator reports the test as a skip with that reason.
4. Answer `failure` for a fault of the agent itself. The orchestrator reports the test as an error.
5. On `reset`, cancel every task and close every entity.
6. Never judge a result. Return what the SDK returned.

### Fault proxy

A client with `through_proxy: true` connects through a proxy listener of its own, so every frame on the listener
belongs to that client.
The proxy reads the 256-byte consensus header of each frame, and the result section of a reply that has one.
It records the frame and forwards the bytes unchanged, so checksums stay valid.
A reply takes its command and its session from the request with the same request number on its connection.
A deferred-poll cancel reuses the request number of its poll and gets no reply, so it does not replace the poll.
The proxy covers plain TCP only, so a test that uses it declares `transports: [tcp]`.

The `proxy` object has these calls:

| Call | Arguments | Effect |
| --- | --- | --- |
| `fail_frames` | `client`, `match`, `action`, `skip`, `times`, `delay_ms` | Applies the action to the matching frames. |
| `refuse_connections` | `client`, `duration_ms` | Closes each new connection to the listener for the given time. |
| `clear_rules` | `client` | Removes the rules of the client. |

`match` selects frames by `direction` (`request` or `reply`), `command` and `code`.
`skip` lets the first matching frames through, and `times` limits how many frames the rule changes, one by default.

| Action | Effect | What it models |
| --- | --- | --- |
| `drop_request` | The server never sees the request. The connection stays open. | A primary that ignores a request |
| `drop_reply` | The server applies the request, and the reply never arrives. | A reply lost after the commit |
| `close_before_forward` | The proxy closes the connection instead of forwarding. | A network failure before the server |
| `close_after_forward` | The proxy forwards the frame, then closes the connection. | A failure with an unknown outcome |
| `delay` | The proxy holds the frame for `delay_ms`. Later frames in the same direction wait behind it. | A slow network or a slow primary |

`expect_frames` matches the recorded frames of a client as a list.
`commands` and `direction` select the frames, and pings stay out unless `include_pings` is `true`.
Each recorded frame has these fields:

| Field | Meaning |
| --- | --- |
| `connection` | The number of the TCP connection, from 1. |
| `direction` | `request` or `reply`. |
| `command` | The name from `iggy_binary_protocol::codes::command_name`, such as `message.send`. A frame without a request has the consensus command, such as `ping_client`. |
| `code` | The command code. A reply has the code of its request. |
| `request`, `client`, `session` | The request number, the client id as decimal text, and the session. A reply has the session of its request. |
| `status` | The status in the reply header: zero, or the code of an error before the commit. Zero alone does not prove a commit. |
| `result` | For a reply to a metadata or consumer-offset operation, the code in its result section, such as 57 (`transient_not_committed`). Success needs a zero `status` and a zero or absent `result`. |
| `committed` | For a reply, whether the header marks a committed result (`ReplyHeader::has_committed_result`). |
| `size`, `at_ms` | The frame size, and the milliseconds since the listener started. |
| `outcome` | `forwarded`, `dropped`, `delayed`, `closed_before` or `closed_after`. |

With `--cluster-proxy-host`, the clients of a test share the listeners that the orchestrator holds for each cluster
node (see [A cluster test](#a-cluster-test)).
The proxy calls and `expect_frames` name them `@leader` and `@follower` in place of a client.
`@leader` is the metadata primary at the start of the run.
A partition primary can be on either node, so a fault test on a partition arms both listeners.

### Skips and divergences

`conformance/skips.toml` holds two kinds of entries:

```toml
[[skip]]
sdk = "python"
test = "cluster/leader_redirection.yaml#A client of the follower moves to the leader."
issue = "https://github.com/apache/iggy/issues/NNNN"

[[divergence]]
sdk = "go"
test = "streams/stream_crud.yaml#Delete a stream."
operation = 2
answer = {error = {code = 1009}}
issue = "https://github.com/apache/iggy/issues/NNNN"
reason = "GetStream returns an error for a missing stream"
```

With `--skips`, the orchestrator applies four rules:

- A skip for a missing capability or an `unsupported` answer fails the run, unless the list names it.
  A skip for a missing topology or transport never fails the run.
- A listed divergence names the `operation` where the test fails, counted from 0, and the `answer` of the SDK there.
  That failure does not fail the run, but a failure at another operation or with another answer does.
- A listed test that passes fails the run, so the list can only shrink.
- A run without `--filter` fails on an entry whose test does not exist.

Test ids are relative to the corpus directory.
To run part of the corpus, use `--filter`, not a narrower corpus directory.

### Isolation, time limits and failures

A run needs a server of its own.
At the start of a run, the orchestrator refuses a server that holds a stream or a user other than root.
Before each test, the orchestrator deletes every stream and every user except root, with its own client.
After `initial_data`, it polls each new partition until a poll succeeds, because the teardown of a deleted partition
runs after the delete commits.
After the operations and `expect_frames`, it sends `reset` to the agent and then runs `outcome`.
If the reset fails or takes more than 30 seconds, the orchestrator stops the agent and starts a new one for the next
test.
At the end of each test, it closes the proxy connections of the test and drops the frames that `delay` holds.
It also clears the rules and the recording of each shared listener, and restarts its connection count and its clock.

The orchestrator waits up to 300 seconds for the hello, because the start of the agent counts toward it.
It waits up to 90 seconds for an answer that has no time bound in the test.
A time bound in the test turns a late answer into a failure that names the time of the answer.

Each test ends with one of four verdicts:

| Verdict | Meaning |
| --- | --- |
| passed | Every operation, frame and outcome matched. |
| failed | The SDK gave an answer that does not match. |
| skipped | The run lacks the topology, transport or capability that the test needs, or the agent answered `unsupported`. |
| errored | The agent, the test file or the orchestrator failed. Examples are a `failure` answer, a timeout and an agent that exited. |

The run fails in three cases:

- A test fails, and no listed divergence names that failure.
- A test errors.
- A rule of the skip list does not hold.

### Versioning

Every test file has `schema_version: "1.0"`.
The orchestrator refuses a file with any other version.
The corpus and the orchestrator come from the same commit, so a format change updates both in one pull request.
The agent protocol has its own version, which the hello names.

### CI

The prototype adds one CI job per SDK:

- `.github/config/components.yml` gains a `conformance-infrastructure` component with the corpus, the skip list, the
  orchestrator and the run script.
  It also gains one `conformance-<sdk>` component per SDK, which depends on that SDK, the server and the
  infrastructure.
  A change to an SDK, its agent, the corpus or the orchestrator then runs only the jobs that it affects.
- The Rust pre-merge job builds `iggy-conformance` with the other runtime binaries, and later jobs download it.
- `.github/workflows/_test_bdd.yml` runs each `conformance-<sdk>` task through
  `.github/actions/conformance/pre-merge`.
  The action installs the SDK toolchain with the existing setup action and runs
  `scripts/run-conformance-tests.sh <sdk>`.
  It writes the matrix of the SDK to the job summary and uploads the reports.
- A `conformance-matrix` job joins the reports of all SDKs into one table in the summary of the workflow run.
- The agents run on the native toolchain of each SDK, not in the BDD images.
  PHP builds its agent in a Docker image, and Swift uses Docker only where no Swift toolchain is installed.

### Migration from Gherkin

Each feature in `bdd/scenarios/` becomes one YAML file.
Both suites run until the YAML file passes on every SDK that ran the feature before.
Until then, a result that differs between the two suites shows a gap in one of them.
Then one pull request deletes the feature file and its step code in every SDK.

The prototype converted all nine features, in this order: `session_contract`, `stream_crud`, `basic_messaging`,
`raw_command`, `stream_options`, `resource_options_patch`, `metadata_changes`, `deferred_polling` and
`leader_redirection`.
Four features met the rule: `stream_crud`, `raw_command`, `resource_options_patch` and `leader_redirection`.
The prototype deleted them, with 4,634 lines of feature files, step code and runner configuration.
Five features stay, because an SDK that ran them before has a divergence or a skip on the YAML file:

| Feature | Blocked by |
| --- | --- |
| `basic_messaging` | a Python divergence and a PHP skip |
| `session_contract` | a C++ divergence and a PHP skip |
| `stream_options` | a Node.js divergence, and PHP |
| `metadata_changes` | PHP |
| `deferred_polling` | a skip in C# and in Node.js, and PHP |

### Keeping the suite honest

The MongoDB paper shows that a suite enforces only what its runners assert.
Three rules keep the assertions in one place:

1. The matcher and the canonical forms live only in the orchestrator.
2. People write the expected values, and `record` output is only a starting point (see
   [Why people write the expected values](#why-people-write-the-expected-values)).
3. A CI check refuses a pull request that changes an SDK and also changes an existing test or adds a skip-list entry.
   A coding agent that ports an SDK change then cannot weaken a test in the same pull request.
   The prototype does not have this check yet.

Two more safeguards are not built yet:

- A weekly mutation run starts the Rust agent with a defect, such as a dropped result field or a wrong error code.
  If the suite still passes, a test is missing.
- A differential mode runs each file on the Rust agent and on the SDK agent and compares whole results.
  It masks time and id fields.

### Prototype

The branch [`conformance-suite`](https://github.com/apache/iggy/tree/conformance-suite) holds a prototype of this
design, based on `master` at `2d570f894`.
Its corpus has 14 files and 49 tests in these areas: session, streams, topics, messaging, raw commands, options,
metadata changes, deferred polling, faults and cluster.
Nine files restate the Gherkin features, and five are new.

| SDK | Passed of 49 | Divergences | Skips |
| --- | ---: | ---: | ---: |
| Rust | 49 | 0 | 0 |
| Java | 49 | 0 | 0 |
| C# | 48 | 0 | 1 |
| Go | 48 | 1 | 0 |
| Swift | 45 | 2 | 2 |
| Python | 43 | 2 | 4 |
| Node.js | 43 | 1 | 5 |
| C++ | 43 | 2 | 4 |
| PHP | 9 | 28 | 12 |

The skip list has 64 entries: 36 divergences and 28 skips.
Their issues are not filed yet, because each one depends on the expected answers that this RFC settles.

| Part | Lines |
| --- | ---: |
| Orchestrator | about 5,500 |
| Rust agent | 1,095 |
| Eight other agents, with their build files | 15,493 |
| Corpus, with license headers | 1,750 |

The Go agent has 2,249 lines, against more than 7,000 lines of Go step code in `bdd/go/`.
A run of the corpus takes about 150 seconds per SDK, without the agent build.
Four tests take about 120 seconds of that, because each one waits about 30 seconds for an SDK deadline.

### Implementation plan

After acceptance, the prototype lands as a stack of pull requests that a reviewer can read one at a time:

1. The orchestrator, the Rust agent, the fault proxy and the corpus, with a CI job for Rust.
2. The Go agent, and the CI pattern that runs one job per SDK and joins the reports.
3. One pull request per remaining agent: Python, C#, Java, Node.js, C++, PHP and Swift.
   The maintainers of each SDK review its agent, and each entry of the skip list gets an issue.
4. One pull request per Gherkin feature that meets the deletion rule.
   It comes only after the conformance jobs of the SDKs that ran the feature pass on `master`.

## Drawbacks

- The SDK teams own more code, about 16,600 lines of agent code in nine languages in the prototype.
  Every new method needs an entry in each agent, or an `unsupported` answer.
- An agent translates SDK results into the canonical form, and a wrong translation can hide a divergence or invent
  one.
  The prototype has three agent choices that need a decision (see [Unresolved questions](#unresolved-questions)).
- The Rust SDK types define the canonical forms.
  An SDK with a different idiom, such as a Go error for a missing stream, shows up as a divergence.
  Each such case needs a decision, and some decisions change a public SDK API.
- CI runs nine more jobs, each about 150 seconds plus the agent build.
  Each new fault test adds about 30 seconds per SDK, until the SDKs take that deadline from their configuration.
- Both suites run during the migration, so CI time and test upkeep go up until the last feature leaves `bdd/`.
  Five features cannot leave until PHP and four other SDKs pass their YAML files.
- YAML with operators such as `$$capture` is harder to read than a Gherkin sentence.
  The `description` of each test keeps one sentence for the reader.
- The proxy covers plain TCP only, so QUIC, WebSocket, HTTP and TLS get no fault tests.
  The prototype runs every test over TCP.
- Each test crosses a process boundary.
  When an agent hangs or crashes, the test errors, and the cause can be in the agent, not in the SDK.

## Rationale and alternatives

### Why one orchestrator and thin agents

MongoDB gives each driver a runner that parses YAML, keeps the entity map, matches results and handles events.
Every runner is then large and can be lax in its own way, as the Node.js runner was (see [Prior art](#prior-art)).
For Iggy, that means nine YAML parsers and nine matchers that can drift apart.

This design puts the parser, the matcher and the canonical forms in one orchestrator, for four reasons:

1. One matcher gives every SDK the same strictness.
2. Agent code grows with the API, not with the number of tests.
   The client traits in `core/common/src/traits/` have about 54 methods.
3. Fault injection and frame recording need no code in any SDK.
4. A contributor or a coding agent can write a dispatch entry from the Rust reference, while the tests stay with the
   maintainers.

The cost is a process boundary and a canonical form for every result.
The canonical form also pays for itself, because it makes the shape of each result an explicit contract.

### Why the Rust SDK types define the canonical forms

The Rust SDK lives next to the server in `core/sdk/`, and Python, C++ and PHP wrap it.
Its result types already derive `Serialize`, and the HTTP API already returns them as JSON.
The forms live in one place, `core/conformance/src/canonical.rs`.
Each conversion destructures its Rust type, so a new field fails the build until the form takes it or drops it by name.
The forms drop some fields, such as `Partition.members`, `Partition.in_sync_members` and `OptionSpec.default_value`.

The forms depart from the raw serde form in three places:

- Sizes are byte counts in `size_bytes`, so every SDK reports the same number.
- Every u64 and u128 value is decimal text, because JavaScript and PHP round a JSON number above 2^53.
  The metadata changes form keeps the exact encoding of its fixture, with a u128 as 32 hex characters.
- Options and headers use the HTTP JSON form of the shared codec fixtures, which all eight foreign SDKs already copy.

This RFC rejects two alternatives:

- The names and the bodies of the HTTP API.
  They do not depend on a language, but the HTTP API has no readers, no consumers and no retained operations.
- A neutral schema, written for the suite.
  It is a second description of every type, and no compiler ties it to the Rust types.

### Why a proxy and not server failpoints

The server has no failpoints, and new failpoints mean server changes and a test build of the server.
A proxy needs no server change, works for every SDK, and sees exactly what each SDK puts on the wire.
Server failpoints can come later for faults that a proxy cannot make, such as a crash between the journal write and the
reply.

### Why YAML and not Gherkin

A Gherkin phrase needs step code in every SDK, and that code decides how strictly it asserts.
A YAML test needs no SDK code, and the orchestrator asserts it.
MongoDB also tried Cucumber first and dropped it.

If Iggy keeps Gherkin and adds features, each new phrase costs nine implementations, and coverage keeps drifting.
The shared codec fixtures are no substitute, because they cover bytes, not call sequences, errors, retries or timing.
They stay as they are, and the canonical forms reuse them.

### Why people write the expected values

`record` prints what the Rust agent observed.
If expected values come from `record` without a review, every bug of the Rust SDK becomes the contract.
A differential mode, which compares each SDK with Rust, has the same weakness, so it starts as a report, not a gate.

### Impact of not doing this

The differences in [Motivation](#motivation) stay hidden until users find them.
The retained-operation contract of the five native SDKs stays without a shared test.
Each new cross-SDK scenario keeps costing step code in nine SDKs, and coverage keeps drifting.
A new SDK, or a port of a Rust change to another SDK, has no shared gate.

## Prior art

The MongoDB unified test format is the model for this design. Two sources describe it:

- A. Jesse Jiryu Davis, Jeremy Mikola and Jeff Yemin, "The Polyglot's Dilemma: Conformance Testing a Dozen Specs in
  as Many Languages", ISSRE 2026, <https://arxiv.org/abs/2608.18039>.
- MongoDB Research, "How to test MongoDB drivers conform to a dozen specs",
  <https://www.mongodb.com/company/research/how-to-test-mongodb-drivers-conform-to-a-dozen-specs>.

The paper reports these results:

- The corpus holds almost 1,500 files and 124,000 lines across a dozen specs.
- When the format arrived in 2020, seven drivers replaced their runners per spec and deleted 22,000 lines between them.
- Four of five drivers had fewer CRUD spec bugs after the change.
  Node.js had more, because its runner did not fully assert `BulkWriteResult`.

| MongoDB | This design |
| --- | --- |
| `schemaVersion` | `schema_version` |
| `runOnRequirements` | `run_on`: topology, transports and agent capabilities |
| `createEntities` | `create_entities`, and `save_result_as` for readers and consumers |
| `initialData` | `initial_data`: calls that the orchestrator makes before each test |
| `testRunner` object | the `orchestrator`, `admin` and `proxy` objects |
| `failPoint` with `failCommand` | `fail_frames` on the proxy |
| `expectEvents`, from command monitoring and connection pool events | `expect_frames`, from the proxy |
| `outcome` | `outcome`, read with the orchestrator's own client |
| one runner per driver | one orchestrator, and one thin agent per SDK |

The Protocol Buffers conformance tests use the same split as this design.
One test runner sends each request over a pipe to a small program in the language under test, and it judges the answer.
Each language keeps a list of known failures. When a listed test passes, the runner fails.
The skip list of this design follows that rule.

The gRPC interoperability tests name each test case in one shared document.
Each language implements the cases as a client and a server, and CI runs the clients of each language against the
servers of the others.
That model is close to the Gherkin suite: the names are shared, but the logic of each test lives in each language.

Two Iggy layers stay as they are.
The shared codec fixtures in `core/common/fixtures/` test the byte encoding in every SDK.
The fake-server tests of the Rust SDK define the retained-operation contract, and the three fault tests of the corpus
port them to a real server.

## Unresolved questions

### Open decisions

1. The expected answer where SDKs differ.
   This RFC proposes the Rust SDK types as the reference.
   Each divergence then becomes an issue for its SDK, unless the discussion changes the expected answer.
   The first cases are the Go error for a missing stream, the Python create calls that return `None`, and the C++ errors
   without codes.
   To accept two answers for one call, the format needs an expectation that matches a result or an error.
2. The canonical names.
   The prototype uses the names of the Rust methods and their parameters, with the exceptions in
   [Operations](#operations).
   The names of the HTTP API are the main alternative.
3. Where the agents live and who owns them.
   The prototype puts every agent under `conformance/agents/<sdk>/`.
   Node.js and C# keep their BDD code inside their SDK trees, and their agents can do the same.
   The maintainers of each SDK must accept its agent.
4. Errors without an Iggy code.
   Agents answer them as code 0, so a test that expects a named error fails.
   Some read calls of the C++ SDK, such as `GetStreams`, report their errors this way today.
5. Three choices of the prototype agents:
   - The Node.js SDK reads an option without its kind, so the Node.js agent takes the kind from the JavaScript type of
     the value.
   - The Node.js SDK reads a stream without its topics.
     If the topic count is 0, the Node.js agent writes `topics: []`. Otherwise it leaves the list out.
   - The Swift SDK names an operation with a 128-bit handle, but the protocol carries a u64.
     The Swift agent sends the low 64 bits and keeps the whole handle for `abandon_operation`.
6. A deadline for fault tests.
   A fault test waits for the operation deadline of the SDK, about 30 seconds in Rust.
   With one configuration key for that deadline in every SDK, these tests take a few seconds.
   An alternative is an optional deadline per call in agent protocol 1.
   The Rust SDK has `with_binary_deadline` for this, but it is `#[doc(hidden)]`.
7. The deletion rule and gaps that last.
   Under the rule, a feature stays in `bdd/` until every SDK that ran it passes its YAML file.
   PHP blocks all five remaining features, and it alone blocks `metadata_changes`.
   If the skip list names such a gap with an issue, can the feature leave?
8. Agent protocol versions across releases.
   The orchestrator can accept only its own protocol version, or also older ones to run the agent of an older SDK
   release.
9. The error of an uncertain join.
   Rust fails the join with `operation_uncertain` (14006) and abandons it at once, but 14006 names a retained operation.
   Swift fails it with `transient_not_committed` (57).
10. Background calls.
    `start` answers before the SDK sends, so the corpus orders background calls with `wait`, and a slow agent can flip
    a test.
    A proxy call that waits for a matching frame can replace `wait`.

### Out of scope

These can come later, each on its own:

- The weekly mutation run and the differential mode in [Keeping the suite honest](#keeping-the-suite-honest).
- Faults on QUIC, WebSocket and TLS connections, and runs of the corpus over transports other than TCP.
- Divergences that show only under load. They need soak runs against a real cluster.
- Server failpoints for faults that a proxy cannot make.
- New tests for producer idempotence, reconnect and session rebind, retained operations and consumer group membership.
  These areas have the most hidden behavior after the migration.

### Simulator versus integration coverage

The deterministic simulator runs the replicas of a cluster in one process, with a simulated network and storage.
It cannot run the SDKs that are not written in Rust, so it cannot replace this suite.
It can cover the server side of the contracts that the suite depends on, such as request deduplication across view
changes.
The suite covers the client side against a real server over real sockets, on a single node, a node without quorum and
a two-node cluster.
Timing under load and faults on transports other than TCP stay outside both.
