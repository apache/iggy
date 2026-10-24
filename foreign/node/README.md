<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Apache Iggy Node.js Client

Apache Iggy Node.js client written in typescript, it currently only supports tcp & tls transports.

Disclaimer: although all iggy commands & basic client/stream are implemented this is still a WIP, provided as is, and has still a long way to go to be considered "battle tested".

note: This lib started as _iggy-bin_ ( [github](https://github.com/T1B0/iggy-bin) / [npm](https://www.npmjs.com/package/iggy-bin)) before migrating under iggy-rs org. package iggy-bin@v1.3.4 is equivalent to @iggy.rs/sdk@v1.0.3 and migrating again under apache iggy monorepo ( [github](https://github.com/apache/iggy/tree/master/foreign/node) and is now published on npmjs as apache-iggy

note: previous works on node.js http client has been moved to [iggy-node-http-client](<https://github.com/iggy-rs/iggy-node-http-client>) (moved on 04 July 2024)

## install

```bash
npm i --save apache-iggy@edge
```

Use matching SDK and server builds from the coordinated VSR release. Older builds are incompatible. This release requires fresh server storage (`IGGY-RESOURCE-OPTIONS-1`). Existing incompatible data is refused unchanged. For a local server with the example credentials, follow the [example setup](../../examples/node/README.md#running-examples).

## basic usage

### Response frame limit

**Compatibility note:** response frames larger than `maxResponseFrameSize` (default 64 MiB) are rejected and close the connection. Raise the limit in the client configuration when polling very large batches.

### VSR framing

The SDK speaks the VSR wire protocol exclusively and requires an Iggy VSR
server:

```typescript
import { SimpleClient, getRawClient } from "apache-iggy";

const config = {
  transport: "TCP" as const,
  options: { host: "127.0.0.1", port: 8090 },
  credentials: { username: "iggy", password: "iggy" },
};
const client = new SimpleClient(getRawClient(config));
const stats = await client.system.getStats();
```

Codes absent from the SDK command table use `Operation::NonReplicated` and
carry the command code in the request header's reserved field. The server
remains authoritative for classifying or rejecting extension commands.

Sends always carry an explicit partition id. The client resolves
`Partitioning.Balanced` round-robin and `Partitioning.MessageKey` as XXH32
(seed 0) of the key modulo the topic's partition count, as the Rust SDK does.
The count is cached per topic. The client reads it again after a refused send,
and after the client deletes a stream or a topic or creates or deletes
partitions. A raw `SendMessages` command without an explicit partition id
fails with error code 5 before the client sends it.

Partition commands carry a context containing the partition incarnation, owner
generation and metadata operation. Pass the context of an earlier poll reply to
keep an offset write or continuation tied to that history. Otherwise the client
captures the route context. Non-admission retries and uncertain-operation replay
preserve the first captured context, request identity and payload. Route refreshes
change the destination. Lifecycle error 88 retries with a fresh ID and the same
context. A send without caller-supplied context can refresh history once after
error 87; explicit contexts return that error. A new call can capture newer
metadata progress.

Route discovery uses command 105. Sends, polls and offset reads/writes use the
partition primary; other commands use the metadata primary. One logical client
owns a shared session, a monotonic request counter and one multiplexed link per
replica. Healthy links survive primary changes. Each link dials the address
that its node advertises in the cluster roster. With a one-node roster, the
link dials the configured endpoint instead. A single node behind a port
mapping or a port-forward thus stays reachable. The legacy `poolSize` option
accepts only one logical client; it does not limit replica links or concurrency.
The public `Client._pool` property has been removed. Applications should use the
client methods directly; the client owns its replica connections.

A backup replica can miss a stream, topic or partition that the metadata primary
already created. When such a replica replies "not found" (error 1009, 1010,
2010, 2011 or 3007) to a route lookup or a partition command, the call looks up
the route again and retries. If the deadline ends the call first, it fails with
error 58. Only a "not found" reply from the metadata primary is final. A leader
change can make the client's view of the metadata primary out of date. If the
client believes that the replica that replied is the metadata primary, it reads
the cluster roster from that replica. If the client has no link to the metadata
primary, it also reads that roster. If that roster names this replica as the
metadata primary, the reply is final.

VSR authentication translates password and personal-access-token login into the
Register handshake. Replacement links bind the existing session. Confirmed
session expiry requires a new Register; an uncertain mutation is never replayed
under that new session. An eviction that ends the session does not prove the
outcome of a mutation that the client already wrote. That call ends with
`OperationUncertainError`. A stale client eviction drops only its link, because
the server keeps the session. The client binds the same session on a new link,
as after any lost link, so retained operations and group membership stay.
Explicit relogin logs out the previous session first.

The client signs in by itself as the last sign-in of the caller, or with the
configured `credentials` while the caller never signed in. After a logout, or
after the server refuses those credentials, it does not sign in by itself, and
a call fails with status 40 until the caller signs in again. A sign-in or
logout of the caller ends each earlier call that has to wait for a new session:
with status 40 if it sent nothing, or with `OperationUncertainError` if its
outcome is unknown. An auto-commit or Next poll, which has no operation ID, ends with status 57 instead. A typed send and a group poll can send more than one
command. If the caller signs in or logs out before the next command of such a
call, the call fails with status 40. Only a call that waits for the client's own sign-in goes on
under the new session. If the server refuses that sign-in, the call fails with
status 40, and its `cause` holds the refusal. A call whose deadline ends while
it waits for a session fails with status 58. So does a call that replicas refuse with 89 until its deadline.

A reply with status 30 (`StaleClient`) says that the server no longer attaches
the link to the session. This happens, for example, after the user was deleted
or deactivated. A 30 refusal before commit fails a first attempt with that
error. After an earlier written attempt, it does not prove the outcome, so that
call ends with `OperationUncertainError`. The client closes that link and binds
the session on a new link, as after any lost link. A replica that replaced its
client table refuses a live session the same way, so the bind decides. A live
session stays, with its retained operations and group membership. An ended
session refuses the bind, and the client signs in again.

A committed 30 comes from the replicated session table, so that session ended.
It is a proved outcome, like any other committed rejection, and the client
drops the session at once and signs in again.

### Mutation recovery and limits

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

Mutations share one FIFO lane per partition history, with a separate metadata
lane. An uncertain mutation remains retained and blocks later mutations in its
lane. `OperationUncertainError.operationId` and `LaneBlockedError.operationId`
are exact `bigint` values. Use `resolveOperation(operationId, { deadline })` to
replay the retained frame, where `deadline` is an absolute `Date.now()` timestamp
in milliseconds. The default operation budget is 30 seconds, including admission,
routing, link waits and retries.

Resolution requires correlated committed-result proof. A proved business
rejection returns the original error and releases the lane. An unproved denial
keeps an uncertain operation retained. The client keeps the proved outcomes of
its last `maxPendingOperations` replays, also across a session reset. If a
resolver already proved the outcome, a later `resolveOperation` of the same ID
returns it without a request. A `resolveOperation` call that waits for another
replay of the same ID and reaches its deadline fails with
`OperationUncertainError`.

`abandonOperation(operationId)` releases the client lane without proving that
the mutation failed. A replay that is already running keeps the lane until it
ends, and the client keeps the outcome that it proves. Logout, session reset and
destruction discard retained operations with their outcomes still unknown.
`OperationNotRetainedError` does not establish the outcome: a reset or an
abandon came before a proved outcome, or the proved outcome aged out.

**Breaking change:** `Client.destroy()` aborts in-flight calls immediately and
discards retained operations. It does not wait for outstanding calls to finish.
Await those calls before destroying the client when their results are needed.

Auto-commit and Next polls hold their partition lane only during the active call.
Error 57, a lost reply or another unknown outcome ends the call immediately with status 57 and no
retained operation ID. These polls never replay after uncertainty. Known
non-admission errors 58/89 remain retryable; pure reads may retry lost replies.
Before uncertainty, lifecycle error 88 retries with a fresh ID within the same
deadline. After uncertainty, only committed-result proof can release the lane.

Configure `maxPendingOperations` (default 256) and `maxPendingOperationBytes`
(default 64 MiB) on the client. Admission waits before allocating an ID or freezing
a mutation frame. Both limits must be positive. The byte budget includes the
256-byte header and body; smaller positive budgets reject mutations that cannot
fit while reads and login remain available. Oversized frames fail locally.
Completion, abandonment and reset release the operation slot. Writer aliases keep bytes charged until they release
the frame. These limits cover admitted active/retained frames, not caller buffers,
queued API input snapshots, or read/control bodies. `sendBinaryRequest` preserves
its immediate payload snapshot before awaiting a custom provider.

An explicit sign-in uses its caller's credentials and supersedes automatic or
queued sign-ins. A failed explicit attempt, including invalid input, timeout or
cancellation, withdraws credentials from automatic sign-in until another explicit
sign-in succeeds. This also discards a session whose Register succeeded when a
later step of the explicit sign-in fails. Earlier calls keep their original
lineage; only SDK recovery continues them under a replacement session.

An established session has its own lifetime. Password or PAT changes and PAT
expiry do not end it. Logout, session lease expiry, or user deactivation end the
session and prevent its bind proof from restoring it.

The client pings every `heartbeatInterval` milliseconds, 5000 by default, which
keeps an idle session alive when the server's `[heartbeat]` eviction is enabled.
`heartbeatInterval` also accepts a duration expression such as `"10s"` or
`"1h 30m"`, like the Rust SDK.
With the default heartbeat settings, a client that sends nothing for 36 s
(1.2 x the 30 s interval) stops renewing its session, and the server ends the
session about 36 s later. The verifier checks once per interval and also evicts
the connection of a consumer-group member that is silent past 36 s. If the
client interval exceeds 36 s, the verifier evicts an idle group member. If it
exceeds about 72 s, or is 0 to disable client heartbeats, any idle session
expires. Any other unusable value is rejected instead of silently disabling the
heartbeat.

A replica link redials on its own after it drops, with jittered exponential
backoff. `reconnect.maxRetries` limits one burst of redials, not the life of
the client. While the client stays signed in, each heartbeat restarts a link
that used its budget, with a fresh budget. A request does not redial a link. It
retries within its deadline until the link returns. If the deadline ends before
the request was sent and no replica refused it, it fails with status 58.
`destroy()` and logout stop the restarts.

When every link used its budget, a request fails with `NotConnectedError` and
does not wait for its deadline. When no endpoint accepts a dial before its retry
budget or its deadline ends, a sign-in also fails with `NotConnectedError`, and
its `cause` holds the last dial error. In both cases a later call can succeed,
because the heartbeat restarts the links and each sign-in dials again. If
reconnection or heartbeats are disabled, nothing restarts a link that used its
budget, so the request fails with a plain `Error` instead.

When a poll fails with status 30, 40, 57, 58 or 89, `singleConsumerStream` and
`groupConsumerStream` poll again after `interval`. They also poll again after a
poll deadline, after a poll with unknown progress, after `NotConnectedError` and
after `ConnectionClosedError`. That error reports a lost connected socket. The
client binds the session again, signs in again,
reconnects or routes again after these on its own. Any other error ends the
stream. The server can also refuse a sign-in with status 40, for example for an
inactive user. So after a 40 the stream waits for the next sign-in, and a
refused sign-in ends the stream. If the reply of an auto-commit poll is lost,
the stream can skip that batch.

`singleConsumerStream` and `groupConsumerStream` sign in before they return the
stream, and `groupConsumerStream` also joins its group. If the sign-in or the
join fails, the call destroys its client and rejects with that error. If no
endpoint accepts a dial before the retry budget or the 30 second sign-in
deadline ends, the error is `NotConnectedError`. When a stream closes, it
destroys its client. A group stream that is still signed in first leaves its
group, so that the server gives its partitions to the other members at once. If
the leave fails or takes more than 5 seconds, the membership ends when the
session lease expires. If the client of a destroyed stream has no session, or
loses it, the stream destroys the client at once. It does not wait for a poll
or a sign-in that still runs.

`sendBinaryRequest(code, payload)` sends an arbitrary command code. Known replicated commands use their registered operation, while unknown codes reach the server as non-replicated requests and are rejected by servers that do not register them.

```typescript
import { ResponseError } from "apache-iggy";

try {
  await client.sendBinaryRequest(60_000, Buffer.from("opaque request"));
} catch (error) {
  if (error instanceof ResponseError) {
    console.error(error.commandCode, error.errorCode);
  }
}
```

The client includes its npm package version and the binary wire protocol
version in VSR registration. An incompatible server rejects registration with
a protocol-version error instead of accepting a mismatched wire contract.

```ts
import { Client } from "apache-iggy";

const credentials = { username: "iggy", password: "iggy" };

const client = new Client({
  transport: "TCP",
  options: { port: 8090, host: "127.0.0.1" },
  credentials,
});

const stats = await client.system.getStats();
```

### Connection strings

Every client constructor (except `SimpleClient` see note) also accepts a
connection string instead of a config object:

```ts
import { Client } from "apache-iggy";

const client = new Client("iggy://iggy:iggy@127.0.0.1:8090");
const stats = await client.system.getStats();
```

Supported schemes are `iggy://` (TCP, default) and `iggy+tcp://`. Credentials
are `username:password` or a single personal access token. Options mirror the
other SDKs: `tls`, `tls_domain`, `tls_ca_file`, `reconnection_retries`,
`reconnection_interval`, `heartbeat_interval`, `nodelay`, `max_pending_operations`
and `max_pending_operation_bytes`. `reestablish_after`
is accepted for format compatibility but has no Node equivalent.

note: `SimpleClient` does not accept a connection string: it wraps an existing
`RawClient` instance rather than building one from configuration. Pass the
connection string to `Client`, `SingleClient` or `getRawClient` and hand the
resulting raw client to `SimpleClient` if needed.

### option limits

| option | limit |
| --- | --- |
| `reconnection_retries` | integer up to `4294967295` (u32 max); larger values are rejected like Rust's u32 overflow, and `unlimited` maps to this ceiling. Defaults to unlimited |
| `heartbeat_interval` | duration up to `2147483647ms` (Node's largest timer delay); `0` disables heartbeats |
| `reconnection_interval` | positive duration (`ms`, `s`, `m`, `h`) up to `2147483647ms` (Node's largest timer delay); zero spellings are rejected. Defaults to `1s` |
| port in the authority | decimal up to `65535` |

Durations accept the same expressions as the Rust SDK, for example `500ms`,
`10s`, `1h 30m`, `5d`, `2w`, `1y`; matching is case-insensitive and
`0`, `unlimited`, `disabled` and `none` map to zero. Unit-less numbers such as
`5` are rejected.

### resource options

A stream can carry its own data encryption key. Build it with `StreamEncryptionKey.from` from exactly 32 random bytes, or from their canonical padded base64, and pass it to `stream.create` as `encryptionKey`. Never derive the key from a password, because its fingerprint is readable by anyone who can read the stream. A key that is not 32 bytes or canonical base64 is refused with error 70 before anything is sent.

A `StreamEncryptionKey` keeps its own copy of the bytes and prints only its fingerprint through `util.inspect`, `JSON.stringify` and string conversion, so create arguments carrying it can be logged. Entries passed through `options` print as given, so an `encryption_key` entry there is refused with error 70 before anything is sent.

The key travels inside the create request over whatever transport the client uses, so connect over TLS when the key must stay confidential. The server stores the key wrapped and never returns it: reads report only `encryptionKeySource`, `encryptionKeyFingerprint` and `encryptionKekFingerprints`. Without a key, a server with encryption enabled generates one for the stream and reports the source `server`.

```ts
import { randomBytes } from "node:crypto";
import { Client, StreamEncryptionKey } from "apache-iggy";

const client = new Client({
  transport: "TLS",
  options: { host: "localhost", port: 8090 },
  credentials: { username: "iggy", password: "iggy" },
});

const key = StreamEncryptionKey.from(randomBytes(32));
const stream = await client.stream.create({ name: "orders", encryptionKey: key });
console.log(stream.encryptionKeySource); // "stream"
console.log(stream.encryptionKeyFingerprint === key.fingerprint()); // true
```

`stream.update`, `topic.update` and `user.update` change only the keys they name and leave every other setting as it is, so an update without `name` keeps the name. `reset` returns a key to the server default. Use the `StreamOptionKey`, `TopicOptionKey` and `UserOptionKey` constants to name keys.

Only the topic keys `message_expiry` and `max_topic_size` can be reset. Settings fixed at create, such as the topic compression algorithm, the partition resize policy or the stream key, have no update field.

The SDK checks the typed fields and `reset` before sending, in the server's order and with its error codes (4041 for an unknown key, 4105 for a read-only key, 4103 for a create-only key, 4104 for a key without a default, then the name, username or status error). For a key this build has no field or constant for, pass `options` entries and `rawReset` names. The SDK sends those as they are and the server decides, so a refusal comes back from the server.

The server returns 4108 when a non-root caller tries to rename root or change root's status, or when root tries to deactivate itself.

```ts
await client.topic.update({
  streamId: "orders",
  topicId: "events",
  messageExpiry: 7_200_000_000n,
  reset: ["max_topic_size"],
});
```

Reads return every setting in `options` (keys the client set) and `derivedOptions` (keys the server filled). `u64` values stay `bigint`, so values above 2^53 keep their precision. `system.describeOptions({ scope: OptionsScope.Stream })` lists the keys a server accepts per scope, with `updatable`, `resettable`, `secret`, `readOnly` and `critical`.

## use sources

Cluster polling uses command 15 (`BindSession`), route discovery 105 and poll
100. Commands 14, 103, 104 and 123 are retired. Pause consumers for the coordinated
server/SDK upgrade and restart them to rejoin their groups. There is no fallback
to retired polling commands.

Stream, topic and user updates use the generic update command 16. The former update commands 35, 204 and 304 are retired.

### Install

```bash
npm ci
```

### build

```bash
npm run build
```

### test

note: use env var `IGGY_TCP_ADDRESS="host:port"` to set the server
address for e2e tests. bdd tests need more variables, see below.

#### unit tests

```bash
npm run test:unit
```

#### e2e tests

e2e test expect an iggy-server at tcp://127.0.0.1:8090

The resource options tests also expect the server to run with encryption enabled and a config key (`IGGY_ENCRYPTION_ENABLED=true` and `IGGY_ENCRYPTION_KEY` set to the base64 of 32 bytes), so a stream created without a key reports a server-generated one.

```bash
npm run test:e2e
```

#### bdd tests

the bdd suite has no defaults and fails when `IGGY_TCP_ADDRESS`,
`IGGY_ROOT_USERNAME` or `IGGY_ROOT_PASSWORD` is missing. from the repository
root run

```bash
./scripts/run-bdd-tests.sh node
```

the script starts the server and sets every variable, so none of them have to be
exported by hand. to iterate against a server you started yourself, see
[src/bdd/README.md](./src/bdd/README.md).

#### run all test

`npm run test` runs unit, bdd and e2e tests suite against an iggy-server at
tcp://127.0.0.1:8090, started with the same root credentials

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
  npm run test
```

### lint

```bash
npm run lint
```
