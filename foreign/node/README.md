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

Use matching SDK and server builds from the coordinated VSR release, with a minimum binary protocol version of 0.12.1. Older builds are incompatible. This release requires fresh server storage (`IGGY-METADATA-CHANGE-LOG-1`). Existing incompatible data is refused unchanged. For a local server with the example credentials, follow the [example setup](../../examples/node/README.md#running-examples).

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

`sendBinaryRequestToReplica(code, payload, replica)` sends a raw command to one node of a cluster, named by the replica id that cluster metadata returns, and `sendCommand` takes the same id as the `replica` option.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. Replicated commands, polls, fetches, consumer-offset reads and session-control codes fail with error code 3 before anything is sent. An id that cluster metadata does not name fails with error code 6. An unreachable node is retried until the request deadline, and a `NotPrimary` (89) reply is returned rather than sent to another node.

Partition commands carry a context containing the partition incarnation, owner
generation and metadata operation. Pass the context of an earlier poll reply to
keep an offset write or continuation tied to that history. Otherwise the client
captures the route context. Non-admission retries and uncertain-operation replay
preserve the first captured context, request identity and payload. Route refreshes
change the destination. Lifecycle error 88 retries with a fresh ID and the same
context. A send without caller-supplied context can refresh history once after
error 87. Explicit contexts return that error. A new call can capture newer
metadata progress.

Route discovery uses command 105. Sends, polls and offset reads/writes use the
partition primary. Other commands use the metadata primary. One logical client
owns a shared session, a monotonic request counter and one multiplexed link per
replica. Healthy links survive primary changes. Each link dials the address
that its node advertises in the cluster roster. With a one-node roster, the
link dials the configured endpoint instead. A single node behind a port
mapping or a port-forward thus stays reachable. The legacy `poolSize` option
accepts only one logical client. It does not limit replica links or concurrency.
The public `Client._pool` property has been removed. Applications should use the
client methods directly. The client owns its replica connections.

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
session expiry requires a new Register. An uncertain mutation is never replayed
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
outcome is unknown. A Next poll that commits nothing, which has no operation ID, ends with status 57 instead, and an auto-commit poll ends with `FetchProgressUncertainError` (see below). A typed send and a group poll can send more than one
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

### Deferred polls

`message.poll` takes `deferred` options. With them, the client sends command
106, and the server waits for messages before it replies. Options that you do
not give take the values of `DEFAULT_DEFERRED_POLL_OPTIONS`, which are the
defaults of the Rust SDK. Durations are in milliseconds.

| option | default | limits |
| --- | --- | --- |
| `maxWait` | `1000` | up to 600 s. When `minCount` messages are there, or when `maxWait` ends, the server replies |
| `minCount` | `1` | from 1 to the `count` of the poll |
| `maxBytes` | `1048576` | at least 344 bytes, the size of a reply with one message header |
| `requestTimeout` | `11000` | from 1 ms to 630 s. If `maxWait` is above 0, at least `maxWait` plus 1 s |

An option outside its limits fails before anything is sent: `minCount` with
error 4009, `maxWait` and `requestTimeout` with error 3, and `maxBytes` with
error 80. The call gives up at `requestTimeout`, not at the 30 s operation
budget. A link fails on a reply larger than `maxResponseFrameSize`, so the
client lowers `maxBytes` to that size minus the 256-byte header.

A client parks at most 64 deferred polls and fetches at once. Another
one waits for a free slot within its own deadline. If the server refuses one
with 90, because the session parks too many requests, the client pauses and
sends it again within the deadline.

The `signal` option takes an `AbortSignal` that cancels the call. If the
request already waits on the server, the client sends command 108 on the same
link. The server then drops the request without a reply. The client ignores a
reply that comes later. A raw 106 gets the same routing, limits and
cancellation as the typed calls. A raw 108 fails with error 3 before anything
is sent.

A consumer-group poll with deferred options needs a partition. Without one, it
fails with error 3 before anything is sent. A member reads all its partitions
with `message.reader`.

An auto-commit or Next deferred poll holds the progress lane of its partition
while it waits on the server, up to `maxWait`. Offset reads, stores and deletes
of the same client on that partition wait behind it. Sends do not take the
progress lane, so a parked poll does not delay them.

### Polling one partition

`message.poll` with a `strategy` reads one batch of one partition with
`PollOptions`. It waits on the server until `minCount` messages are ready, the
reply reaches `maxBytes`, or `maxWait` ends, and returns what is ready, which
can be nothing.

```typescript
import { CommitPolicy, Consumer, PollingStrategy } from "apache-iggy";

const consumer = Consumer.Single;
const options = { count: 100, maxWait: 5_000 };
const polled = await client.message.poll({
  streamId, topicId, partitionId: 1, consumer, strategy: PollingStrategy.Next, options
});
for (const message of polled.messages) console.log(message.payload.toString());

const checkpoint = polled.checkpoint();
if (checkpoint) await client.offset.storePosition({ streamId, topicId, consumer, checkpoint });

const next = await client.message.poll({
  streamId, topicId, partitionId: 1, consumer, strategy: polled.continuation(), options
});
```

| option | default | limits |
| --- | --- | --- |
| `count` | `1000` | the most messages of one batch |
| `maxWait` | `0` | up to 600 s. Zero replies at once. The call gives up 10 s after `maxWait` |
| `minCount` | `1` | from 1 to `count`, else error 4009 |
| `maxBytes` | `1048576` | at least 344 bytes, else error 80 |
| `commit` | `CommitPolicy.Manual` | who stores the offset, see below |

- `CommitPolicy.Manual` stores nothing. Store `checkpoint()`, or
  `checkpointAt(index)` for a part of the batch, with `offset.storePosition`,
  and remove it with `offset.deletePosition`. A checkpoint keeps the partition
  history that delivered it, so a store after the partition was deleted and
  created again fails with error 87.
- `CommitPolicy.OnRead` makes the server store the offset of the last message
  of each read as part of the read, in one round trip. While the primary stays
  up a message is delivered at most once. A failover before the store
  replicates can deliver the last batch again. A one-shot poll refuses
  `CommitPolicy.Auto()` with error 2 (InvalidConfiguration), since it has
  nothing to count.
- `continuation()` continues the partition after the batch, under its context.
  An empty batch keeps the start of its read, so `Next` stays `Next`. A poll of
  another partition refuses the continuation with error 3.
- A first message larger than `maxBytes` comes alone in its read.
- If you abort a waiting poll, the client cancels it on the server. A poll that
  stores nothing rejects with the abort reason. A poll that commits keeps its
  reply: the server answers it once, refused when the cancel won and nothing was
  stored, so the call rejects with the abort reason, or with the messages that
  it stored before the cancel came, which the call returns.

### Partition reader

`message.reader` opens a `PartitionReader`, which reads many partitions in
batches and keeps the position of each. It reads the partitions of one primary
in one request, finds a moved primary by itself and gives each partition its
turn. A partition whose last read ended before the end of the partition is read
in a fetch of its own while the read ahead has room for another full read, up to
16 reads in flight, so a backlog of many partitions is read as fast as one poll
of each partition would read it. Under `CommitPolicy.OnRead` the reader starts
those fetches only while `next()` waits, and each partition is in one committing
read at a time.

```typescript
import { Consumer, ReadError } from "apache-iggy";

const reader = await client.message.reader({
  streamId, topicId, consumer: Consumer.Single, partitions: [1, 2, 3], options: { maxWait: 5_000 }
});
try {
  while (true) {
    try {
      const batch = await reader.next();
      process(batch);
      await reader.commit(batch.checkpoint()!);
    } catch (error) {
      // A message that does not decode. The reader reads it again until a seek passes it.
      if (error instanceof ReadError && error.skip) await reader.seek(error.partitionId!, error.skip);
      else if (!(error instanceof ReadError)) throw error;
    }
  }
} finally {
  await reader.close();
}
```

- A consumer group reads the assignment of its member:
  `{ consumer: Consumer.Group('g') }` without `partitions` joins the group,
  follows rebalances and leaves the group in `close()`.
- `next()` returns one batch of one partition, never empty. A `ReadError`
  names the partition of an error, or none for an error of the whole reader,
  and the reader retries by itself. Error 87 stops a partition until `seek`
  when its position came from an earlier read, and so do error 80 and a refusal
  that six retries did not change.
- For a message that does not decode, `ReadError` carries `skip`, `offset` and
  `record`, the record as the reply carried it: its 48-byte frame header,
  payload and user headers, so the message can be kept or inspected. When the
  framing of the reply is malformed, `offset` is undefined and `record` holds
  the remaining reply bytes from where decoding stopped.
- With `CommitPolicy.Manual`, `commit` stores a checkpoint. With
  `CommitPolicy.OnRead` the server stores the offset of each read, the reader
  reads only while `next()` waits, and `close()` moves the offset back to the
  last batch that the application took. A read whose reply was lost is
  repaired: the next read finds what it stored, and the reader reads those
  messages again by offset without a commit. Only a lost read makes the reader
  read skipped offsets again: when the stored offset moved ahead because the
  application or another session stored it, the reader delivers from there. A
  batch that the server stored is handed over even when its partition left the
  assignment of the member. A `seek` to an offset above 0 first stores the
  offset before it, so a restart resumes at the sought offset, and a refused
  store fails the seek and moves nothing. Every other `seek` stores nothing.
- A refusal 5009 of a group partition drops its position, so the next read
  starts again from `start` under the new owner of the partition. A 5009 that
  six retries did not change is reported and stops the partition until `seek`.
- With `CommitPolicy.Auto({ interval, every, after })` the reader counts each
  batch when `next()` returns it (`CommitAfter.Delivery`), at the next `next()`
  or `close()` (`CommitAfter.Processing`), or on `mark` (`CommitAfter.Marked`),
  and stores the latest counted position at the interval, on the event
  (`CommitEvery.Message`, `CommitEvery.Messages(n)` or `CommitEvery.Batch`),
  and in `close()`. `DEFAULT_AUTO_COMMIT` stores every 10,000 messages or 5 s
  after processing.
- The reader reads ahead up to 16 MiB and 16,000 messages (`prefetchBytes`,
  `prefetchMessages`). Without options a read waits up to 5 s. After a refused
  read, a lost read or a failed refresh it waits `retryInterval`, 1 s by
  default.

### Consumers

`client.consumer` and `client.consumerGroup` hand over one message at a time on
a partition reader, as an async iterable, with a handler loop, or as a
`Readable`.

```typescript
import { CommitAfter, CommitPolicy } from "apache-iggy";

const consumer = await client.consumerGroup({
  name: 'orders', streamId, topicId,
  options: { commit: CommitPolicy.Auto({ after: CommitAfter.Processing }) },
  onError: (error) => console.warn(error)
});
await consumer.consume(async ({ message }) => handle(message));
await consumer.shutdown();
```

- `client.consumerGroup` creates the group when it is missing and joins it.
  `client.consumer` reads the given `partitions` under the consumer `name`.
- Under `CommitAfter.Processing` a message counts once the iterator is read
  again or its handler returns. A handler that throws ends `consume` with its
  error and leaves its message uncounted, and so does a shutdown while the
  handler runs. `shutdown()` counts the last message of the iterator, stores or
  moves back the offsets and leaves the group. A later `shutdown()` waits for
  the first and ends as it does. `seek` drops the rest of the batch of its
  partition.
- A read error that the reader retries by itself goes to `onError` and the
  consumer goes on. A message that does not decode ends the iteration with its
  `ReadError`, whose `skip` passes it with `seek`.
- `toReadable()` takes the next message only when its own reader asks for it,
  and destroying it shuts the consumer down.

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
lane. Progress polls (`Next` or auto-commit), offset reads, stores and deletes
share a separate progress lane per partition. Sends use only the mutation lane,
so a parked poll does not delay a producer that shares the client. Offset
stores and deletes hold both lanes. They take the progress lane first, so a
send can pass a store that waits behind a parked poll.

An uncertain mutation remains retained and blocks later mutations in its lane.
An uncertain offset store or delete also blocks the progress lane until it is
resolved or abandoned. `OperationUncertainError.operationId` and
`LaneBlockedError.operationId` are exact `bigint` values. Use `resolveOperation(operationId, { deadline })` to
replay the retained frame, where `deadline` is an absolute `Date.now()` timestamp
in milliseconds. The default operation budget is 30 seconds, including admission,
routing, link waits and retries.

For a blocked fetch target, `LaneBlockedError.partitionId` identifies the partition
whose retained operation needs repair. A block from an uncertain committing fetch
also sets `requiresExplicitRecovery`, including on stores and deletes, and has
`errorCode` 14009 (`PARTITION_LANE_BLOCKED`). Other blocks have `errorCode` 14007
(`LANE_BLOCKED`). Other single-lane errors leave `partitionId` undefined.

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

Next polls that commit nothing (100/106), and non-committing fetches, hold their progress lane only during the active call.
Error 57, a lost reply or another unknown outcome ends the call immediately with status 57 and no
retained operation ID. These polls never replay after uncertainty. Known
non-admission errors 58/89 remain retryable. Pure reads may retry lost replies.
Before uncertainty, lifecycle error 88 retries with a fresh ID within the same
deadline. After uncertainty, only committed-result proof can release the lane.

An auto-commit poll (100/106) or a committing fetch (109) with an unknown outcome throws `FetchProgressUncertainError` (local code 14010) and retains progress blocks for every affected partition history. Its `operationId` is a client-lifetime recovery key, not a server replay receipt. A lost reply, a transport failure after the send, a disconnect or a logout while the call waits leave the outcome unknown.

So does every refusal except those that prove the server stored nothing: 5, 6, 58, 80, 87, 89, 90, 1009, 1010, 2010, 2011, 3007, 3024, 4100 and 5009 for a whole reply, and 58, 80, 87, 89, 3007, 3024 and 5009 for a fetch section.

Later progress polls, fetches, stores and deletes fail with `LaneBlockedError` carrying that ID, the blocked partition and `errorCode` 14009. Stateless reads, `GetOffset` inspection and sends remain available. Reconnect, sign-in and late replies do not clear these blocks.

`resolveOperation` returns the same uncertainty without I/O. `abandonOperation` explicitly releases the blocks while accepting that the old server request may still finish. An inspected offset is not a completion barrier.

Valid partial replies preserve healthy sections. An uncertain section carries
`status: 14010` and `progressError`. Record decode failures keep their existing
rewind behavior. Raw replies keep their wire bytes, so raw callers inspect section
statuses and obtain the recovery ID from a later blocked-target call. A partition
reader delivers healthy sections and abandons the lost reads of its own committing
fetches, so the next read finds what they stored and its streams and consumers go
on. Creating a new independent client does not inherit that client's blocks.

Committing fetches reserve a separate bounded recovery pool using
`maxPendingOperations` and `maxPendingOperationBytes` before sending. The byte charge
is the request size, but retained state holds only target identities and the local
ID. Capacity refusal occurs before send. A proved result or explicit abandonment
releases the reservation. Session reset does not release retained fetch blocks.

Configure `maxPendingOperations` (default 256) and `maxPendingOperationBytes`
(default 64 MiB) on the client. Admission waits before allocating an ID or freezing
a mutation frame. Both limits must be positive. The byte budget includes the
256-byte header and body. Smaller positive budgets reject mutations that cannot
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
lineage. Only SDK recovery continues them under a replacement session.

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

**Breaking change:** `singleConsumerStream` and `groupConsumerStream` are
adapters over a partition reader, each on a client of its own, and hand over
batches with messages only, never an empty one. `pollOptions` sets `maxWait`,
`minCount` and `maxBytes` of each read, and a read waits up to 5 s by default.
`requestTimeout` is not used: a read gives up 10 s after its `maxWait`. With
`autocommit`, which is the default, the reader commits on read, and the stream
reads only when its consumer asks for another batch, so the server never stores
a batch that waits in the stream buffer. Without `autocommit` the stream reads
ahead. `endOnLastOffset` ends the stream once the last read of every partition
found no message.

When a read fails with status 30, 40, 57, 58, 89 or 90, after a deadline, after
unknown progress, after `NotConnectedError` and after `ConnectionClosedError`,
the stream reads again after `pollingRetryInterval`, 1000 ms by default, the
same as `polling_retry_interval` of the Rust consumer. A lost reply of a read
that commits is repaired by the reader. The client binds the session again,
signs in again, reconnects or routes again after these errors on its own. Any
other error ends the stream, and so does a message that does not decode. The
server can also refuse a sign-in with status 40, for example for an inactive
user. So after a 40 the stream signs in again, and a refused sign-in ends the
stream.

`singleConsumerStream` and `groupConsumerStream` sign in before they return the
stream, and `groupConsumerStream` also creates and joins its group. If the
sign-in or the join fails, the call destroys its client and rejects with that
error. If no endpoint accepts a dial before the retry budget or the 30 second
sign-in deadline ends, the error is `NotConnectedError`. When a stream closes,
it closes its reader, which waits for a read that commits, moves the offsets of
the batches that nobody read back and leaves the group, then destroys its
client. A batch still in the stream buffer counts as unread. If the client of
a destroyed stream has no session, the stream destroys the client at once, and
it never keeps the client longer than 5 seconds for the close.

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
| `reconnection_retries` | integer up to `4294967295` (u32 max). Larger values are rejected like Rust's u32 overflow, and `unlimited` maps to this ceiling. Defaults to unlimited |
| `heartbeat_interval` | duration up to `2147483647ms` (Node's largest timer delay). `0` disables heartbeats |
| `reconnection_interval` | positive duration (`ms`, `s`, `m`, `h`) up to `2147483647ms` (Node's largest timer delay). Zero spellings are rejected. Defaults to `1s` |
| port in the authority | decimal up to `65535` |

Durations accept the same expressions as the Rust SDK, for example `500ms`,
`10s`, `1h 30m`, `5d`, `2w`, `1y`. Matching is case-insensitive and
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

### metadata changes

`system.getMetadataChanges` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manage_servers` permission.

```ts
import { setTimeout as sleep } from "node:timers/promises";
import {
  Client, MetadataChangeStart, MetadataChangeStatus,
  type MetadataChangeGroup, type MetadataCurrentPageToken,
} from "apache-iggy";

const follow = async (client: Client, groups: MetadataChangeGroup[]) => {
  let start: MetadataChangeStart = MetadataChangeStart.Earliest;
  for (;;) {
    const page = await client.system.getMetadataChanges({ start, groups });
    switch (page.status) {
      case MetadataChangeStatus.Ok:
        for (const record of page.records)
          console.log(record.op, record.kind, record.actorUserId, record.body);
        if (page.next) start = MetadataChangeStart.After(page.next);
        if (!page.hasMore) await sleep(1000);
        break;
      case MetadataChangeStatus.Behind:
        await sleep(100);
        break;
      default:
        start = await rebuild(client, groups);
    }
  }
};

/** Pages the current state, then returns the position to follow from. */
const rebuild = async (client: Client, groups: MetadataChangeGroup[]) => {
  let pageToken: MetadataCurrentPageToken | undefined;
  for (;;) {
    const page = await client.system.getMetadataChanges({
      start: MetadataChangeStart.Current(pageToken), groups,
    });
    if (page.status === MetadataChangeStatus.Behind) {
      await sleep(100);
      continue;
    }
    if (page.status !== MetadataChangeStatus.Ok) {
      pageToken = undefined;
      continue;
    }
    for (const item of page.items)
      console.log(item.group, item.body);
    if (page.currentPageToken) pageToken = page.currentPageToken;
    else if (page.next) return MetadataChangeStart.After(page.next);
    else pageToken = undefined;
  }
};
```

`expired` means the log no longer holds the history after the cursor, and `reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `topics` with `streams`. Keep a rebuilt state private until a change page answers `ok` with `hasMore` false and a `headOp` at or above the last current page's.

Records narrow by `kind` and items by `group`, so `record.kind === "stream_created"` types `record.body` as a stream. A group or kind newer than this build arrives as `"unknown"` with the raw body and the wire codes in `groupCode` and `kindCode`, while a record of a newer kind keeps its known `group`, so a mirror that cannot apply it can stop without advancing its cursor. Ops, timestamps and 128-bit ids are `bigint`.

A query that breaks the request rules, such as a limit outside 1 to 1000 or a time bound with a latest or current start, is refused with error 3 before anything is sent. `MetadataChangeCursor` and `MetadataCurrentPageToken` print and parse as the `mc1_` and `cs1_` text forms the HTTP API and the CLI use, and `JSON.stringify` writes them as that text.

## use sources

Cluster polling uses command 15 (`BindSession`), route discovery 105 and poll
100. Deferred polls use 106, fetches 109 and their
cancellation 108.
Commands 14, 103, 104 and 123 are retired. Pause consumers for the coordinated
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

The resource options tests also expect the server to run with encryption enabled and a config key (`IGGY_ENCRYPTION_ENABLED=true` and `IGGY_ENCRYPTION_KEY` set to the base64 of 32 bytes), so a stream created without a key reports a server-generated one. The keyed metadata changes case registered from the same file needs the config key too.

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
