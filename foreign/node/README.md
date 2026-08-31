<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Apache Iggy Node.js Client

Apache Iggy Node.js client written in typescript, it currently only supports tcp & tls transports.

diclaimer: although all iggy commands & basic client/stream are implemented this is still a WIP, provided as is, and has still a long way to go to be considered "battle tested".

note: This lib started as _iggy-bin_ ( [github](https://github.com/T1B0/iggy-bin) / [npm](https://www.npmjs.com/package/iggy-bin)) before migrating under iggy-rs org. package iggy-bin@v1.3.4 is equivalent to @iggy.rs/sdk@v1.0.3 and migrating again under apache iggy monorepo ( [github](https://github.com/apache/iggy/tree/master/foreign/node) and is now published on npmjs as apache-iggy

note: previous works on node.js http client has been moved to [iggy-node-http-client](<https://github.com/iggy-rs/iggy-node-http-client>) (moved on 04 July 2024)

## install

```bash
npm i --save apache-iggy
```

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

Sends must use explicit `Partitioning.PartitionId` partitioning: the client
routes each request to a partition-scoped namespace, so broker-side balancing
(`Partitioning.Balanced`) and key hashing (`Partitioning.MessageKey`) are
rejected before the request is sent.
<!-- TODO(hubcio): Balanced and MessageKey partitioning to be implemented;
not decided yet whether it'll be on server side or client side. -->

VSR works over TCP and TLS. It restricts `Client` to one pooled connection because authentication, request sequencing, and consumer-group assignments belong to one consensus session. Configurations requesting more than one pooled connection fail before a socket is opened.

VSR authentication translates the existing password and personal-access-token
login APIs into the register handshake required by the consensus protocol. A
disconnect or eviction invalidates the session, and later work must register a
new session. Transient not-committed responses retry the exact encoded request
within one bounded deadline. A disconnected mutation is never replayed under a
new session.

The client pings every `heartbeatInterval` milliseconds, 5000 by default, which
keeps an idle session alive when the server's `[heartbeat]` eviction is enabled.
`heartbeatInterval` also accepts a duration expression such as `"10s"` or
`"1h 30m"`, like the Rust SDK.
The server evicts a connection silent for 36 s, which is 1.2 x its 30 s
heartbeat interval. Raising the client interval past that window, or setting it
to 0 to disable client heartbeats, exposes an idle consumer-group member to
eviction; a connection holding no group membership is left alone. Any other
unusable value is rejected instead of silently disabling the heartbeat.

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

The client includes its npm package version and the binary protocol crate
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
`reconnection_interval`, `heartbeat_interval` and `nodelay`. `reestablish_after`
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

## use sources

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
address for bdd and e2e tests.

#### unit tests

```bash
npm run test:unit
```

#### e2e tests

e2e test expect an iggy-server at tcp://127.0.0.1:8090

```bash
npm run test:e2e
```

#### bdd tests

bdd test expect an iggy-server at tcp://127.0.0.1:8090

```bash
npm run test:bdd
```

#### run all test

`npm run test` runs unit, bdd and e2e tests suite (expect an iggy-server at tcp://127.0.0.1:8090)

### lint

```bash
npm run lint
```
