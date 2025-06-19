# iggy node.js client

iggy node.js client for [iggy-rs](https://iggy.rs/)'s binary protocol, written in typescript. it currently only support tcp & tls transports.

diclaimer: although all iggy commands & basic client/stream are implemented this is still a WIP, provided as is, and has still a long way to go to be considered "battle tested".

note: This lib started as _iggy-bin_ ( [github](https://github.com/T1B0/iggy-bin) / [npm](https://www.npmjs.com/package/iggy-bin)) before migrating under iggy-rs org. package iggy-bin@v1.3.4 is equivalent to @iggy.rs/sdk@v1.0.3 and migrating again under apache iggy monorepo ( [github](https://github.com/apache/iggy/tree/master/foreign/node) and is now published on npmjs as @apache-iggy/node-sdk

note: previous works on node.js http client has been moved to [iggy-node-http-client](<https://github.com/iggy-rs/iggy-node-http-client>) (moved on 04 July 2024)

## install

```bash
npm i @apache-iggy/node-sdk
```

## basic usage

```ts
import { Client } from "@apache-iggy/node-sdk";

const credentials = { username: "iggy", password: "iggy" };

const c = new Client({
  transport: "TCP",
  options: { port: 8090, host: "127.0.0.1" },
  credentials,
});

const stats = await c.system.getStats();
```

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

#### unit tests

```bash
npm run test
```

#### e2e tests

e2e test expect an iggy-server at tcp://127.0.0.1:8090

```bash
npm run test:e2e
```

### lint

```bash
npm run lint
```
