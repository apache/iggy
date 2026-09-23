# Kafka gateway scope — [apache/iggy#3421](https://github.com/apache/iggy/issues/3421)

## Issue #3421 — in scope (this iteration)

Foundation layer only: a TCP listener on the Kafka wire port that decodes requests, validates scoped API keys and versions, validates request wire formats, and returns stub responses. **No Iggy backend integration.**

**Stub semantics (important):** Produce discards the payload and answers with retriable `NOT_LEADER_OR_FOLLOWER` (6). CreateTopics validates the request but answers with `NOT_CONTROLLER` (41) so clients do not believe topics were created. Do not treat `ec=0` stub success as durable storage — that arrives in the Iggy bridge phase.

| Deliverable | Status | Location |
| ------------- | -------- | ---------- |
| TCP listener on `127.0.0.1:9093` (configurable) | Done | `src/server.rs`, `src/main.rs` |
| Length-prefixed frame read/write with `max_frame_size` cap | Done | `src/server.rs` |
| Request header v1/v2 auto-detection | Done | `src/protocol/header.rs` (delegates to `kafka_protocol::messages::ApiKey`) |
| Version negotiation firewall (`SUPPORTED_RANGES`) | Done | `src/protocol/api.rs` |
| Request decode + stub encode for 6 API keys | Done | `src/protocol/api.rs`, `responses.rs` (via the `kafka_protocol` crate) |
| Produce hot path: RecordBatch as opaque `Bytes` | Done | `src/protocol/responses.rs` |
| Pre-decode bounds guard against unbounded allocation | Done | `src/protocol/bounds_guard.rs` |
| Graceful errors (corrupt decode, invalid header) | Done | `src/protocol/api.rs`, `src/server.rs` |
| Regression test suite | Done | `tests/` — see [`TEST_SUITE.md`](TEST_SUITE.md) |
| Manual testing procedure | Done | [`MANUAL_TESTING.md`](MANUAL_TESTING.md) |
| Wire fixture tool for manual/integration testing | Done | `tools/kafka-tool/` |

Source of truth for supported ranges: `SUPPORTED_RANGES` in [`src/protocol/api.rs`](../src/protocol/api.rs).

### Governance model

Expand `SUPPORTED_RANGES` only after a key/version pair is manually tested. ApiVersions advertises exactly what the firewall allows.

**Every unsupported-version case closes the connection, for every listed key** - not just above
the encoder max. `kafka_protocol`'s schema floor for each of the six supported messages happens
to equal `SUPPORTED_RANGES`' own min today (Produce 3, Fetch 4, ListOffsets 1, Metadata 0,
ApiVersions 0, CreateTopics 2), so there is no version below an API's min that the crate can
actually encode a response for either - `unsupported_version_response` still tries, but the
encode attempt fails and the connection closes rather than sending a malformed body.
**ApiVersions is the sole exception** (KIP-511): out of range still answers with a v0 error body,
because a client probing an unknown server must be able to parse the discovery response before
it knows the server supports flexible encoding.

---

## Supported API keys and versions

| API key | Name | Min version | Max version | Valid versions | Behavior |
| --------- | ------ | ------------- | ------------- | ---------------- | ---------- |
| 18 | ApiVersions | 0 | 3 | 0, 1, 2, 3 | Advertise supported ranges; flexible encoding at v3+ |
| 3 | Metadata | 0 | 9 | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 | Decode topic list count; stub broker host from `advertised_host` or the bound `local_addr` IP; flexible encoding at v9+ |
| 0 | Produce | 3 | 9 | 3, 4, 5, 6, 7, 8, 9 | Decode request; stub returns `NOT_LEADER_OR_FOLLOWER` (6) |
| 1 | Fetch | 4 | 12 | 4, 5, 6, 7, 8, 9, 10, 11, 12 | Decode request; stub response |
| 2 | ListOffsets | 1 | 6 | 1, 2, 3, 4, 5, 6 | Decode request; stub response |
| 19 | CreateTopics | 2 | 5 | 2, 3, 4, 5 | Decode request; stub returns `NOT_CONTROLLER` (41); `-1` partitions/RF = broker default on v4+ |

A request is accepted when `min_version ≤ api_version ≤ max_version` for that API key. Any other version for a listed key closes the connection (ApiVersions excepted - see Governance model above). Any unlisted API key also closes the connection: no api-specific response schema exists for it, so any body this gateway could send would be misparsed by the client against the schema it expected.

### Valid versions reference (by API key)

Use this table when configuring clients or generating wire fixtures with `kafka-message-gen`.

| API key | Name | Valid versions (inclusive range) | Flexible wire encoding from |
| --------- | ------ | ---------------------------------- | ---------------------------- |
| 0 | Produce | 3–9 | v9 |
| 1 | Fetch | 4–12 | v12 |
| 2 | ListOffsets | 1–6 | v6 |
| 3 | Metadata | 0–9 | v9 |
| 18 | ApiVersions | 0–3 | v3 |
| 19 | CreateTopics | 2–5 | v5 |

---

## Unsupported API keys (foundation)

All API keys not listed above close the connection (see Governance model above) - none receives an `UNSUPPORTED_VERSION` response. Examples not in this foundation scope:

| API key | Name | Notes |
| --------- | ------ | ------- |
| 8 | OffsetCommit | Consumer group — later issue |
| 9 | OffsetFetch | Consumer group — later issue |
| 10 | FindCoordinator | Consumer group — later issue |
| 11–16 | JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups | Consumer group — later issue |
| 17 | SaslHandshake | Auth — later issue |
| 20+ | DeleteTopics, InitProducerId, transactions, ACLs, etc. | Later issues |

Full reference for future phases: [`kafka_api_keys_reference.md`](kafka_api_keys_reference.md).

---

## Architecture (three layers)

| Layer | #3421 | Description |
| ------- | ------- | ------------- |
| **1 — Wire framing** | In scope | `server.rs` — custom, zero-copy frame I/O; `header.rs` delegates version selection to `kafka_protocol::messages::ApiKey` |
| **2 — Request/response codecs** | Partial | Decode/encode via the `kafka_protocol` crate (broker feature only) for 6 hot-path keys; `bounds_guard.rs` pre-validates against unbounded allocation before handing a frame to the crate; stub responses only |
| **3 — Iggy bridge** | Landed, not wired in | `bridge/` module (connection, topic mapping, provisioning, high watermark) landed; Produce/Fetch handler wiring itself is a follow-on ([#3535](https://github.com/apache/iggy/issues/3535)/[#3536](https://github.com/apache/iggy/issues/3536)) |

---

## TODO — post-#3421 (architecture review backlog)

Items from the [hybrid architecture review](https://github.com/apache/iggy/discussions/3252) and maintainer feedback. **Not part of #3421.**

### Phase 2 — Iggy bridge

[#3533](https://github.com/apache/iggy/issues/3533) landed the bridge module itself; the items
below it are still open for the issues that build on top of it.

- [x] Add `bridge/` module (`iggy_bridge`) - connection lifecycle, topic mapping, provisioning,
      high watermark, error mapping. See [README.md](../README.md#iggy-bridge-3533). Produce →
      `send_messages` / Fetch → `poll_messages` handler wiring itself is
      [#3535](https://github.com/apache/iggy/issues/3535)/[#3536](https://github.com/apache/iggy/issues/3536),
      not part of `bridge/`'s own scope.
- [x] Idempotent `ensure_stream_and_topic()` (create-if-not-exists) - `src/bridge/iggy_bridge.rs`,
      exercised end-to-end in `tests/bridge_iggy_integration_tests.rs`.
- [x] Real CreateTopics ([#3538](https://github.com/apache/iggy/issues/3538)): with
      `IGGY_KAFKA_BRIDGE_ENABLED=true`, creates the Iggy stream/topic through
      `IggyBridge::create_kafka_topic` - an atomic create-or-report-exists call, not a separate
      existence read followed by an idempotent create (that sequence has a TOCTOU window: two
      concurrent requests for the same new name could both observe "doesn't exist yet" and both
      receive `NONE`, when Kafka guarantees exactly one caller does).
      `src/protocol/handlers/create_topics.rs`, `tests/create_topics_real_bridge_tests.rs`. With
      the bridge off, the stub from #3421 answers `NOT_CONTROLLER` (41) as before.
  - Every occurrence of a duplicate topic name within one request is rejected with
    `INVALID_REQUEST` (42) and nothing is created for it, matching real Kafka
    (`ControllerApis.createTopics`) rather than creating the first occurrence and reporting the
    rest as already existing.
  - A manual partition `assignments` list combined with an explicit `num_partitions`/
    `replication_factor` is rejected with `INVALID_REQUEST` (42) even when the count agrees with
    `assignments.len()` - real Kafka's own `ReplicationControlManager` treats the two as mutually
    exclusive inputs, not independently-checked values that happen to agree.
  - A manual assignment's own partition indices are checked too, not just its length:
    `ERROR_INVALID_REPLICA_ASSIGNMENT` (39) for a duplicate index or one that isn't exactly
    `0..assignments.len()` - `{5: [...], 7: [...]}` has the right length for a 2-partition topic
    but names neither partition `0` nor `1`, matching real Kafka's own
    `ReplicationControlManager.createTopic` key-set validation.
  - `IggyError::RequestAlreadyApplied` (the SDK's reconnect path replayed a write that already
    committed) maps to `NONE`, not the `UNKNOWN_SERVER_ERROR` catch-all - the operation did
    succeed, and a Java client treats `UNKNOWN_SERVER_ERROR` as non-retriable. Special-cased
    locally in `create_topics.rs`, not in the shared `BridgeError -> Kafka error code` mapping
    every handler's error path goes through: "the write already applied" is a write-only fact,
    and a read (Metadata, ListOffsets) reaching this variant has no write to have applied.
  - `error_message` on a rejected topic never re-embeds the topic name `CreatableTopicResult.name`
    already carries - `BridgeError::InvalidKafkaTopicName`'s `Display` does, so using it directly
    would roughly double the response cost per invalid name, for free (name validation runs before
    any bridge I/O).
  - `IggyBridge::get_kafka_topic` no longer probes `get_stream` separately before `get_topic` -
    `get_topic` already answers `Ok(None)` when the stream itself is missing, so the probe was a
    second round trip to learn something the one call already told it.
  - Bridge fan-out is bounded independently of `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096,
    still a pre-decode ceiling, not a usability one): a duplicate name never reaches the bridge at
    all (rejected up front, see above), a request naming more than 100 distinct non-duplicate
    topics is rejected outright (`INVALID_REQUEST`, no bridge call for any of them), and the
    request's own wire `timeout_ms` (clamped to `[1s, 30s]`) now bounds the whole handler's
    aggregate bridge work, not just decoded and discarded - a deadline that fires answers every
    topic `REQUEST_TIMED_OUT` rather than continuing to hold the shared lockstep `IggyClient`.
- [x] Document partition mapping in [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md):
  - Iggy partitions are **0-based** (same as Kafka) — direct `partition_id` mapping, no offset conversion
  - Kafka consumer groups do **not** map onto Iggy consumer groups. Assignment stays client-side, and Iggy's group registry is used as an offset key only ([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md))
  - `Partitioning::partition_id(index)` on every Produce. A Kafka producer resolves the partition before it builds the request, so `Partitioning::balanced()` has no trigger there. The `-1` default-partition-count case belongs to CreateTopics
- [x] Real Metadata topic/partition data ([#3534](https://github.com/apache/iggy/issues/3534)):
      with `IGGY_KAFKA_BRIDGE_ENABLED=true`, a named lookup answers from
      `IggyBridge::get_kafka_topic` (`UNKNOWN_TOPIC_OR_PARTITION` if not found) and a null
      topics array ("all topics") answers from `IggyBridge::list_kafka_topics` - the target of
      every configured `TopicMapping` override plus every other topic in the default stream, so
      an Iggy stream this bridge has no mapping rule pointing at is never listed (nothing a Kafka
      client ever named). Every reported partition names this gateway's single broker (node id 1)
      as leader/replica/ISR, since there is only ever one. `src/protocol/handlers/metadata.rs`,
      `src/bridge/iggy_bridge/topics.rs` (`get_kafka_topic`/`list_kafka_topics`),
      `tests/metadata_real_bridge_tests.rs`. With the bridge off, the stub from #3421 reports
      every requested topic unknown, as before.
  - Fixed alongside: the stub's `decode_topics` collapsed a null topics array ("all topics") and
    an explicit empty one (`Some(vec![])`, "these zero topics") to the same `Vec::new()` - the
    real path's `decode_requested_topics` keeps `Option<Vec<_>>` so the two aren't conflated.
    Also: at `api_version == 0` an explicit empty array is folded into "all topics" too, matching
    Kafka's own `isAllTopics()` rule (`topics == null || (topics.isEmpty() && version == 0)`) -
    there is no v0 wire shape for "cluster info only, zero topics" (that distinct shape, KIP-4's
    `describeCluster()`, starts at v1).
  - `get_kafka_topic` shares its implementation with CreateTopics' own existence check above -
    both need "does this Kafka-side name resolve to a real Iggy topic," so this bridge exposes one
    method returning the SDK's own `TopicDetails`, not two narrower, independently-maintained
    lookups.
  - The response-size cap above only covers the "all topics" and per-name-expansion cases. A
    named lookup is separately bounded on the request side: names repeated in one request are
    deduped up front to one response entry, not just one `get_kafka_topic` round trip - real
    Kafka answers a topic named twice in one request with one response entry, and re-expanding to
    match the request would let a handful of repeats of one large topic name amplify a response
    sized off the repeat count instead of the distinct count. A lookup naming more than 100
    distinct topics is rejected outright (`INVALID_REQUEST`, no bridge call for any of them -
    `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode ceiling, not a usability one),
    and the whole lookup's aggregate bridge work runs under a fixed 20s wall-clock deadline
    (`Metadata` carries no `timeout_ms` field in any version, unlike `CreateTopics`, so this cannot
    be client-honored) - a deadline that fires answers every name `REQUEST_TIMED_OUT` rather than
    continuing to hold the shared lockstep `IggyClient`.
  - The "all topics" path is server-driven, not client-count-driven, so it has no distinct-topic
    cap - only the response-size projection applies there, and it **truncates** rather than
    closes on overflow: `list_kafka_topics()`'s result is trimmed to as many whole topics (in
    listing order) as `max_frame_size` allows. Closing instead, as the named-lookup path still
    does, would make every all-topics Metadata call - the bootstrap/refresh shape both librdkafka
    and the Java client use - fail identically and permanently once the cluster's total partition
    count crosses the trip point, since that size is the catalog's own, not anything the
    requesting client chose or can shrink.
- [ ] Multi-broker topology (this gateway is, and will stay, a single logical broker - node id 1
      always leads every partition it reports; nothing here models an Iggy cluster as multiple
      Kafka-visible brokers)
- [ ] A raw/non-conformant client's extra trailing byte on some Metadata request shapes seen
      against real `kcat`/`librdkafka` traffic in earlier testing on a since-restructured branch -
      not reproduced or root-caused against the current `kafka_protocol`-based decode path in this
      session, so not carried forward as a fix here rather than guessed at. Needs fresh
      reproduction against a real `kcat` before it's re-closed.
- [x] Real ListOffsets ([#3537](https://github.com/apache/iggy/issues/3537)): with
      `IGGY_KAFKA_BRIDGE_ENABLED=true`, `LATEST` answers from `IggyBridge::high_watermarks` and
      `EARLIEST` answers `0`. Any other requested timestamp (arbitrary-timestamp offset search,
      including `offsetsForTimes`/`by_duration` resets) is unsupported - Iggy exposes no
      per-message timestamp index - and answers `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43) per
      partition rather than a fabricated offset; non-retriable, so a Java client resolves this
      immediately instead of spinning until `default.api.timeout.ms`.
      `src/protocol/handlers/list_offsets.rs`, `tests/list_offsets_real_bridge_tests.rs`. With the
      bridge off, the stub from #3421 answers `NOT_LEADER_OR_FOLLOWER` (6) as before.
  - `EARLIEST = 0` is real *only* for a partition this bridge has never had retention trim: Iggy
    tracks no rolling low-watermark distinct from partition creation, so once a partition is
    old enough for retention to purge its first segment, `0` names a log-start offset that no
    longer exists - a real consumer with `auto.offset.reset=earliest` seeks into a hole. Not
    fixable client-side; needs the bridge to expose a real start offset. Harmless *today* only
    because Fetch (`#3536`) is still a stub - nothing yet reads at the offset this returns.
  - Bridge fan-out is bounded independently of `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096,
    still a pre-decode ceiling, not a usability one): topic entries sharing a name are deduped to
    one `high_watermarks` call before any bridge work starts (a name repeated across request
    entries costs one round trip per entry), and a topic whose every partition asks for an
    unsupported timestamp skips the call entirely. A request naming more than 100 distinct topics
    resolves the first 100 and answers the rest `REQUEST_TIMED_OUT` (retriable) with no bridge call
    at all, so a client that retries only its still-erroring topics narrows below the cap on its
    own. The whole request's aggregate bridge work runs under one 20s wall-clock deadline
    (`ListOffsets` carries no `timeout_ms` field in any version this gateway supports, so this is a
    fixed ceiling, not a client-honored one), applied per topic rather than once around the whole
    batch: a topic already resolved when the deadline arrives keeps its real answer, and only the
    not-yet-started topics answer `REQUEST_TIMED_OUT`.
- [ ] Real Metadata topology (brokers, partitions, leaders) backed by Iggy state

### `kafka-protocol` crate adoption — superseded, done differently

This TODO originally proposed a selective, feature-gated adoption (`kafka-protocol-cold`)
alongside the hand-rolled `requests.rs`/`responses.rs` codecs, keeping custom code for the
Produce/Fetch hot paths. That hybrid approach was not taken: `kafka_protocol` (broker feature
only) now decodes/encodes all six supported message types wholesale, and the hand-rolled
`codec.rs`/`requests.rs` were deleted. RecordBatch bytes stay opaque (`Option<Bytes>`, never
decoded) on the Produce/Fetch hot paths, preserving the one property this TODO was protecting.
`bounds_guard.rs` covers the DoS-bound gap the crate itself leaves open (see Governance model
above).

- [ ] Consumer-group API keys (8–14, 10) and complex Metadata/FindCoordinator responses remain unimplemented (see Phase 3 below) - the crate can decode them when that phase starts

### Phase 3 — Consumer groups (~7 API keys)

Offset persistence design ([#3540](https://github.com/apache/iggy/issues/3540)):
[`OFFSET_STORAGE.md`](OFFSET_STORAGE.md).

- [ ] OffsetCommit (8), OffsetFetch (9), FindCoordinator (10)
- [ ] JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14)
- [ ] DescribeGroups (15), ListGroups (16) as needed by target clients

### Phase 3+ — Auth, admin, tuning

InitProducerId and idempotent producers
([#3545](https://github.com/apache/iggy/issues/3545)): [`IDEMPOTENCE.md`](IDEMPOTENCE.md).

- [ ] SASL (17, 36) if required by deployment
- [ ] Tune `max_frame_size` per workload (Kafka defaults: ~1 MiB produce, ~50 MiB fetch; current default 8 MiB)
- [ ] Target **~15–20 API keys** total for a functional bridge — not all 74+ admin keys

### Open questions (ask maintainers before Phase 2)

- [X] Repo placement: `gateways/kafka/` in [apache/iggy](https://github.com/apache/iggy) vs separate proxy repo (affects workspace deps and CI)
- [ ] Confirm bridge dependency strategy ([Discussion #3081](https://github.com/apache/iggy/discussions/3081), [#3252](https://github.com/apache/iggy/discussions/3252))
