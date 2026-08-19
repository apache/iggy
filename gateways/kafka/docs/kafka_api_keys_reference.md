# Kafka Protocol API Key Reference — Kafka 4.0.0

> **Source**: [`ApiKeys.java` @ Kafka 4.0.0](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java)
> and the canonical [protocol message schemas](https://github.com/apache/kafka/tree/4.0.0/clients/src/main/resources/common/message).
>
> Generated for: **Iggy Kafka Bridge Gateway** — `gateways/kafka/`

---

## Legend

| Symbol | Meaning |
| -------- | --------- |
| 🔴 Bridge | Core data path — must be fully implemented and forwarded to Iggy |
| 🟠 Required Stub | Client state-machine API — must return a well-formed response or clients will stall/crash |
| 🟡 Optional Stub | Admin/observability — can safely return `UNSUPPORTED_VERSION` or `NOT_CONTROLLER` |
| ❌ Reject | Internal broker / KRaft only — return `INVALID_REQUEST` with a well-formed frame; **do not close the connection** |

> This table no longer carries a per-key header-framing status column. `src/protocol/header.rs`
> has no per-key table of its own to be behind or caught up on: it delegates entirely to
> `kafka_protocol::messages::ApiKey::request_header_version` / `response_header_version`. Any key
> that crate recognizes is framed correctly; any key it doesn't (see `header_tests.rs` for the
> known set - inter-broker-only keys 4-7 in Group 14 below) falls back to header v1 (never
> flexible), since an unrecognized key was never negotiated through `ApiVersions` and gets no
> response either way.

---

## KIP-896 Note — Minimum Version Changes in Kafka 4.0

Kafka 4.0 removed all protocol versions older than Kafka 2.1.0 (KIP-896).
Key new minimums:

| API | Old Min | New Min (4.0) |
| ----- | :-------: | :-------------: |
| Produce | 0 | 3 |
| Fetch | 0 | 4 |
| ListOffsets | 0 | 1 |
| OffsetCommit | 0 | 2 |
| OffsetFetch | 0 | 1 |
| CreateTopics | 0 | 2 |
| OffsetForLeaderEpoch | 0 | 1 |

> ⚠️ **KAFKA-18659 / librdkafka bug**: Even though Produce's actual minimum in Kafka 4.0 is v3, the
> `ApiVersions` response **must advertise min=0** for Produce to avoid breaking librdkafka clients.
> `ApiKeys.java` has a dedicated constant `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0` for this.
> Your gateway's `ApiVersions` response encoder must replicate this special case.

---

## Group 1 — Core Data Path

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 0 | **Produce** | 3 † | 12 | v9 | 🔴 Bridge |
| 1 | **Fetch** | 4 | 17 | v12 | 🔴 Bridge |
| 2 | **ListOffsets** | 1 | 9 | v6 | 🟠 Required Stub |
| 3 | **Metadata** | 1 | 12 | v9 | 🔴 Bridge |

† See KAFKA-18659 librdkafka workaround above — advertise min=0 in ApiVersions response.

---

## Group 2 — API Negotiation & Auth

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 17 | **SaslHandshake** | 0 | 1 | never | 🔴 Bridge (auth flow) |
| 18 | **ApiVersions** | 0 | 4 | v3 | 🔴 Bridge (advertise Iggy caps) |
| 36 | **SaslAuthenticate** | 0 | 2 | v2 | 🔴 Bridge (auth flow) |

> **ApiVersions special case**: The response header is **always v0** (no tagged fields), regardless of
> the request version. This allows clients that don't yet know the server's encoding to parse the
> discovery response. `kafka_protocol::messages::ApiVersionsResponse`'s own `HeaderVersion` impl
> already hard-codes this (unconditionally returns 0); `header.rs` just delegates to it.

---

## Group 3 — Classic Consumer Group Protocol

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 8 | **OffsetCommit** | 2 | 9 | v8 | 🟠 Required Stub |
| 9 | **OffsetFetch** | 1 | 9 | v6 | 🟠 Required Stub |
| 10 | **FindCoordinator** | 1 | 6 | v3 | 🟠 Required Stub |
| 11 | **JoinGroup** | 2 | 9 | v6 | 🟠 Required Stub |
| 12 | **Heartbeat** | 1 | 4 | v4 | 🟠 Required Stub |
| 13 | **LeaveGroup** | 1 | 5 | v4 | 🟠 Required Stub |
| 14 | **SyncGroup** | 1 | 5 | v4 | 🟠 Required Stub |
| 15 | **DescribeGroups** | 0 | 6 | v5 | 🟡 Optional Stub |
| 16 | **ListGroups** | 1 | 5 | v3 | 🟡 Optional Stub |
| 42 | **DeleteGroups** | 1 | 2 | v2 | 🟡 Optional Stub |

---

## Group 4 — New Consumer Group Protocol (KIP-848, Kafka 3.7+)

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 68 | **ConsumerGroupHeartbeat** | 0 | 1 | v0 | 🟠 Required Stub |
| 69 | **ConsumerGroupDescribe** | 0 | 1 | v0 | 🟡 Optional Stub |

> ⚠️ Kafka 4.0 clients use the **new group protocol by default** and will send key 68.
> A gateway that hard-rejects this breaks all modern Kafka consumers.

---

## Group 5 — Topic Administration

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 19 | **CreateTopics** | 2 | 7 | v5 | 🟠 Required Stub |
| 20 | **DeleteTopics** | 1 | 6 | v4 | 🟡 Optional Stub |
| 21 | **DeleteRecords** | 0 | 2 | v2 | 🟡 Optional Stub |
| 37 | **CreatePartitions** | 0 | 3 | v2 | 🟡 Optional Stub |

> ⚠️ `SUPPORTED_RANGES` in `api.rs` currently advertises CreateTopics max=5; actual max is v7.

---

## Group 6 — Transactions (EOS — Exactly Once Semantics)

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 22 | **InitProducerId** | 2 | 5 | v2 | 🟡 Optional Stub |
| 23 | **OffsetForLeaderEpoch** | 1 | 5 | v4 | 🟡 Optional Stub |
| 24 | **AddPartitionsToTxn** | 1 | 5 | v3 | 🟡 Optional Stub |
| 25 | **AddOffsetsToTxn** | 1 | 4 | v3 | 🟡 Optional Stub |
| 26 | **EndTxn** | 1 | 4 | v3 | 🟡 Optional Stub |
| 27 | **WriteTxnMarkers** | 0 | 1 | v1 | 🟡 Optional Stub |
| 28 | **TxnOffsetCommit** | 2 | 5 | v3 | 🟡 Optional Stub |

---

## Group 7 — Security & ACLs

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 29 | **DescribeAcls** | 0 | 3 | v2 | 🟡 Optional Stub |
| 30 | **CreateAcls** | 0 | 3 | v2 | 🟡 Optional Stub |
| 31 | **DeleteAcls** | 0 | 3 | v2 | 🟡 Optional Stub |
| 38 | **CreateDelegationToken** | 0 | 3 | v2 | 🟡 Optional Stub |
| 39 | **RenewDelegationToken** | 0 | 2 | v2 | 🟡 Optional Stub |
| 40 | **ExpireDelegationToken** | 0 | 2 | v2 | 🟡 Optional Stub |
| 41 | **DescribeDelegationToken** | 0 | 3 | v2 | 🟡 Optional Stub |
| 50 | **DescribeUserScramCredentials** | 0 | 0 | v0 | 🟡 Optional Stub |
| 51 | **AlterUserScramCredentials** | 0 | 0 | v0 | 🟡 Optional Stub |

---

## Group 8 — Configuration & Quotas

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 32 | **DescribeConfigs** | 0 | 4 | v4 | 🟡 Optional Stub |
| 33 | **AlterConfigs** | 0 | 2 | v2 | 🟡 Optional Stub |
| 44 | **IncrementalAlterConfigs** | 0 | 1 | v1 | 🟡 Optional Stub |
| 48 | **DescribeClientQuotas** | 0 | 1 | v1 | 🟡 Optional Stub |
| 49 | **AlterClientQuotas** | 0 | 1 | v1 | 🟡 Optional Stub |

---

## Group 9 — Log & Partition Admin

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 34 | **AlterReplicaLogDirs** | 0 | 2 | v2 | 🟡 Optional Stub |
| 35 | **DescribeLogDirs** | 0 | 4 | v2 | 🟡 Optional Stub |
| 43 | **ElectLeaders** | 0 | 2 | v2 | 🟡 Optional Stub |
| 45 | **AlterPartitionReassignments** | 0 | 0 | v0 | 🟡 Optional Stub |
| 46 | **ListPartitionReassignments** | 0 | 0 | v0 | 🟡 Optional Stub |
| 47 | **OffsetDelete** | 0 | 0 | never | 🟡 Optional Stub |
| 57 | **UpdateFeatures** | 0 | 1 | v1 | 🟡 Optional Stub |

---

## Group 10 — Cluster Introspection

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 55 | **DescribeQuorum** | 0 | 2 | v0 | 🟡 Optional Stub |
| 59 | **FetchSnapshot** | 0 | 1 | v0 | 🟡 Optional Stub |
| 60 | **DescribeCluster** | 0 | 1 | v0 | 🟡 Optional Stub |
| 61 | **DescribeProducers** | 0 | 0 | v0 | 🟡 Optional Stub |
| 64 | **UnregisterBroker** | 0 | 0 | v0 | 🟡 Optional Stub |
| 65 | **DescribeTransactions** | 0 | 0 | v0 | 🟡 Optional Stub |
| 66 | **ListTransactions** | 0 | 1 | v0 | 🟡 Optional Stub |
| 75 | **DescribeTopicPartitions** | 0 | 0 | v0 | 🟡 Optional Stub |

---

## Group 11 — Observability / Telemetry (KIP-714, Kafka 3.7+)

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 71 | **GetTelemetrySubscriptions** | 0 | 0 | v0 | 🟡 Optional Stub |
| 72 | **PushTelemetry** | 0 | 0 | v0 | 🟡 Optional Stub |
| 76 | **ListClientMetricsResources** | 0 | 0 | v0 | 🟡 Optional Stub |

---

## Group 12 — Share Groups (NEW in Kafka 4.0, KIP-932)

> Keys 77–80 use flexible header framing from v0 (added to `header.rs`). Keys 84–88 are internal
> coordinator APIs and can be rejected.

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 77 | **ShareGroupHeartbeat** | 0 | 0 | v0 | 🟠 Required Stub |
| 78 | **ShareGroupDescribe** | 0 | 0 | v0 | 🟡 Optional Stub |
| 79 | **ShareFetch** | 0 | 0 | v0 | 🔴 Bridge (share consume) |
| 80 | **ShareAcknowledge** | 0 | 0 | v0 | 🟠 Required Stub |

---

## Group 13 — KRaft Raft Voter Management (NEW in Kafka 4.0)

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 81 | **AddRaftVoter** | 0 | 0 | v0 | ❌ Reject (internal) |
| 82 | **RemoveRaftVoter** | 0 | 0 | v0 | ❌ Reject (internal) |
| 83 | **UpdateRaftVoter** | 0 | 0 | v0 | ❌ Reject (internal) |

---

## Group 14 — KRaft Internal / Broker-Only (Always Reject at Gateway)

> These APIs must **never** be handled by a client-facing gateway.
> Return `INVALID_REQUEST` (error code 42) with a properly framed response — **do not drop the connection**.

| Key | API Name | Min (4.0) | Max (4.0) | Flexible From | Gateway Action |
| :---: | ---------- | :---------: | :---------: | :-------------: | :--------------: |
| 4 | **LeaderAndIsr** | 0 | 7 | v4 | ❌ Reject (broker-only) |
| 5 | **StopReplica** | 0 | 4 | v2 | ❌ Reject (broker-only) |
| 6 | **UpdateMetadata** | 0 | 8 | v6 | ❌ Reject (broker-only) |
| 7 | **ControlledShutdown** | 0 | 3 | v3 | ❌ Reject (broker-only) |
| 52 | **Vote** | 0 | 1 | v0 | ❌ Reject (KRaft) |
| 53 | **BeginQuorumEpoch** | 0 | 1 | v0 | ❌ Reject (KRaft) |
| 54 | **EndQuorumEpoch** | 0 | 1 | v0 | ❌ Reject (KRaft) |
| 56 | **AlterPartition** | 0 | 3 | v0 | ❌ Reject (KRaft) |
| 58 | **Envelope** | 0 | 0 | v0 | ❌ Reject (KRaft) |
| 62 | **BrokerRegistration** | 0 | 4 | v0 | ❌ Reject (KRaft) |
| 63 | **BrokerHeartbeat** | 0 | 1 | v0 | ❌ Reject (KRaft) |
| 67 | **AllocateProducerIds** | 0 | 0 | v0 | ❌ Reject (KRaft) |
| 70 | **ControllerRegistration** | 0 | 0 | v0 | ❌ Reject (KRaft) |
| 74 | **AssignReplicasToDirs** | 0 | 0 | v0 | ❌ Reject (KRaft) |
| 84 | **InitializeShareGroupState** | 0 | 0 | v0 | ❌ Reject (internal) |
| 85 | **ReadShareGroupState** | 0 | 0 | v0 | ❌ Reject (internal) |
| 86 | **WriteShareGroupState** | 0 | 0 | v0 | ❌ Reject (internal) |
| 87 | **DeleteShareGroupState** | 0 | 0 | v0 | ❌ Reject (internal) |
| 88 | **ReadShareGroupStateSummary** | 0 | 0 | v0 | ❌ Reject (internal) |

---

## Summary Counts

| Category | Count | Notes |
| ---------- | :-----: | ------- |
| 🔴 Bridge (data path) | 7 | Produce, Fetch, Metadata, SaslHandshake, ApiVersions, SaslAuthenticate, ShareFetch |
| 🟠 Required Stub (client state machine) | 12 | ListOffsets, consumer group (8-14), CreateTopics, ConsumerGroupHeartbeat (68), ShareGroupHeartbeat (77), ShareAcknowledge (80) |
| 🟡 Optional Stub (admin/observability) | 47 | Can return `UNSUPPORTED_VERSION` or `NOT_CONTROLLER` safely |
| ❌ Reject (broker/KRaft internal) | 22 | Return `INVALID_REQUEST` with valid frame — never close the TCP connection |
| **Total API Keys in this document** | **88** | Key IDs 0-88 with a gap at 73 |

---

## Current implementation gaps

### `SUPPORTED_RANGES` is behind the latest Kafka 4.0 max versions

| API | Declared range | Kafka 4.0 max | Gap |
| ----- | :---: | :---: | :---: |
| Produce | v3-v9 | v12 | 3 versions behind |
| Fetch | v4-v12 | v17 | 5 versions behind |
| ListOffsets | v1-v6 | v9 | 3 versions behind |
| Metadata | v0-v9 | v12 | 3 versions behind |
| ApiVersions | v0-v3 | v4 | 1 version behind |
| CreateTopics | v2-v5 | v7 | 2 versions behind |

### Missing from `SUPPORTED_RANGES` (82 of the 88 API keys in this document)

Every key not in `SUPPORTED_RANGES` closes the connection - the same policy applied to every
other unlisted key, not a special case for these. No api-specific response schema exists for an
unlisted key, so any body the gateway could send would be misparsed by the client against the
schema it expected. This includes:

- **Client bootstrap blockers**: OffsetCommit (8), OffsetFetch (9), FindCoordinator (10)
- **Classic consumer group protocol**: JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14)
- **New consumer group protocol**: ConsumerGroupHeartbeat (68) — default in Kafka 4.0
- **Share groups (KIP-932)**: ShareFetch, ShareGroupHeartbeat, ShareAcknowledge
- **Auth flow**: SaslHandshake (17), SaslAuthenticate (36)
- **All broker/KRaft-internal keys** (Group 14)

Remaining scope (consumer groups, auth, admin/tuning) is tracked in `SCOPE.md`'s TODO section,
not duplicated here.

### Header framing for keys `kafka_protocol` doesn't recognize

`src/protocol/header.rs` delegates to `kafka_protocol::messages::ApiKey`; there is no per-key
table in this codebase to have gaps in. `header_tests.rs` documents the one case where the crate
itself has a gap: keys 4-7 (LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown, Group
14 below) are inter-broker-only APIs `kafka_protocol` 0.17 does not implement at all, so
`ApiKey::try_from` fails for them and the gateway's wrapper falls back to header v1 (never
flexible) - moot in practice, since none of these keys are ever dispatched (`SUPPORTED_RANGES`
closes the connection before any header-version lookup for an unlisted key would matter).

## References

- [ApiKeys.java @ Kafka 4.0.0](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java)
- [Kafka Protocol Message Schemas @ 4.0.0](https://github.com/apache/kafka/tree/4.0.0/clients/src/main/resources/common/message)
- [KIP-896: Remove old client protocol API versions in Kafka 4.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)
- [KIP-848: Apache Kafka Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-932: Queues for Kafka (Share Groups)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [KIP-714: Client Metrics and Observability](https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability)
- [Kafka Wire Protocol Documentation](https://kafka.apache.org/protocol.html)
- [kafka-protocol Rust crate](https://crates.io/crates/kafka-protocol)
