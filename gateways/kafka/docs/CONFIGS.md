# Topic configuration

`DescribeConfigs` (32) and `AlterConfigs` (33) apply to Kafka topic resources only
(resource type 2). Only the deprecated `Admin.alterConfigs` (API 33) writes
retention. `IncrementalAlterConfigs` (44) is not in this change and is not
advertised in `ApiVersions`. A client picks its admin API from that response, so
a Java or librdkafka client that would otherwise send 44 raises an
`UnsupportedVersionException` (or the librdkafka equivalent) locally and never
sends the request - the gateway never sees it, and the connection is not
affected. Only `AlterConfigs` (33) writes retention.

With the bridge off, both APIs answer `NOT_CONTROLLER` (41) for each resource and
do not read or write Iggy.

## Authorization

`AlterConfigs` requires the `manage_topics` (or `manage_streams`) Iggy permission
on the authenticated principal when SASL is enabled. A principal without it gets
`TOPIC_AUTHORIZATION_FAILED` (29) for every resource in the request, and
`update_topic` is never called. With SASL disabled there is no principal to
check, and the request is not gated here. `DescribeConfigs` performs no
authorization check of its own.

## What a describe returns

A null or empty `configuration_keys` list returns both keys below. A non-empty
list returns only the requested names this gateway knows, in request order,
with a name repeated in the list collapsed to one entry. A name it does not
know, including `retention.minutes` and `retention.hours`, is omitted. The
resource's error code stays `NONE` (0), and the known keys in that request are
still returned.

| Key | Value | Read only | Source |
| --- | ----- | --------- | ------ |
| `retention.ms` | Sealed-segment lifetime from Iggy `message_expiry`, or `-1` when those segments never expire | no | `DEFAULT_CONFIG` (5) for the unset never-expire default; `DYNAMIC_TOPIC_CONFIG` (1) when a client set it or a stored duration cannot be shown as milliseconds |
| `cleanup.policy` | `delete` | yes | `DEFAULT_CONFIG` (5) |

`is_sensitive` is false for both. From v3, `include_documentation` is honored:
true attaches one sentence per known key, false sends no documentation.
`include_synonyms` has no effect: this gateway models no config-inheritance
chain beyond `retention.ms` itself, so the synonym list on every entry is
always empty.

Iggy stores `message_expiry` as `IggyExpiry`. A duration is `IggyDuration`, and
that type counts microseconds. Retention applies to sealed segments only. Iggy
does not time-roll, so the active segment does not expire. Describe divides by
1000 only when the stored count is a positive whole number of milliseconds.
Anything else, including 0 microseconds, is `INVALID_CONFIG` (40) with a null
value and source `DYNAMIC_TOPIC_CONFIG` (1). That expiry was stored; it is not
the never-expire default. The gateway does not round.

## What an alter stores

`retention.ms` is the only writable duration. Iggy stores milliseconds, through
`update_topic`'s `message_expiry`. `retention.minutes` and `retention.hours` are
not real Kafka topic-level configs - only the broker-level `log.retention.minutes`
and `log.retention.hours` exist in real Kafka - so this gateway rejects both with
`INVALID_CONFIG` (40), the same as any other unknown key.

- `retention.ms` is the source of truth, and the only retention key this
  gateway writes.
- `-1` stores `IggyExpiry::NeverExpire`. The server treats `0`
  (`IggyExpiry::ServerDefault`) as "leave the current expiry", so `-1` is
  not sent as 0. A following describe reports `-1` with source
  `DYNAMIC_TOPIC_CONFIG`.
- A positive count must be the canonical decimal of that integer (`1`, not
  `0001` or `+1`). The resulting duration must be at most `u32::MAX` seconds,
  the same limit `IggyExpiry::from_str` enforces. A longer value is
  `INVALID_CONFIG` (40) and is not stored. A following describe of
  `retention.ms` reports that millisecond count with source
  `DYNAMIC_TOPIC_CONFIG`.
- `0` and every other value are `INVALID_CONFIG` (40).

Kafka's `AlterConfigs` replaces a topic's full configuration. An empty `configs`
list on that API is a reset to defaults. This gateway patches the named keys
instead of replacing the set, so an empty list is `INVALID_CONFIG` (40) and
writes nothing. A previously set expiry stays in place.

`cleanup.policy`, `retention.minutes`, `retention.hours`, and any other key are
`INVALID_CONFIG` (40). They are not stored. Kafka has no `NOT_CONFIGURABLE`
code. One bad key fails that resource and writes nothing for it. Other
resources in the same request are still processed.

`validate_only` runs the same checks, including the topic lookup, and does not
call `update_topic`.

## Limitations

- `IncrementalAlterConfigs` (API 44) is not implemented here. It is planned
  after this work merges. It is not advertised, so a client falls back to
  `AlterConfigs` (33); see "Authorization" above for what an unadvertised key
  does to the client.
- `retention.ms` remains the only value Iggy stores.

## Other resource errors

| Condition | Code | Notes |
| --------- | ---- | ----- |
| Caller lacks `manage_topics` (SASL on, `AlterConfigs` only) | `TOPIC_AUTHORIZATION_FAILED` (29) | Checked before the topic lookup. Every resource in the request |
| Resource type other than topic | `INVALID_REQUEST` (42) | Message is `only topic resources are supported`. Other resources in the batch still run |
| Topic name fails the same rules as CreateTopics | `INVALID_TOPIC_EXCEPTION` (17) | The message is the validation reason and does not repeat the topic name |
| Topic does not exist | `UNKNOWN_TOPIC_OR_PARTITION` (3) | Checked before config keys, so a missing topic is not `INVALID_CONFIG` |
| Empty `configs` list on alter | `INVALID_CONFIG` (40) | Kafka would replace the set, which is a reset. This gateway patches named keys, so an empty list is rejected and writes nothing |
| More than 100 distinct topic names | `POLICY_VIOLATION` (44) | Every resource in the request, checked before the bridge-availability check |
| A topic name repeated across resources | `INVALID_REQUEST` (42) | Every occurrence of the duplicate, not just the second one. Same choice CreateTopics makes for a repeated topic name |

Unknown tagged fields at the request, resource, or config-entry level are
ignored, not rejected: Kafka's flexible versions define them as a forward-compatible
extension point a server that does not recognize them must skip, not refuse.
