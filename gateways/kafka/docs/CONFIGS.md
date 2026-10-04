# Topic configuration

`DescribeConfigs` (32) and `AlterConfigs` (33) apply to Kafka topic resources only
(resource type 2). Only the deprecated `Admin.alterConfigs` (API 33) writes
`retention.ms`. `IncrementalAlterConfigs` (44) stays unimplemented: it is not
advertised and closes the connection like every other unlisted key.

With the bridge off, both APIs answer `NOT_CONTROLLER` (41) for each resource and
do not read or write Iggy.

## What a describe returns

A null or empty `configuration_keys` list returns both keys below. A non-empty
list returns only the requested names, in request order. A name this gateway does
not know is still returned, and that resource's error code is `INVALID_CONFIG`
(40).

| Key | Value | Read only | Source |
| --- | ----- | --------- | ------ |
| `retention.ms` | Sealed-segment lifetime from Iggy `message_expiry`, or `-1` when those segments never expire | no | `DEFAULT_CONFIG` (5) for the unset never-expire default; `DYNAMIC_TOPIC_CONFIG` (1) when a client set it or a stored duration cannot be shown as milliseconds |
| `cleanup.policy` | `delete` | yes | `DEFAULT_CONFIG` (5) |

`is_sensitive` is false for both. `include_synonyms` is honored and the synonym
list is empty either way. From v3, `include_documentation` is honored: true
attaches one sentence per known key, false sends no documentation.

Iggy stores `message_expiry` as `IggyExpiry`. A duration is `IggyDuration`, and
that type counts microseconds. Retention applies to sealed segments only. Iggy
does not time-roll, so the active segment does not expire. Describe divides by
1000 only when the stored count is a positive whole number of milliseconds.
Anything else, including 0 microseconds, is `INVALID_CONFIG` (40) with a null
value and source `DYNAMIC_TOPIC_CONFIG` (1). That expiry was stored; it is not
the never-expire default. The gateway does not round.

## What an alter stores

Only `retention.ms` is written, through `update_topic`'s `message_expiry`. A resource
whose `configs` list does not name `retention.ms` is left unchanged. Omitting the key
does not clear a previously set expiry.

- `-1` stores `IggyExpiry::NeverExpire`. The server treats `0`
  (`IggyExpiry::ServerDefault`) as "leave the current expiry", so `-1` is not
  sent as 0. A following describe reports `-1` with source
  `DYNAMIC_TOPIC_CONFIG`.
- A positive millisecond count is multiplied by 1000 and stored as
  `IggyExpiry::ExpireDuration`. The text must be the canonical decimal of that
  integer (`1`, not `0001` or `+1`). The duration must be at most `u32::MAX`
  seconds, the same limit `IggyExpiry::from_str` enforces. A longer value is
  `INVALID_CONFIG` (40) and is not stored. A following describe reports the same
  decimal with source `DYNAMIC_TOPIC_CONFIG`.
- `0` and every other value are `INVALID_CONFIG` (40).

`cleanup.policy` and any other key are `INVALID_CONFIG` (40). They are not
stored. Kafka has no `NOT_CONFIGURABLE` code. One bad key fails that resource
and writes nothing for it. Other resources in the same request are still
processed.

`validate_only` runs the same checks, including the topic lookup, and does not
call `update_topic`.

## Other resource errors

| Condition | Code | Notes |
| --------- | ---- | ----- |
| Resource type other than topic | `INVALID_REQUEST` (42) | Message is `only topic resources are supported`. Other resources in the batch still run |
| Topic name fails the same rules as CreateTopics | `INVALID_TOPIC_EXCEPTION` (17) | The message is the validation reason and does not repeat the topic name |
| Topic does not exist | `UNKNOWN_TOPIC_OR_PARTITION` (3) | Checked before config keys, so a missing topic is not `INVALID_CONFIG` |
| More than 100 distinct topic names | `POLICY_VIOLATION` (44) | Every resource in the request |
| A topic name repeated across resources | `INVALID_REQUEST` (42) | Every occurrence of the duplicate, not just the second one. Same choice CreateTopics makes for a repeated topic name |
| Non-empty unknown tagged fields | `INVALID_REQUEST` (42) | Request-level tags fail every resource. A resource or config entry's own tags fail that resource. Empty tagged fields are the normal flexible encoding |
