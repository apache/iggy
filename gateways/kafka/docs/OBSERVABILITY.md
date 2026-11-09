# Metrics, readiness and liveness

The gateway serves Prometheus metrics, a readiness check and a liveness check over HTTP, at the
address in `IGGY_KAFKA_HTTP_ADDR`. The endpoint has no authentication and no TLS, as with a
Prometheus exporter. Bind it where only the scraper and the orchestrator reach it.

The variable is unset by default, so the binary serves no endpoint. The image sets
`IGGY_KAFKA_HTTP_ADDR=0.0.0.0:9096`.

## Readiness and liveness

`/ready` answers 200 with `ready` while the gateway accepts clients and the bridge reaches Iggy.
Otherwise it answers 503 and the reason:

| Answer | When |
| --- | --- |
| `not ready: starting` | The gateway waits for Iggy, loads the cluster id, or sets up the coordinator log. Startup gives up after 60 s and stops the process |
| `not ready: Iggy is unreachable: <reason>` | Two pings of Iggy failed in a row |
| `not ready: shutting down` | The gateway received SIGTERM or Ctrl-C and drains its connections |

The bridge pings Iggy each second on a client of its own. A ping that takes more than 2 s fails.
After two failed checks in a row, `/ready` answers 503, about 5 s after Iggy stops or stops
answering. After a failed ping, the next check dials every node the bridge knows at once, and the
first node that answers within 2 s counts. So one dead node does not make the gateway unready while
another node answers. One check that succeeds makes the gateway ready again.

The ping checks that one Iggy node answers and that the bridge's user signs in. It does not check
that the cluster takes writes. Error codes in the metrics show that.

`/live` answers 200 with `live` while the process answers. It stays 200 while Iggy is unreachable,
because a restart does not bring Iggy back.

The endpoint starts before the bridge connects. An orchestrator therefore sees a gateway that waits
for Iggy as live and not ready, and does not restart it.

`iggy-gateway-kafka --check-ready` asks `/ready` at `IGGY_KAFKA_HTTP_ADDR` and exits 0 on 200.
`--check-live` asks `/live`. On any other answer, or no answer within 3 s, it prints the reason and
exits 1. An unspecified address such as `0.0.0.0` is asked on the loopback address. The image's
`HEALTHCHECK` runs `--check-live`, so the image needs no HTTP client and an Iggy outage restarts no
gateway. `docker-compose.yml` runs `--check-ready`, so `depends_on` waits for Iggy.

## Metrics

`/metrics` answers in the OpenMetrics text format. Every name starts with `iggy_kafka_gateway_`.

### Requests

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `requests_total` | counter | `api`, `version` | Kafka requests answered |
| `request_duration_seconds` | histogram | `api` | Time from a request read in full to its response written |
| `response_errors_total` | counter | `api`, `code`, `error` | Error codes in the responses sent |

`api` is the Kafka name of the API, such as `Produce` or `OffsetCommit`. A key that Kafka does not
define is `unknown`. `version` is the request version, or `unsupported` for a version that the
gateway does not serve. A client therefore cannot add a label value per key or version it makes up.

`response_errors_total` counts each code other than `NONE` in a response: the top-level code, and
the code of each topic, partition, group or resource. A Produce response with three failed
partitions adds 3. Kafka counts its `ErrorsPerSec` the same way. `code` is the number, and `error`
is the name in Kafka's `Errors` enum, such as `NOT_LEADER_OR_FOLLOWER`.

The gateway reads the codes back from the response it encoded. The count therefore matches the
codes that clients receive.

### Connections

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `connections_accepted_total` | counter | `listener` | Connections accepted |
| `connections_refused_total` | counter | `listener` | Connections refused, because the listener held `IGGY_KAFKA_MAX_CONNECTIONS` |
| `connections_closed_total` | counter | `listener`, `cause` | Connections closed, per cause |
| `connections_open` | gauge | `listener` | Connections open now |
| `connections_limit` | gauge | `listener` | `IGGY_KAFKA_MAX_CONNECTIONS` |

`listener` is the security protocol of the listener: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or
`SASL_SSL`.

### Pools and Produce

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `slots_used` | gauge | `pool` | Slots that requests hold now |
| `slots` | gauge | `pool` | Slots of the pool |
| `produce_memory_used_bytes` | gauge | | Bytes of decoded Produce partitions held now |
| `produce_memory_capacity_bytes` | gauge | | Bytes of decoded Produce partitions held at most |
| `produce_sessions` | gauge | | Producers with an Iggy session of their own |
| `produce_sessions_limit` | gauge | | `IGGY_KAFKA_PRODUCE_SESSIONS` |
| `produce_shared_producers` | gauge | | Producers that found every session taken, and share one Iggy client |
| `produce_sends_in_doubt_total` | counter | | Produce sends whose outcome is unknown |

The pools are:

| `pool` | Slots |
| --- | --- |
| `fetch` | Fetch read slots, `IGGY_KAFKA_FETCH_SLOTS` |
| `produce_large` | Large Produce requests that decode and send at once |
| `authentication` | SASL credential checks, `IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS`. Only with SASL |
| `certificate_lookup` | Lookups of the Iggy user that a client certificate names. Only with client certificates |

A pool that is full for a long time makes requests wait. Fetch then answers late, and Produce
answers `REQUEST_TIMED_OUT`.

A send is in doubt in two cases. Its answer did not come back by the deadline, or the connection
broke during the send. The client gets `REQUEST_TIMED_OUT` (7) and retries. The
records can still land, so a client that is not idempotent can write them twice.

The pools, the Produce metrics and the in-doubt count appear only with the bridge.

### Consumer groups and the coordinator log

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `groups` | gauge | `state` | Consumer groups that this gateway coordinates, per Kafka state |
| `group_members` | gauge | | Members of those groups |
| `coordinator_partitions_served` | gauge | | Coordinator log partitions that this gateway serves |
| `coordinator_partition_served` | gauge | `partition` | 1 for each partition that this gateway serves, else 0 |
| `coordinator_lease_age_seconds` | gauge | | Seconds since this gateway last read its share of the partitions from Iggy |

`state` is `Empty`, `PreparingRebalance`, `CompletingRebalance` or `Stable`, as `ListGroups`
reports it.

The coordinator metrics appear only with the coordinator log. The gateway reads its lease each
second. At a lease age of 10 s it stops serving its partitions, and their groups move to other
gateways.

### Gateway

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `ready` | gauge | | 1 while `/ready` answers 200 |
| `iggy_reachable` | gauge | | 1 while the bridge reaches Iggy. Only with the bridge |
| `build_info` | gauge | `version` | 1, with the gateway's version |

## Closed connections

Each closed connection counts once in `connections_closed_total`, with its cause. The log line names
the peer, the listener, the cause and a detail, such as the error or the request.

| `cause` | Level | Meaning |
| --- | --- | --- |
| `client_closed` | debug | The client closed or reset the connection |
| `idle_timeout` | debug | No request within `IGGY_KAFKA_IDLE_TIMEOUT_SECS`, or within `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` before authentication |
| `shutdown` | debug | The gateway shut down |
| `user_revoked` | info | The Iggy user of the connection was deleted, deactivated or renamed |
| `read_timeout` | warn | A request did not arrive in full within `IGGY_KAFKA_READ_TIMEOUT_SECS` |
| `write_timeout` | warn | The client did not take a response within `IGGY_KAFKA_WRITE_TIMEOUT_SECS` |
| `frame_too_large` | warn | A frame is larger than `IGGY_KAFKA_MAX_FRAME_SIZE`. A TLS or HTTP client on a plaintext listener looks the same |
| `invalid_frame` | warn | A frame has a length of zero or less |
| `invalid_request` | warn | A request header does not decode |
| `request_refused` | warn | No response can answer a request, such as an API key or version that the gateway does not serve, or a body that does not decode |
| `unacknowledged_produce_failed` | warn | Iggy did not take the records of a Produce with `acks=0` in time. Kafka closes such a connection, so the client refreshes its metadata |
| `illegal_sasl_state` | warn | A request came before SASL authentication. The client is not configured for SASL on this listener |
| `unsupported_sasl_mechanism` | warn | The client asked for a SASL mechanism other than `PLAIN` |
| `authentication_failed` | warn | Iggy rejected the credentials, or the token was malformed |
| `authentication_dropped` | warn | The credentials, or the user a client certificate names, were not checked. `detail` says why: the peer was throttled after a rejected login, no check slot came free, the check timed out, or Iggy was unreachable. A check cut short by a shutdown counts as `shutdown` |
| `tls_handshake_failed` | warn | The TLS handshake failed or did not finish in time. A plaintext client on a TLS listener looks the same |
| `certificate_rejected` | warn | The client certificate names no active Iggy user, or no mapping rule matches its subject |
| `connection_limit` | warn | The listener held `IGGY_KAFKA_MAX_CONNECTIONS`. Counted in `connections_refused_total` |
| `io_error` | warn | Any other socket error |
| `internal_error` | error | A state that the gateway's checks rule out |

Warn and error lines are rate limited per cause: at most 5 lines in each 10 s. The first line after
a quiet spell carries `left_out`, the number of lines of that cause that the limit dropped. The
counter counts every connection.

A log line never names the user of a failed login, so the log does not show which accounts exist.

## Scrape configuration

For Prometheus without the Prometheus Operator, scrape each gateway directly:

```yaml
scrape_configs:
  - job_name: iggy-kafka-gateway
    static_configs:
      - targets: ["gateway-1:9096", "gateway-2:9096", "gateway-3:9096"]
```

The [`iggy-kafka-gateway` chart](../../../helm/charts/iggy-kafka-gateway) creates a ServiceMonitor
with `serviceMonitor.enabled=true`.
