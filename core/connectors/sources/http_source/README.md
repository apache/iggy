# HTTP Source Connector (Webhook Gateway)

The HTTP source connector turns Apache Iggy into a webhook receiver. It runs an embedded HTTP server, accepts `POST` bodies on authenticated paths, and produces them to the instance's configured stream and topic as raw bytes.

Every instance of this plugin shares one listener, so a single port can serve many providers, each routed to its own topic.

## Delivery semantics (read this first)

> **Delivery guarantee: best-effort.** HTTP 200 means "accepted into the connector's in-memory buffer", not "durably stored in Iggy". This is weaker than at-most-once: a message can be lost after acknowledgment, with no record beyond a log line and a metric.

Loss windows, explicitly:

1. **Process crash** between HTTP 200 and the producer send. Buffered messages are volatile.
2. **Producer failure.** Narrowed by #3855, which gave sources a delivery result. The runtime NACKs a batch it could not send, or whose state it could not persist, and neither is abandoned here: a message batch is held and replayed on the next `poll()`, and a state-only batch re-arms the flush so the mutation is handed out again. Answering 200 already told the sender this gateway owns the event, and the only honest way to shed load is the 429 handlers return once the bridge fills.
3. **Shutdown.** Narrowed by #3321, which closes the plugin before tearing down the forwarding channel so in-flight batches drain. Messages still in the bridge when the poll task stops are lost; the connector logs the count and increments `http_source_dropped_on_close_total`.
4. **Poll task stopped by the SDK.** A source is stopped after five consecutive NACKs, roughly 1.5s of backoff plus five send rounds, so a broker outage longer than that ends the poll task while the listener keeps accepting. Whatever the bridge holds is then lost on the next restart, and the stop is unobservable: the runtime's forwarding loop stays parked, so the source keeps being counted as running while nothing polls. Tracked in #3941.

What mitigates this in practice is the caller: webhook senders such as GitHub, Stripe, and Twilio retry on timeout and 5xx, so the sender side is at-least-once up to the moment this connector returns 200. The connector's job is to make the post-200 window as small and as observable as possible.

**Duplicates are also possible.** The same retries that mitigate loss create a duplicate window:

1. A caller times out after the connector enqueued the message but before the 200 reached it, retries, and the payload lands twice.
2. A load balancer or proxy retries a POST after a hiccup downstream of a successful enqueue.

So the guarantee is best-effort in both directions: no silent-loss guarantee and no dedup guarantee. Consumers that need effectively-once processing should dedupe on the provider's delivery id (`X-GitHub-Delivery`, `svix-id`, Stripe's `event.id` in the body), which is why forwarding those headers is the default recommendation.

Stronger guarantees need an SDK change, not a connector change: at-least-once by construction requires the runtime to hand the connector a producer handle so it can await the send before answering 200. See #3039.

## Configuration

Two instances sharing one listener, each routing to its own topic. Each block is a separate connector configuration file.

```toml
# instance 1: GitHub webhooks -> webhooks/github_events
type = "source"
key = "http_github"
enabled = true
version = 0
name = "HTTP source (GitHub)"
path = "../../target/release/libiggy_connector_http_source"
verbose = false

[[streams]]
stream = "webhooks"
topic = "github_events"
schema = "raw"
batch_length = 100
linger_time = "5ms"

[plugin_config]
# Loopback so a copied snippet cannot expose a port. Production wants 0.0.0.0
# behind a load balancer, with TLS terminated in front of it, and real secrets
# supplied through the runtime's env overrides rather than written here.
listen_addr = "127.0.0.1:9090"
admin_listen_addr = "127.0.0.1:9091"
instance_name = "http_github"
topic_path = "github_events"
auth_bearer_token = "replace_with_secret_token"
management_token = "replace_with_admin_token"
max_body_size_bytes = 1048576
buffer_capacity = 10000
max_batch_size = 500
include_http_metadata = true
forward_headers = ["X-GitHub-Delivery", "X-Request-ID"]

[[plugin_config.endpoints]]
endpoint_id = "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d"
auth_type = "hmac-sha256"
auth_secret = "whsec_github_example"
hmac_header = "X-Hub-Signature-256"
hmac_prefix = "sha256="
```

```toml
# instance 2: partner webhooks -> webhooks/partner_events, joining the same listener
type = "source"
key = "http_partner"
enabled = true
version = 0
name = "HTTP source (partner)"
path = "../../target/release/libiggy_connector_http_source"

[[streams]]
stream = "webhooks"
topic = "partner_events"
schema = "raw"

[plugin_config]
listen_addr = "127.0.0.1:9090"
admin_listen_addr = "127.0.0.1:9091"
instance_name = "http_partner"
management_token = "replace_with_admin_token"
# no topic_path: this instance serves secret-path endpoints only

[[plugin_config.endpoints]]
endpoint_id = "0b7d9e2f4a6c8e1d3b5f7a9c2e4d6f81"
auth_type = "bearer"
auth_secret = "partner-token"
```

The stream's `schema` **must be `raw`**. This connector always produces raw bodies; with `schema = "json"` the runtime's encoder rejects every message, which counts as a processing error and becomes a NACK rather than a drop.

The batch is then replayed on every poll and the SDK stops the poll task after five consecutive NACKs, while the listener keeps answering 200. The connector cannot guard against this itself: `schema` lives under `[[streams]]` and the plugin only ever receives `[plugin_config]`, so nothing in `open()` can see it.

### Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `listen_addr` | string | required | Public listener. Every instance sharing it must configure the identical value. |
| `admin_listen_addr` | string | `127.0.0.1:9091` | Management API, admin health, and metrics. Never route this through a public load balancer. |
| `instance_name` | string | runtime id | Identifies the instance in message headers and on the admin listener. The default is the plugin's numeric runtime id, assigned in load order and **not stable across restarts** — set it explicitly in production. |
| `topic_path` | string | none | Exposes `POST /topics/{topic_path}`. Unset leaves only secret-path endpoints. |
| `auth_bearer_token` | string | none | Guards the named topic path. Unset leaves it unauthenticated, for deployments behind an authenticating gateway. |
| `management_token` | string | none | Enables `/admin/endpoints`. Unset means the management API does not exist. |
| `max_body_size_bytes` | usize | `1048576` | Request body limit, enforced by the `Bytes` extractor. Routing wins over it: an oversized POST to an unknown path answers 404, not 413. On a known path the body is buffered before authentication, which HMAC-over-raw-body requires. Must match across instances sharing a listener. |
| `buffer_capacity` | usize | `10000` | Messages the instance bridge holds. A full bridge answers 429, though see Backpressure: until #3795 lands that signals an arrival burst, not a slow Iggy. |
| `max_batch_size` | usize | `500` | Maximum messages a single `poll()` returns. |
| `include_http_metadata` | bool | `true` | Adds instance, peer address, and receive time as message headers. |
| `forward_headers` | array | `[]` | Request headers copied onto the message. Invalid names fail `open()`, as do `Authorization`, `Proxy-Authorization`, and `Cookie` — forwarding a reusable credential would copy it onto every message and persist it in the log. |
| `endpoints` | array | `[]` | Static secret-path endpoints. |

### Endpoint options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `endpoint_id` | string | required | Exactly 32 lowercase hex characters. Generate with `openssl rand -hex 16`. |
| `auth_type` | string | `none` | `none`, `bearer`, `hmac-sha256`, or `hmac-sha1`. |
| `auth_secret` | string | none | Required unless `auth_type` is `none`. |
| `hmac_header` | string | `X-Hub-Signature-256` | Header carrying the signature. |
| `hmac_prefix` | string | `sha256=` | Prefix stripped before hex-decoding. Use `""` for a bare hex signature. |
| `expires_at` | u64 | none | Unix seconds. Requests arriving at or after this answer 404. |

`HttpSourceConfig` deliberately does not implement `Serialize`, so this connector cannot write a credential out by accident. That does **not** protect the values in your TOML: the runtime keeps plugin configuration as raw JSON and serves it verbatim from `GET /sources/{key}/configs/plugin` (and inside `/configs` and `/configs/active`), so anyone who can reach the runtime's control API can read every secret configured here. Treat that API as privileged. (`/stats` carries no plugin configuration.)

## Routing

Two kinds of path, both `POST`:

```text
POST /topics/{topic_path}   named path, one per instance, guarded by auth_bearer_token
POST /e/{endpoint_id}       secret path, many per instance, each with its own auth
```

A secret-path URL is itself the credential: 32 hex characters is 128 bits of entropy, the same model as a Slack webhook URL. Treat these URLs as secrets and prefer adding an HMAC on top for providers that support one.

Paths must be unique across every instance sharing a listener. Two instances claiming the same `topic_path` or the same `endpoint_id` fail the second instance's `open()` rather than letting one silently take the other's traffic.

## Request and response contract

```text
POST /e/a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d
X-Hub-Signature-256: sha256=...
Content-Type: application/json

{"event": "push", "repository": "apache/iggy"}
```

| Status | Condition | Body |
| ------ | --------- | ---- |
| 200 | Accepted into the bridge | `{"status":"queued"}` |
| 401 | Bearer or HMAC validation failed | `{"error":"unauthorized"}` |
| 404 | Unknown path, or a revoked endpoint | `{"error":"not found"}` |
| 400 | Malformed request body, e.g. the client reset mid-send | `{"error":"bad request"}` |
| 413 | Body over `max_body_size_bytes` | `{"error":"payload too large"}` |
| 429 | Bridge full | `{"error":"service temporarily unavailable"}` plus `Retry-After: 1` |
| 503 | `GET /health` with no instance serving | `{"status":"unavailable"}` |

Revoked and expired endpoints both answer 404 rather than 410 or 403 on purpose: a leaked URL must not be usable to confirm that it was once live. The lookup runs before any credential is checked, so anything other than 404 would answer that question for an unauthenticated caller. Error bodies carry no internals; diagnostics live on the admin listener.

`GET /health` on the public listener answers 200 while at least one instance is serving and 503 otherwise, which is what a load balancer should watch.

HMAC signatures are validated over the raw request body exactly as received, never over a re-serialized form, and compared in constant time.

The supported shape is a hex digest of the body behind a fixed prefix, which covers GitHub (`X-Hub-Signature-256: sha256=<hex>`) and most generic partner webhooks. Set `hmac_prefix = ""` for a bare hex signature.

Providers that sign a composed string rather than the body alone are **not** supported in v1. Stripe signs `{timestamp}.{body}` and packs it into `Stripe-Signature: t=...,v1=...`; Twilio signs the URL plus sorted form parameters as base64. For those, use `bearer` or an endpoint with `auth_type = "none"`, forward the signature header, and verify it downstream.

## Message headers

With `include_http_metadata = true` each message carries:

| Header | Value |
| ------ | ----- |
| `iggy_source_instance` | `instance_name`, or the connector id if unset |
| `iggy_http_remote_addr` | Peer IP address |
| `iggy_http_received_at` | Accept time, microseconds since the Unix epoch |

Anything listed in `forward_headers` is copied alongside, under its own name. Iggy rejects header values over 255 bytes, and real values such as `User-Agent` routinely exceed that, so forwarded values are truncated to fit rather than failing the message. Truncations and drops are counted in `http_source_headers_clamped_total` and `http_source_headers_dropped_total`.

Forwarding the provider's delivery id is the recommended default, because it is the dedup key a consumer needs to close the duplicate window described above.

## Sharing one listener

The first instance to open binds both ports. Later instances validate their configuration against the running listener and join it. `admin_listen_addr`, `max_body_size_bytes`, and `management_token` must agree; a mismatch fails that instance's `open()` with a message naming the field, rather than silently handing it a listener its configuration does not describe.

`listen_addr` is not validated that way, because it is what groups instances onto a listener in the first place. Two spellings of the same socket, `0.0.0.0:9090` and `127.0.0.1:9090`, are two groups, so the second instance tries to bind a port the first already holds and fails with an address-in-use error naming that field. Instances meant to share a listener must spell `listen_addr` identically.

`instance_name` must also be unique on the listener, since it is how the management API addresses an instance and how every metric series is labelled. Instances that leave it unset get their distinct numeric plugin id, so they cannot collide.

Closing an instance deregisters its routes immediately, so its paths answer 404 while its siblings keep serving. The last instance to close shuts both listeners down gracefully and releases the ports, which the runtime's stop-then-start restart flow depends on.

The registry is per process. Several runtime processes behind a load balancer will not converge on dynamically registered endpoints; that is a fleet-coordination problem left to a future version. Single-process, multi-instance deployments are consistent by construction.

## Dynamic endpoint management

Set `management_token` to enable `/admin/endpoints` on the admin listener. Without it the API is not mounted at all and every path under it answers 404. Every call needs `Authorization: Bearer <management_token>`.

```text
POST   /admin/endpoints          register, id generated server-side  -> 201
GET    /admin/endpoints          list every endpoint, secrets omitted -> 200
GET    /admin/endpoints/{id}     one endpoint                         -> 200
PATCH  /admin/endpoints/{id}     rotate auth_secret in place          -> 200
DELETE /admin/endpoints/{id}     revoke                               -> 204
```

```bash
curl -sS -X POST http://127.0.0.1:9091/admin/endpoints \
  -H 'Authorization: Bearer replace_with_admin_token' \
  -H 'Content-Type: application/json' \
  -d '{"instance":"http_github","auth_type":"hmac-sha256","auth_secret":"whsec_new"}'
# {"endpoint_id":"9f2c...","path":"/e/9f2c..."}
```

Rotation deliberately keeps the path: a webhook sender configures the URL once, so changing the shared secret must not force it to be reconfigured. It applies to dynamic endpoints only — a static endpoint's secret lives in TOML, so rotating one answers 409 and points you at the file, because restoring prefers TOML and would silently revert the change on the next restart.

| Status | Condition |
| ------ | --------- |
| 400 | Empty `auth_secret`, or an `expires_at` already in the past |
| 401 | Missing or wrong `management_token` |
| 404 | Unknown endpoint, unknown `instance`, or the API is not configured |
| 409 | Rotating a static endpoint, or a generated id collision |
| 500 | The route table could not be rebuilt |
| 503 | The owning instance closed while the request was in flight |

Revocation writes a tombstone rather than deleting the entry. The tombstone persists, so a restart against a stale TOML file cannot resurrect an endpoint someone revoked. That rests on `open()` failing when the state file cannot be decoded, rather than falling back to the TOML: the connector reports the decode failure as `last_error` and serves nothing, instead of quietly putting revoked endpoints back on the wire.

The 204 means the endpoint stopped serving *now*, in memory. Durability follows the same path as registration below, with one asymmetry worth knowing: losing an unsaved registration fails closed, but losing an unsaved revocation fails **open**, so the endpoint would come back after a restart.

`submitted` on `GET /admin/endpoints/{id}` is not the durability check. It flips when the mutation is handed to the runtime, which happens before the runtime persists it. A save that failed shows up as `last_error` on the connector and re-arms the flush for the next poll, so treat a revocation as final only once the connector is error-free. If it cannot reach Iggy, remove the endpoint from the TOML too.

Dynamic endpoints ride the SDK's `ConnectorState`, which the runtime writes after the next successful send. A management response therefore means "accepted", not "durable": if Iggy is unreachable, the endpoint is live in memory but not yet on disk, and a crash in that window loses it.

`GET /admin/endpoints` reports `submitted` per endpoint. It is named that, and not `persisted`, on purpose: the flag is set when the registry is handed to the runtime, so `submitted: true` means it reached the runtime, not that the write landed.

Since #3855 the runtime does acknowledge the batch, and a failed save comes back as a NACK that re-arms the flush for the next poll. That re-arm does not clear `submitted`, so between a failed save and the retry that succeeds, the flag over-reports. Clearing it would mean rewriting the registry from a sync path that cannot take the writer lock, and would risk discarding a revocation that landed in the meantime. Watch the connector's `last_error` for save failures rather than this flag.

## Backpressure

The chain below is the **target** behaviour and is not yet complete: it needs the bounded runtime forwarding channel from #3795.

The bridge is bounded today, so 429 does fire on an arrival burst the poll loop cannot keep up with. What is missing is the coupling: until #3795 lands, `poll()` drains into an unbounded runtime channel, so a slow Iggy does not propagate back into 429 and shows up as memory growth instead.

```text
Iggy slow -> forwarding loop blocks -> bounded channel fills -> poll() stalls
          -> instance bridge fills -> HTTP 429 + Retry-After: 1
```

The handler never blocks on a full bridge. Waiting would hold connections open and, once the sender times out, produce a retry storm; a fast 429 tells the sender exactly what to do.

| Traffic | `buffer_capacity` | Rationale |
| ------- | ----------------- | --------- |
| Under 100 req/s | 1000 | Small footprint |
| 100 to 1000 req/s | 10000 (default) | Absorbs roughly ten seconds of burst |
| Over 1000 req/s | 50000 to 100000 | Sustained bursts; tune against the buffer metrics |

The bridge is bounded by message count, not bytes, so worst-case memory is `buffer_capacity * max_body_size_bytes`. At the defaults that is about 10 GB. Size the two together.

## Observability

`GET /admin/health` returns per-instance JSON: queue depth and capacity, serving endpoint counts by origin plus expired and revoked counts, `state_submitted`, and header loss counters.

`GET /admin/metrics` returns Prometheus text format. The runtime's own stage histograms begin at `poll()`, so they cannot see accept-to-200 latency; these fill that gap.

| Metric | Type | Labels | Notes |
| ------ | ---- | ------ | ----- |
| `http_source_requests_total` | counter | `instance`, `kind`, `status` | |
| `http_source_request_duration_seconds` | histogram | `instance`, `status` | |
| `http_source_rejected_full_total` | counter | `instance` | |
| `http_source_dropped_on_close_total` | counter | `instance` | |
| `http_source_headers_clamped_total` | counter | `instance` | |
| `http_source_headers_dropped_total` | counter | `instance` | |
| `http_source_buffer_used` | gauge | `instance` | |
| `http_source_buffer_capacity` | gauge | `instance` | |
| `http_source_endpoints_active` | gauge | `instance`, `kind` | endpoints that would accept a request now: neither revoked nor past `expires_at` |

`kind` is `named` or `secret` for requests and `static` or `dynamic` for endpoints. `status` is the response class, `2xx`, `4xx`, or `5xx`, rather than the exact code, so a caller cannot inflate cardinality by probing. Only requests to a genuinely unknown path are counted under `instance="unrouted"`, which is where a scan for live endpoint ids shows up; a revoked or expired endpoint is still metered against the instance that owns it.

A metric with no series yet is absent from the scrape rather than reported as zero, which is how Prometheus client libraries represent labelled families. Write dashboard queries accordingly.

## Operational notes

**Protect the state directory.** Once anything writes state, the file holds every endpoint's secret in the clear, static and dynamic alike, in the runtime's state path. That is the same at-rest posture as the TOML those endpoints would otherwise live in, but it means the state directory needs `chmod 700` and the same handling as any credential store.

**Revocation tombstones accumulate.** They are retained deliberately, so a revocation survives a restart and stays auditable, and nothing evicts them.

Each is roughly a hundred bytes and the whole registry is rewritten on every mutation, so a deployment that churns endpoints continuously will see the state file grow over time. An instance whose endpoints are all static writes no state file until something mutates its registry. Revoking a static endpoint through the management API does exactly that, and the tombstone it writes is what stops the TOML entry coming back.

**`dropped_on_close` disappears with its listener.** The counter lives on the shared listener's registry, so it survives one instance of several leaving — but when the *last* instance closes, the listener and its metrics go with it. In a single-instance deployment the `warn!` log line is the only surviving record of messages lost at shutdown.

**Sampled gauges are dropped when an instance leaves**, so `buffer_used` and `endpoints_active` do not linger at a stale value for an instance that no longer exists. The counters persist, as counters should.

**Put a reverse proxy in front of the public listener.** It sets no header-read timeout, no idle timeout, and no connection cap, so a client that opens a socket and stops writing holds a task and a file descriptor indefinitely. The descriptor limit is shared with the rest of the runtime process, so exhaustion is not contained to this connector.

**Keep the admin listener private.** It defaults to loopback. The management API is token-guarded, but health and metrics are not, and they expose instance names and traffic volumes.

## Limitations

- Best-effort delivery in both directions, as described above.
- Endpoints registered through the management API do not converge across runtime processes.
- The bridge is bounded by message count, not by bytes.
- `POST` only. There is no support for provider handshakes that require answering a `GET` challenge.
- HMAC validation covers hex-digest-of-body schemes. Composed-string schemes such as Stripe's and Twilio's need downstream verification.
