# iggy-kafka-gateway

A Helm chart for the Apache Iggy Kafka gateway, N Kafka brokers in front of an Iggy server or cluster

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.2.1-edge.1](https://img.shields.io/badge/AppVersion-0.2.1--edge.1-informational?style=flat-square)

The chart runs the gateways as a StatefulSet. Each gateway is one Kafka broker: node id `ordinal + 1`,
advertised under a headless Service, and listed to the others in `IGGY_KAFKA_BROKERS`. With more than
one gateway they share the consumer groups through the coordinator log in Iggy, and a gateway that
stops hands its groups to the others.

## Prerequisites

* Kubernetes 1.27+ (`unhealthyPodEvictionPolicy` of the PodDisruptionBudget)
* Helm 3.2.0+
* An Iggy server or cluster that the pods reach over TCP. The [iggy chart](../iggy) runs one.
* With more than one gateway, an Iggy user with the `manage_servers` permission, or the gateways do
  not start. The root user has it.
* Prometheus Operator CRDs if `serviceMonitor.enabled=true`

## Quick Start

```bash
helm install kafka ./helm/charts/iggy-kafka-gateway \
  --set iggy.address=iggy.default.svc.cluster.local:8090 \
  --set iggy.password=<password of the iggy user>
```

Kafka clients in the cluster bootstrap from the release's Service, for example
`kafka-iggy-kafka-gateway.default.svc.cluster.local:9093`, and then connect to each gateway by its
pod name under the headless Service.

## Probes and Metrics

Each gateway serves `/metrics`, `/ready` and `/live` on `http.port` (9096).

* `/live` answers 200 while the process answers. The liveness probe restarts a gateway that hangs.
* `/ready` answers 503 while the gateway starts, while it drains at shutdown, and from about 5 s
  after it loses Iggy. The bootstrap Service then sends no new client to it.
* `/metrics` serves Prometheus metrics: requests, latency and error codes per Kafka API, closed
  connections per cause, slot use, consumer groups and the coordinator partitions each gateway
  serves. See `gateways/kafka/docs/OBSERVABILITY.md`.

The headless Service publishes every gateway, ready or not, so a client that holds metadata reaches
a gateway that lost Iggy and gets a Kafka error code that it retries.

## Limits

* Clients connect to the per-pod DNS names, so they run inside the cluster. The chart has no
  external access per gateway yet.
* The Kafka listener is PLAINTEXT, or SASL_PLAINTEXT with `kafka.sasl.enabled`. The chart has no
  TLS listener yet.
* SASL is off by default: any pod that reaches the Kafka port can create and delete topics as
  `iggy.username`. Set `kafka.sasl.enabled=true`, and give the bridge a user without root rights
  where you can.
* One release per Iggy stream (`iggy.stream`, default `kafka`). The stream holds the coordinator
  log, and every release numbers its gateways from 1.
* The image runs as user 10001 with a read-only root file system.

## Development

```bash
# Format templates
helmfmt helm/charts/iggy-kafka-gateway/

# Lint YAML files
yamllint -c helm/charts/iggy-kafka-gateway/.yamllint.yml helm/charts/iggy-kafka-gateway/

# Regenerate README after changing values.yaml
cd helm/charts/iggy-kafka-gateway && helm-docs

# Run all validations
scripts/ci/test-helm.sh validate
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` | Labels added to every resource |
| affinity | object | `{}` | Affinity of the gateway pods |
| extraEnv | list | `[]` | Extra environment variables of the gateway container, such as `IGGY_KAFKA_FETCH_SLOTS`. See the gateway README for every `IGGY_KAFKA_*` variable. The render fails on a variable the chart sets, and on `IGGY_KAFKA_LISTENERS`, `IGGY_KAFKA_ADVERTISED_LISTENERS` and `IGGY_KAFKA_TLS_*`. |
| extraVolumeMounts | list | `[]` | Extra volume mounts of the gateway container |
| extraVolumes | list | `[]` | Extra volumes of the gateway pods |
| fullnameOverride | string | `""` | Override the full name of the resources |
| http.port | int | `9096` | Port of /metrics, /ready and /live. No authentication: keep it inside the cluster. |
| iggy.address | string | `""` | Address of the Iggy server's TCP transport, `host:port`. Required. In a cluster, any node: the gateway learns the other nodes from it and dials them when it is down. |
| iggy.existingSecret.name | string | `""` | Name of an existing Secret with the Iggy credentials. Empty creates one from `iggy.username` and `iggy.password`. |
| iggy.existingSecret.passwordKey | string | `"password"` | Key of the password in the existing Secret |
| iggy.existingSecret.usernameKey | string | `"username"` | Key of the username in the existing Secret |
| iggy.password | string | `""` | Password of `iggy.username`. Stored in a Secret this chart creates. Ignored when `iggy.existingSecret.name` is set. |
| iggy.stream | string | `""` | Default Iggy stream of Kafka topics (`IGGY_KAFKA_IGGY_STREAM`). It also holds the coordinator log. Empty uses the gateway default, `kafka`. One release per stream. |
| iggy.tls.caKey | string | `"ca.crt"` | Key of the CA certificate in `iggy.tls.caSecret` |
| iggy.tls.caSecret | string | `""` | Name of a Secret with the CA that signed Iggy's certificate. Empty trusts the system CAs. |
| iggy.tls.domain | string | `""` | Server name that Iggy's certificate must carry. Empty uses the host of `iggy.address`. |
| iggy.tls.enabled | bool | `false` | Encrypt the gateways' connections to Iggy |
| iggy.username | string | `"iggy"` | Iggy user that the gateways sign in as. With several gateways it needs the `manage_servers` permission, to read Iggy's metadata change log for the coordinator log. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"apache/iggy-kafka-gateway"` | Gateway image repository |
| image.tag | string | `""` | Gateway image tag. Empty uses the chart appVersion. Pin a version, not `edge`. |
| imagePullSecrets | list | `[]` | Secrets for pulling the image from a private registry |
| kafka.autoCreateTopics.enabled | bool | `false` | Create a missing topic when a client looks it up in Metadata (`auto.create.topics.enable`) |
| kafka.autoCreateTopics.partitions | int | `1` | Partitions of a topic created that way |
| kafka.clusterDomain | string | `"cluster.local"` | Cluster domain of the per-pod DNS names that each gateway advertises to Kafka clients |
| kafka.port | int | `9093` | Port of the Kafka listener, in the container and on the Services |
| kafka.sasl.enabled | bool | `false` | Run SASL/PLAIN on the Kafka listener (SASL_PLAINTEXT). Clients sign in with Iggy users. Off, any pod that reaches the listener can create and delete topics as `iggy.username`. Turn it on, and give the bridge a user without root rights where you can. |
| livenessProbe | object | `{"failureThreshold":3,"httpGet":{"path":"/live","port":"http"},"periodSeconds":10,"timeoutSeconds":3}` | Liveness probe on /live. It fails only when the process stops answering, not while Iggy is unreachable: a restart does not bring Iggy back. |
| nameOverride | string | `""` | Override the chart name |
| nodeSelector | object | `{}` | Node selector of the gateway pods |
| podAnnotations | object | `{}` | Annotations of the gateway pods |
| podDisruptionBudget.enabled | bool | `true` | Create a PodDisruptionBudget, so a voluntary disruption stops one gateway at a time |
| podDisruptionBudget.maxUnavailable | int | `1` | Gateways that a voluntary disruption may stop at once |
| podDisruptionBudget.unhealthyPodEvictionPolicy | string | `"AlwaysAllow"` | `AlwaysAllow` evicts a gateway that is not ready, so a node drain does not wait out an Iggy outage. Needs Kubernetes 1.27+. |
| podLabels | object | `{}` | Labels of the gateway pods |
| podSecurityContext | object | `{"fsGroup":10001,"runAsGroup":10001,"runAsNonRoot":true,"runAsUser":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod security context. The image runs as user 10001. |
| readinessProbe | object | `{"failureThreshold":2,"httpGet":{"path":"/ready","port":"http"},"periodSeconds":5,"timeoutSeconds":3}` | Readiness probe on /ready. It fails while the gateway starts or drains, and within about 5 s after the gateway loses Iggy. |
| replicaCount | int | `3` | Gateways to run. Each one is a Kafka broker with node id `ordinal + 1`, listed to the others in `IGGY_KAFKA_BROKERS`. With more than one, the gateways share the consumer groups through the coordinator log in Iggy, and fail to start unless `iggy.username` has `manage_servers`. One release per Iggy stream: two releases on one stream share the log with the same node ids. |
| resources | object | `{}` | Resource requests and limits of the gateway container |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Container security context. The gateway writes no file. |
| service.annotations | object | `{}` | Annotations of the bootstrap Service |
| service.type | string | `"ClusterIP"` | Type of the bootstrap Service that Kafka clients connect to first. Clients then connect to each gateway by the per-pod DNS name of the headless Service, so they run inside the cluster. |
| serviceAccount.annotations | object | `{}` | Annotations of the ServiceAccount |
| serviceAccount.create | bool | `true` | Create a ServiceAccount |
| serviceAccount.name | string | `""` | Name of the ServiceAccount. Empty uses the full name. |
| serviceMonitor.additionalLabels | object | `{}` | Labels added to the ServiceMonitor, such as the one your Prometheus selects |
| serviceMonitor.enabled | bool | `false` | Create a ServiceMonitor for the Prometheus Operator |
| serviceMonitor.interval | string | `"30s"` | Scrape interval. Empty uses the Prometheus default. |
| serviceMonitor.namespace | string | `""` | Namespace of the ServiceMonitor. Empty uses the release namespace. |
| serviceMonitor.scrapeTimeout | string | `"10s"` | Scrape timeout |
| terminationGracePeriodSeconds | int | `40` | Seconds that Kubernetes waits between SIGTERM and SIGKILL. The gateway hands its consumer groups to the other gateways and drains its connections for up to 25 s, then signs out of Iggy for up to 5 s. |
| tolerations | list | `[]` | Tolerations of the gateway pods |
| topologySpreadConstraints | list | `[]` | Topology spread constraints of the gateway pods, such as one gateway per node |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
