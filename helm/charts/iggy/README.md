# iggy

A Helm chart for Apache Iggy server and web-ui

![Version: 0.6.0](https://img.shields.io/badge/Version-0.6.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.9.0-edge.6](https://img.shields.io/badge/AppVersion-0.9.0--edge.6-informational?style=flat-square)

## Prerequisites

* Kubernetes 1.19+
* Helm 3.2.0+
* PV provisioner support in the underlying infrastructure (if persistence is enabled)
* Prometheus Operator CRDs if `server.serviceMonitor.enabled=true`

### io_uring Requirements

Iggy server uses `io_uring` for high-performance async I/O. This requires:

1. **Linux kernel 5.19 or newer on the node**

   * Shard rings require `IORING_SETUP_COOP_TASKRUN` and `IORING_SETUP_TASKRUN_FLAG`.
   * Compio's asynchronous socket creation requires `IORING_OP_SOCKET`.
   * Mainline Linux provides these features starting in 5.19.
   * Older kernels fail during shard startup. The node kernel matters, not the container image.

2. **IPC_LOCK capability** - For locking memory required by io_uring
3. **Unconfined seccomp profile** - To allow io_uring syscalls

These are configured by default for the Iggy server via the chart's root-level
`securityContext` and `podSecurityContext`. The web UI uses `ui.securityContext`
and `ui.podSecurityContext`, which default to empty.

Some local or container-based Kubernetes environments may still fail during Iggy runtime
initialization if the node/kernel does not provide the `io_uring` support required by the
server runtime.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/apache/iggy.git
cd iggy

# Install with persistence enabled
helm install iggy ./helm/charts/iggy \
  --set server.persistence.enabled=true

# Install with custom root credentials
helm install iggy ./helm/charts/iggy \
  --set server.persistence.enabled=true \
  --set server.users.root.username=admin \
  --set server.users.root.password=secretpassword
```

> **Note:** `server.serviceMonitor.enabled` defaults to `false`.
> Enable it only if Prometheus Operator is installed and you want a `ServiceMonitor` resource.
> The server still requires node/kernel support for `io_uring`, including on clean local clusters such as `kind` or `minikube`.

## Installation

### From Git Repository

```bash
git clone https://github.com/apache/iggy.git
cd iggy
helm install iggy ./helm/charts/iggy
```

### With Persistence

```bash
helm install iggy ./helm/charts/iggy \
  --set server.persistence.enabled=true \
  --set server.persistence.size=50Gi
```

### With Custom Values File

```bash
helm install iggy ./helm/charts/iggy -f custom-values.yaml
```

If Prometheus Operator is installed and you want monitoring, set
`server.serviceMonitor.enabled=true` in `custom-values.yaml` or pass it on the
command line with `--set server.serviceMonitor.enabled=true`.

## Cluster Mode

The server runs a Viewstamped Replication cluster when `cluster.enabled` is set
and each node is told which roster entry it is. The chart models this as **one
release per node**: every release is handed the same roster and overrides only
its own identity.

`helm/charts/iggy/examples/cluster-3-node.yaml` is a ready three-node roster.
Point the `ip` values at the nodes you will pin the releases to, then:

```bash
for i in 0 1 2; do
  helm upgrade --install "iggy-n$i" ./helm/charts/iggy \
    -f ./helm/charts/iggy/examples/cluster-3-node.yaml \
    --set "server.cluster.selfReplicaId=$i" \
    --set "server.nodeSelector.kubernetes\.io/hostname=node-$i"
done
```

The chart turns `server.cluster` into the server's `IGGY_CLUSTER_*` environment
variables and passes `--replica-id`, so no configuration file is mounted. The
values mirror the server's `[[cluster.nodes]]` config one for one.

### Why hostNetwork and node pinning are required

The server's consensus listener binds `cluster.nodes[*].ip` **verbatim**, unlike
the client listeners, which keep their own bind address and take only the port
from the roster. A pod therefore has to own the address its roster entry names.
Pod IPs are neither stable nor known before install, and a Service ClusterIP is
not an address a pod can bind, so the workable layout today is `hostNetwork:
true` with each release pinned to a node and its roster `ip` set to that node's
IP. Node IPs are known up front and survive a pod restart, which is what lets a
replica come back and rejoin.

The cost is real: the server pod shares the node's network namespace and its
ports. Weigh it before running this in a shared cluster.

If a release lands on a node whose IP is not the one in its roster entry, the
server refuses to start rather than misbehaving quietly:

```text
Error: ShardJoinFailures { failures: [ShardJoinFailure { shard_id: 0,
  kind: Error(Iggy(CannotBindToSocket("10.0.1.11:9090"))) }] }
```

Check the `nodeSelector` on that release against the `ip` in its roster entry.

### Upgrades

Host ports make a rolling update impossible: the replacement pod cannot bind
ports the outgoing pod still holds, so it stays `Pending` while the Deployment
waits for it to become ready. The chart therefore switches the server to the
`Recreate` strategy whenever `hostNetwork` is set, which takes the node down
for the length of the restart. Roll **one release at a time** and let the
cluster regain quorum before starting the next.

Turning `hostNetwork` on for a release that already exists moves the Deployment
from `RollingUpdate` to `Recreate`. Under Helm 4's server-side apply that is
rejected, because the patch cannot drop the `rollingUpdate` block the API server
defaulted in:

```text
Deployment.apps "iggy-n0" is invalid: spec.strategy.rollingUpdate: Forbidden:
  may not be specified when strategy `type` is 'Recreate'
```

Run that one upgrade with `helm upgrade --server-side=false`, which replaces the
strategy in place. Set `server.strategy` explicitly if you want a different
strategy.

### Roster rules

* Every node runs the **identical** `server.cluster.nodes` list. Only
  `selfReplicaId` differs between releases.
* `replicaId` values are unique and cover `0..N-1` for an `N`-node roster.
* `ports.tcpReplica` is required on every entry. In cluster mode the server
  takes every listener port from the roster and will not fall back to defaults,
  because two nodes on one host would otherwise race for the same socket. The
  remaining ports default to `server.ports`.
* `server.cluster.name` is hashed into the on-disk cluster id on first boot.
  Changing it later makes the server refuse to start against existing data.
* `ip` must be a literal IP. Hostnames are rejected. Use
  `advertisedAddress` for the name clients dial, which does accept DNS.

### Secrets

Four settings have to carry the byte-identical value on every node, so they
belong in one Secret that all releases reference rather than in each release's
values. Create it in the release namespace before the first install:

```bash
JWT="$(head -c 32 /dev/urandom | base64)"
kubectl create secret generic iggy-cluster-secrets \
  --from-literal=username=iggy \
  --from-literal=password="$(head -c 24 /dev/urandom | base64)" \
  --from-literal=clusterSharedSecret="$(head -c 32 /dev/urandom | base64)" \
  --from-literal=jwtEncodingSecret="$JWT" \
  --from-literal=jwtDecodingSecret="$JWT" \
  --from-literal=encryptionKey="$(head -c 32 /dev/urandom | base64)"
```

`examples/cluster-3-node.yaml` points `server.users.root`, `server.encryption`,
`server.jwt` and `server.cluster.auth` at that one Secret. The last three also
accept an inline value, which the chart turns into its own Secret; that is
convenient for a single node and a poor fit for a cluster, since the value then
lives in every release's stored values.

* **`server.users.root`** seeds the root user. Every node creates it locally
  from its own `IGGY_ROOT_USERNAME` and `IGGY_ROOT_PASSWORD`, so a password that
  differs on one release logs in on that node only, and a first cluster boot
  refuses to start without both.
* **`server.cluster.auth`** makes every replica connection complete an
  authenticated handshake or be rejected. The key is at least 32 bytes of
  CSPRNG output. Turning it on or off is a coordinated restart of the whole
  cluster: a node that authenticates cannot talk to one that does not.
* **`server.encryption`** encrypts message payloads and state commands at rest
  with AES-256-GCM, under a 32-byte base64 key. A node holding a different key
  cannot read what its peers wrote.
* **`server.jwt`** makes HTTP bearer tokens valid on every node and survive a
  restart. With `cluster.auth` enabled the server already derives a cluster-wide
  key from the replica PSK, so setting the JWT secrets is an alternative to that
  rather than an addition; setting them anyway keeps tokens working if auth is
  later turned off.

None of the encryption, JWT and replica-auth values is written to the data
directory, and each is masked as `******` in the startup log.

### Storage

A replica cannot move between nodes without invalidating its roster entry, so
give each release storage that stays on its node, and size
`server.persistence` per node rather than for the cluster. A replica that starts
on an empty volume rejoins by state transfer, which is correct but re-reads the
whole dataset from its peers. `server.persistence.enabled: false` puts the
replica on `emptyDir` and forces exactly that on every pod replacement, so the
cluster example enables persistence and leaves `storageClass` for you to point
at a node-local provisioner.

### What the chart refuses

`server.replicaCount > 1` fails at render time. Scaling the server Deployment
produces N independent servers behind one Service, all writing the same PVC
subpath with no lock between them, which corrupts the data directory while
`helm --wait` still reports success. The chart ships no HorizontalPodAutoscaler
for the same reason. Cluster size is a roster decision, so add a node to
`server.cluster.nodes` and install another release instead.

## Uninstallation

```bash
helm uninstall iggy
```

## Testing

The chart CI paths are also available locally from the repository root.

### Render Validation

If `helm` is already installed locally:

```bash
scripts/ci/test-helm.sh validate
```

If you want the pinned Linux CI tool version instead:

```bash
scripts/ci/setup-helm-tools.sh
scripts/ci/test-helm.sh validate
```

This runs `helm lint --strict` plus the CI render scenarios, including:

* default chart output
* all-features render
* legacy Kubernetes 1.18 API coverage
* server-only render
* UI-only render
* existing-secret render

### Runtime Smoke Test

The smoke path requires `helm`, `kind`, `kubectl`, and `curl`.

Before running the local smoke path, keep these common gotchas in mind:

* the Iggy server requires working `io_uring` support from the Kubernetes node/kernel/runtime
* the server also needs enough available memory and locked-memory headroom during startup
* `scripts/ci/test-helm.sh cleanup-smoke` removes the Helm release and smoke namespace, but it does not delete the reusable kind cluster created by `scripts/ci/setup-helm-smoke-cluster.sh`

If `helm` and `kind` are already installed:

```bash
scripts/ci/setup-helm-smoke-cluster.sh
scripts/ci/test-helm.sh smoke --cleanup
```

If you want the pinned Linux CI tool versions:

```bash
scripts/ci/setup-helm-tools.sh --install-kind
scripts/ci/setup-helm-smoke-cluster.sh
scripts/ci/test-helm.sh smoke --cleanup
```

If a previous local smoke install failed and left resources behind, reset the smoke namespace with:

```bash
scripts/ci/test-helm.sh cleanup-smoke
```

On Apple Silicon hosts, the released `arm64` server image may still fail during the runtime smoke path in kind. If your Docker setup supports amd64 emulation well enough, you can try recreating the dedicated smoke cluster with:

```bash
HELM_SMOKE_KIND_PLATFORM=linux/amd64 scripts/ci/setup-helm-smoke-cluster.sh
```

The smoke script defaults `IGGY_SYSTEM_SHARDING_CPU_ALLOCATION=1` for the server pod so the local kind path avoids the chart's `numa:auto` default and keeps the local runtime to a single shard, which has been more reliable on containerized local nodes. If you need a different local override, set `HELM_SMOKE_SERVER_CPU_ALLOCATION` before running `scripts/ci/test-helm.sh smoke`. Pass `--cleanup` to remove the smoke namespace after a successful run; omit it if you want to inspect the deployed resources.

On smoke-test failures you can collect the same diagnostics as CI with:

```bash
scripts/ci/test-helm.sh collect-smoke-diagnostics
```

> **Note:** `scripts/ci/setup-helm-tools.sh` currently supports Linux `x86_64` only.
> On other local platforms, install equivalent `helm` and `kind` binaries yourself and then use the same scripts above.
> The runtime smoke test may still fail on some local/containerized clusters if the node/kernel does not provide the `io_uring` support required by the server runtime even after the local sharding override, or if the local environment does not provide enough memory for the server to initialize cleanly.

## Troubleshooting

### Pod CrashLoopBackOff with "Out of memory" error

If you see:

```text
Cannot create runtime: Out of memory (os error 12)
```

This means io_uring cannot lock sufficient memory. Ensure:

1. `securityContext.capabilities.add` includes `IPC_LOCK`
2. `podSecurityContext.seccompProfile.type` is `Unconfined`

These server settings are set by default but may be overridden.

### Pod CrashLoopBackOff with "Invalid argument" during server startup

If the Iggy server exits with a panic similar to:

```text
called `Result::unwrap()` on an `Err` value: Os { code: 22, kind: InvalidInput, message: "Invalid argument" }
```

the Kubernetes node may not support the `io_uring` runtime configuration required by the server.
This has been observed on local/container-based clusters even when `IPC_LOCK` and
`podSecurityContext.seccompProfile.type=Unconfined` are set.

### ServiceMonitor CRD not found

If you see:

```text
no matches for kind "ServiceMonitor" in version "monitoring.coreos.com/v1"
```

Either install Prometheus Operator or disable ServiceMonitor:

```bash
helm install iggy ./helm/charts/iggy --set server.serviceMonitor.enabled=false
```

### Server not accessible from other pods

Ensure the server binds to `0.0.0.0` instead of `127.0.0.1`. This is configured by default via environment variables:

* `IGGY_HTTP_ADDRESS=0.0.0.0:3000`
* `IGGY_TCP_ADDRESS=0.0.0.0:8090`
* `IGGY_QUIC_ADDRESS=0.0.0.0:8080`

A wildcard bind says which interfaces accept connections, not where clients
reach the pod, so the server also needs the address to publish in cluster
metadata. Server builds that carry the setting refuse to start without it.
`0.9.0-edge.6`, the image this chart pins, predates it:
that build logs `IGGY_NODE_ADVERTISED_ADDRESS` as an unknown variable and
publishes the bind address, so the chart's default stays inert until the pinned
image moves past it. Either way the chart sets `IGGY_NODE_ADVERTISED_ADDRESS` to
the in-cluster Service DNS name; override it with `server.advertisedAddress`
when clients arrive through a LoadBalancer or an Ingress.

In cluster mode the server ignores the variable altogether and publishes each
node's roster `ip`, or its `advertisedAddress` when the entry carries one, so
the chart leaves the variable out there and refuses a render that sets
`server.advertisedAddress` alongside `server.cluster.enabled`.

Declaring `IGGY_NODE_ADVERTISED_ADDRESS` in `server.env` yourself works too:
the chart then leaves its own default out, so the variable is declared once.
Give that entry a non-empty value; an empty one suppresses the chart default
and reaches the server as unset, so the render fails instead.
Setting it in `server.env` and in `server.advertisedAddress` at the same time
is refused at render time rather than resolved silently, since only the
`server.env` entry would take effect.

## Accessing the Server

### Port Forward

```bash
# HTTP API
kubectl port-forward svc/iggy 3000:3000

# Web UI
kubectl port-forward svc/iggy-ui 3050:3050
```

### Using Ingress

Enable ingress in values. Set `className` and any controller-specific annotations to match your
ingress implementation:

```yaml
server:
  ingress:
    enabled: true
    className: "<your-ingress-class>"
    annotations: {}
    hosts:
      - host: iggy.example.com
        paths:
          - path: /
            pathType: Prefix
    tls: []

ui:
  ingress:
    enabled: true
    className: "<your-ingress-class>"
    annotations: {}
    hosts:
      - host: iggy-ui.example.com
        paths:
          - path: /
            pathType: Prefix
    tls: []
```

The chart is controller-neutral and works with any Ingress controller (nginx, Traefik, HAProxy, Contour, etc.).

## Development

### Formatting and Linting

This chart uses automated tools to maintain code quality:

| Tool | Purpose | Files |
|------|---------|-------|
| [helm-docs](https://github.com/norwoodj/helm-docs) | Auto-generate this README | `values.yaml` → `README.md` |
| [yamllint](https://github.com/adrienverge/yamllint) | YAML formatting | `values.yaml`, `Chart.yaml` |
| [helmfmt](https://github.com/digitalstudium/helmfmt) | Helm template formatting | `templates/*.yaml`, `*.tpl` |

### Local Development

```bash
# Install tools (macOS)
brew install norwoodj/tap/helm-docs
pip install "yamllint==1.38.0"
go install github.com/digitalstudium/helmfmt@latest

# Format templates
helmfmt helm/charts/iggy/

# Lint YAML files
yamllint -c helm/charts/iggy/.yamllint.yml helm/charts/iggy/

# Regenerate README after changing values.yaml
cd helm/charts/iggy && helm-docs

# Run all validations
scripts/ci/test-helm.sh validate
```

### Pre-commit Hooks

These tools are integrated with pre-commit. Install hooks with:

```bash
pre-commit install
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` | Additional labels for all resources |
| fullnameOverride | string | `""` | Override full release name |
| imagePullSecrets | list | `[]` | Image pull secrets for private registries |
| nameOverride | string | `""` | Override chart name |
| podAnnotations | object | `{}` | Pod annotations |
| podSecurityContext | object | `{"seccompProfile":{"type":"Unconfined"}}` | Pod security context (server uses io_uring, requires unconfined seccomp) |
| resources | object | `{}` | Resource limits and requests for server |
| securityContext | object | `{"capabilities":{"add":["IPC_LOCK"]}}` | Container security context (server requires IPC_LOCK for io_uring) |
| server | object | `{"advertisedAddress":"","affinity":{},"cluster":{"auth":{"enabled":false,"existingSecret":{"name":"","previousSharedSecretKey":"clusterPreviousSharedSecret","sharedSecretKey":"clusterSharedSecret"},"previousSharedSecret":"","sharedSecret":""},"enabled":false,"name":"iggy-cluster","nodes":[],"requireHostNetwork":true,"selfReplicaId":0},"enabled":true,"encryption":{"enabled":false,"existingSecret":{"key":"encryptionKey","name":""},"key":""},"env":[{"name":"RUST_LOG","value":"info"},{"name":"IGGY_HTTP_ADDRESS","value":"0.0.0.0:3000"},{"name":"IGGY_TCP_ADDRESS","value":"0.0.0.0:8090"},{"name":"IGGY_QUIC_ADDRESS","value":"0.0.0.0:8080"},{"name":"IGGY_WEBSOCKET_ADDRESS","value":"0.0.0.0:8092"}],"extraArgs":[],"hostNetwork":false,"image":{"pullPolicy":"Always","repository":"apache/iggy","tag":""},"ingress":{"annotations":{},"className":"","enabled":false,"hosts":[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}],"tls":[]},"jwt":{"decodingSecret":"","encodingSecret":"","existingSecret":{"decodingSecretKey":"jwtDecodingSecret","encodingSecretKey":"jwtEncodingSecret","name":""}},"nodeSelector":{},"persistence":{"accessMode":"ReadWriteOnce","annotations":{},"enabled":false,"existingClaim":"","size":"8Gi","storageClass":""},"ports":{"http":3000,"quic":8080,"tcp":8090,"tcpReplica":9090,"websocket":8092},"replicaCount":1,"service":{"port":3000,"type":"ClusterIP"},"serviceMonitor":{"additionalLabels":{},"authorization":{},"enabled":false,"honorLabels":false,"interval":"30s","namespace":"","path":"/metrics","scrapeTimeout":"10s"},"strategy":{},"tolerations":[],"users":{"root":{"createSecret":true,"existingSecret":{"name":"","passwordKey":"password","usernameKey":"username"},"password":"changeit","username":"iggy"}}}` | Iggy server configuration |
| server.advertisedAddress | string | `""` | Client-facing address published in cluster metadata. Declaring `IGGY_NODE_ADVERTISED_ADDRESS` in `server.env` instead also works, but setting both is refused at render time. Empty falls back to the in-cluster Service DNS name. Ignored in cluster mode, where the address comes from the node's roster entry, so setting both is refused there too. |
| server.affinity | object | `{}` | Affinity rules for server pods |
| server.cluster.auth | object | `{"enabled":false,"existingSecret":{"name":"","previousSharedSecretKey":"clusterPreviousSharedSecret","sharedSecretKey":"clusterSharedSecret"},"previousSharedSecret":"","sharedSecret":""}` | Replica-to-replica authentication on the consensus port. When enabled every peer must complete an authenticated handshake or be rejected, and a shared secret becomes mandatory. Enabling it on a running cluster is a coordinated-restart change, not a rolling one. |
| server.cluster.auth.enabled | bool | `false` | Require the authenticated replica handshake |
| server.cluster.auth.existingSecret.name | string | `""` | Name of an existing Secret holding the pre-shared keys |
| server.cluster.auth.existingSecret.previousSharedSecretKey | string | `"clusterPreviousSharedSecret"` | Key inside that Secret holding the retiring shared secret |
| server.cluster.auth.existingSecret.sharedSecretKey | string | `"clusterSharedSecret"` | Key inside that Secret holding the active shared secret |
| server.cluster.auth.previousSharedSecret | string | `""` | Retiring key, accepted for verification only while a rotation is in flight. Leave empty outside a rotation. |
| server.cluster.auth.sharedSecret | string | `""` | Cluster-wide pre-shared key, at least 32 bytes of CSPRNG output, byte-identical on every node. Ignored when `existingSecret.name` is set. |
| server.cluster.enabled | bool | `false` | Enable cluster (VSR consensus) mode. One Helm release per node: every release shares the same `nodes` roster and overrides only `selfReplicaId`. See the Cluster Mode section of the chart README. |
| server.cluster.name | string | `"iggy-cluster"` | Cluster name, byte-identical on every node. Hashed into the on-disk cluster id on first boot, so changing it later means starting from an empty data directory. |
| server.cluster.nodes | list | `[]` | Cluster roster, mirroring the server's `[[cluster.nodes]]` config. Every node runs the identical list. `ip` is the replica-plane address: this node's consensus listener binds it verbatim and every peer dials it verbatim, so it must be a literal IP that the pod itself owns. With `server.hostNetwork` that is the node IP. `ports.tcpReplica` is required on every entry; the remaining ports default to `server.ports`. |
| server.cluster.requireHostNetwork | bool | `true` | Refuse to render a cluster node without `server.hostNetwork`. The replica listener binds the roster `ip` verbatim, which no pod owns on the cluster network, so the pod would die at boot with `CannotBindToSocket`. Set this to false only when the roster `ip` is an address the pod itself holds. |
| server.cluster.selfReplicaId | int | `0` | Which `nodes` entry this release runs, matched against `replicaId`. |
| server.enabled | bool | `true` | Enable the Iggy server deployment |
| server.encryption | object | `{"enabled":false,"existingSecret":{"key":"encryptionKey","name":""},"key":""}` | Server-side encryption of message payloads and state commands, using AES-256-GCM. Every node of a cluster must hold the identical key, or it cannot read data another node wrote. |
| server.encryption.enabled | bool | `false` | Enable encryption at rest |
| server.encryption.existingSecret.key | string | `"encryptionKey"` | Key inside that Secret |
| server.encryption.existingSecret.name | string | `""` | Name of an existing Secret holding the encryption key |
| server.encryption.key | string | `""` | 32-byte key, base64 encoded. Ignored when `existingSecret.name` is set. Prefer `existingSecret` outside development: a value here is stored in the Helm release and readable by anyone who can read it. |
| server.env | list | `[{"name":"RUST_LOG","value":"info"},{"name":"IGGY_HTTP_ADDRESS","value":"0.0.0.0:3000"},{"name":"IGGY_TCP_ADDRESS","value":"0.0.0.0:8090"},{"name":"IGGY_QUIC_ADDRESS","value":"0.0.0.0:8080"},{"name":"IGGY_WEBSOCKET_ADDRESS","value":"0.0.0.0:8092"}]` | Environment variables for the server container |
| server.extraArgs | list | `[]` | Extra command-line arguments appended to the server entrypoint, e.g. `["--with-default-root-credentials"]` for a throwaway development install. `--replica-id` is not one of them: the chart passes it already whenever `cluster.enabled` is set. |
| server.hostNetwork | bool | `false` | Run the server pod in the host network namespace. Required for cluster mode, where the replica listener binds the roster IP verbatim. |
| server.image.pullPolicy | string | `"Always"` | Image pull policy |
| server.image.repository | string | `"apache/iggy"` | Server image repository |
| server.image.tag | string | `""` | Server image tag. Empty uses the chart appVersion. |
| server.ingress.annotations | object | `{}` | Ingress annotations (controller-specific) |
| server.ingress.className | string | `""` | Ingress class name (controller-neutral) |
| server.ingress.enabled | bool | `false` | Enable ingress for the server |
| server.ingress.hosts | list | `[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}]` | Ingress hosts configuration |
| server.ingress.tls | list | `[]` | Ingress TLS configuration |
| server.jwt | object | `{"decodingSecret":"","encodingSecret":"","existingSecret":{"decodingSecretKey":"jwtDecodingSecret","encodingSecretKey":"jwtEncodingSecret","name":""}}` | Secrets used to sign and validate HTTP bearer tokens. Left unset, each node generates a random secret on every start, which invalidates tokens across restarts and keeps them node-local. Setting the identical secret on every node makes bearers valid cluster-wide and activates follower to primary HTTP forwarding; `cluster.auth.enabled` derives the same thing from the replica PSK, so it is an alternative rather than an addition. |
| server.jwt.decodingSecret | string | `""` | Decoding secret. Ignored when `existingSecret.name` is set. |
| server.jwt.encodingSecret | string | `""` | Encoding secret. Ignored when `existingSecret.name` is set. |
| server.jwt.existingSecret.decodingSecretKey | string | `"jwtDecodingSecret"` | Key inside that Secret holding the decoding secret |
| server.jwt.existingSecret.encodingSecretKey | string | `"jwtEncodingSecret"` | Key inside that Secret holding the encoding secret |
| server.jwt.existingSecret.name | string | `""` | Name of an existing Secret holding the JWT secrets |
| server.nodeSelector | object | `{}` | Node selector for server pods |
| server.persistence.accessMode | string | `"ReadWriteOnce"` | PVC access mode |
| server.persistence.annotations | object | `{}` | PVC annotations |
| server.persistence.enabled | bool | `false` | Enable persistence using PVC |
| server.persistence.existingClaim | string | `""` | Use existing PVC (requires persistence.enabled: true) |
| server.persistence.size | string | `"8Gi"` | PVC storage size |
| server.persistence.storageClass | string | `""` | Storage class for PVC (empty uses default provisioner) |
| server.ports.http | int | `3000` | HTTP API port |
| server.ports.quic | int | `8080` | QUIC protocol port (UDP) |
| server.ports.tcp | int | `8090` | TCP protocol port |
| server.ports.tcpReplica | int | `9090` | Replica-to-replica consensus port. Only published when `server.cluster.enabled` is true. |
| server.ports.websocket | int | `8092` | WebSocket protocol port |
| server.replicaCount | int | `1` | Number of server replicas |
| server.service.port | int | `3000` | Service port for the server |
| server.service.type | string | `"ClusterIP"` | Service type for the server |
| server.serviceMonitor.additionalLabels | object | `{}` | Additional labels for the ServiceMonitor |
| server.serviceMonitor.authorization | object | `{}` | Authorization for the scrape request. The metrics endpoint requires a bearer credential (e.g. a personal access token stored in a Secret): authorization:   credentials:     name: iggy-metrics-token     key: token |
| server.serviceMonitor.enabled | bool | `false` | Enable ServiceMonitor for Prometheus Operator |
| server.serviceMonitor.honorLabels | bool | `false` | Honor labels from the target |
| server.serviceMonitor.interval | string | `"30s"` | Scrape interval (fallback to Prometheus default) |
| server.serviceMonitor.namespace | string | `""` | Namespace to deploy the ServiceMonitor |
| server.serviceMonitor.path | string | `"/metrics"` | Path to scrape metrics from |
| server.serviceMonitor.scrapeTimeout | string | `"10s"` | Timeout for scrape metrics request |
| server.strategy | object | `{}` | Deployment update strategy. Empty lets the chart choose: `Recreate` when `hostNetwork` is set, because a rolling update would wait forever for a replacement pod that cannot bind host ports the outgoing pod still holds, and the Kubernetes default otherwise. |
| server.tolerations | list | `[]` | Tolerations for server pods |
| server.users.root.createSecret | bool | `true` | Create a secret for the root user credentials |
| server.users.root.existingSecret.name | string | `""` | Name of existing secret for root credentials |
| server.users.root.existingSecret.passwordKey | string | `"password"` | Key in secret for password |
| server.users.root.existingSecret.usernameKey | string | `"username"` | Key in secret for username |
| server.users.root.password | string | `"changeit"` | Root password |
| server.users.root.username | string | `"iggy"` | Root username |
| serviceAccount.annotations | object | `{}` | Service account annotations |
| serviceAccount.create | bool | `true` | Create a service account |
| serviceAccount.name | string | `""` | Service account name (generated if not set) |
| ui | object | `{"affinity":{},"enabled":true,"env":{},"image":{"pullPolicy":"Always","repository":"apache/iggy-web-ui","tag":"edge"},"ingress":{"annotations":{},"className":"","enabled":false,"hosts":[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}],"tls":[]},"nodeSelector":{},"podSecurityContext":{},"ports":{"http":3050},"replicaCount":1,"resources":{},"securityContext":{},"server":{"endpoint":""},"service":{"port":3050,"type":"ClusterIP"},"tolerations":[]}` | Iggy web UI configuration |
| ui.affinity | object | `{}` | Affinity rules for UI pods |
| ui.enabled | bool | `true` | Enable the web UI deployment |
| ui.env | object | `{}` | Extra environment variables for UI container |
| ui.image.pullPolicy | string | `"Always"` | UI image pull policy |
| ui.image.repository | string | `"apache/iggy-web-ui"` | UI image repository |
| ui.image.tag | string | `"edge"` | UI image tag (overrides chart appVersion) |
| ui.ingress.annotations | object | `{}` | Ingress annotations (controller-specific) |
| ui.ingress.className | string | `""` | Ingress class name (controller-neutral) |
| ui.ingress.enabled | bool | `false` | Enable ingress for the UI |
| ui.ingress.hosts | list | `[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}]` | Ingress hosts configuration |
| ui.ingress.tls | list | `[]` | Ingress TLS configuration |
| ui.nodeSelector | object | `{}` | Node selector for UI pods |
| ui.podSecurityContext | object | `{}` | Pod security context for UI pods |
| ui.ports.http | int | `3050` | HTTP port for web UI |
| ui.replicaCount | int | `1` | Number of UI replicas |
| ui.resources | object | `{}` | Resource limits and requests for UI |
| ui.securityContext | object | `{}` | Container security context for UI |
| ui.server.endpoint | string | `""` | Iggy server endpoint (blank uses service URL) |
| ui.service.port | int | `3050` | Service port for the UI |
| ui.service.type | string | `"ClusterIP"` | Service type for the UI |
| ui.tolerations | list | `[]` | Tolerations for UI pods |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
