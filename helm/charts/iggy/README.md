# iggy

A Helm chart for [Apache Iggy](https://github.com/apache/iggy) server and web-ui.

## Prerequisites

* Kubernetes 1.19+
* Helm 3.2.0+
* PV provisioner support in the underlying infrastructure (if persistence is enabled)

### io_uring Requirements

Iggy server uses `io_uring` for high-performance async I/O. This requires:

1. **IPC_LOCK capability** - For locking memory required by io_uring
2. **Unconfined seccomp profile** - To allow io_uring syscalls

These are configured by default in the chart's `securityContext` and `podSecurityContext`.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/apache/iggy.git
cd iggy

# Install with persistence enabled
helm install iggy ./helm/charts/iggy \
  --set server.persistence.enabled=true \
  --set server.serviceMonitor.enabled=false

# Install with custom root credentials
helm install iggy ./helm/charts/iggy \
  --set server.persistence.enabled=true \
  --set server.serviceMonitor.enabled=false \
  --set server.users.root.username=admin \
  --set server.users.root.password=secretpassword
```

> **Note:** Set `server.serviceMonitor.enabled=false` if Prometheus Operator is not installed.

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

## Uninstallation

```bash
helm uninstall iggy
```

## Configuration

### Server Configuration

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `server.enabled` | bool | `true` | Enable the Iggy server deployment |
| `server.replicaCount` | int | `1` | Number of server replicas |
| `server.image.repository` | string | `"apache/iggy"` | Server image repository |
| `server.image.tag` | string | `""` | Server image tag (defaults to chart appVersion) |
| `server.ports.http` | int | `3000` | HTTP API port |
| `server.ports.tcp` | int | `8090` | TCP protocol port |
| `server.ports.quic` | int | `8080` | QUIC protocol port |

### Persistence Configuration

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `server.persistence.enabled` | bool | `false` | Enable persistence using PVC |
| `server.persistence.size` | string | `"8Gi"` | PVC storage size |
| `server.persistence.storageClass` | string | `""` | Storage class (empty = default) |
| `server.persistence.accessMode` | string | `"ReadWriteOnce"` | PVC access mode |
| `server.persistence.existingClaim` | string | `""` | Use existing PVC |

### Security Configuration

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `server.users.root.username` | string | `"iggy"` | Root user username |
| `server.users.root.password` | string | `"changeit"` | Root user password |
| `server.users.root.createSecret` | bool | `true` | Create secret for root user |
| `server.users.root.existingSecret.name` | string | `""` | Use existing secret |
| `securityContext.capabilities.add` | list | `["IPC_LOCK"]` | Container capabilities (required for io_uring) |
| `podSecurityContext.seccompProfile.type` | string | `"Unconfined"` | Seccomp profile (required for io_uring) |

### Monitoring Configuration

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `server.serviceMonitor.enabled` | bool | `true` | Enable ServiceMonitor for Prometheus Operator |
| `server.serviceMonitor.interval` | string | `"30s"` | Scrape interval |
| `server.serviceMonitor.path` | string | `"/metrics"` | Metrics endpoint path |

### Web UI Configuration

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `ui.enabled` | bool | `true` | Enable the Web UI deployment |
| `ui.replicaCount` | int | `1` | Number of UI replicas |
| `ui.image.repository` | string | `"apache/iggy-web-ui"` | UI image repository |
| `ui.ports.http` | int | `3050` | UI HTTP port |
| `ui.server.endpoint` | string | `""` | Iggy server endpoint (auto-detected if empty) |

## Troubleshooting

### Pod CrashLoopBackOff with "Out of memory" error

If you see:

```text
Cannot create runtime: Out of memory (os error 12)
```

This means io_uring cannot lock sufficient memory. Ensure:

1. `securityContext.capabilities.add` includes `IPC_LOCK`
2. `podSecurityContext.seccompProfile.type` is `Unconfined`

These are set by default but may be overridden.

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

## Accessing the Server

### Port Forward

```bash
# HTTP API
kubectl port-forward svc/iggy 3000:3000

# Web UI
kubectl port-forward svc/iggy-ui 3050:3050
```

### Using Ingress

Enable ingress in values:

```yaml
server:
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: iggy.example.com
        paths:
          - path: /
            pathType: Prefix
```

## Values

See [values.yaml](values.yaml) for the full list of configurable values.
