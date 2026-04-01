# iggy

A Helm chart for Apache Iggy server and web-ui

![Version: 0.5.0](https://img.shields.io/badge/Version-0.5.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.7.0](https://img.shields.io/badge/AppVersion-0.7.0-informational?style=flat-square)

## Prerequisites

* Kubernetes 1.19+
* Helm 3.2.0+
* PV provisioner support in the underlying infrastructure (if persistence is enabled)
* Prometheus Operator CRDs if `server.serviceMonitor.enabled=true`

### io_uring Requirements

Iggy server uses `io_uring` for high-performance async I/O. This requires:

1. **IPC_LOCK capability** - For locking memory required by io_uring
2. **Unconfined seccomp profile** - To allow io_uring syscalls

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

## Uninstallation

```bash
helm uninstall iggy
```

## Using Ingress

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

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` | Additional labels for all resources |
| autoscaling.enabled | bool | `false` | Enable horizontal pod autoscaling |
| autoscaling.maxReplicas | int | `100` | Maximum replicas for autoscaling |
| autoscaling.minReplicas | int | `1` | Minimum replicas for autoscaling |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Target CPU utilization for autoscaling |
| fullnameOverride | string | `""` | Override full release name |
| imagePullSecrets | list | `[]` | Image pull secrets for private registries |
| nameOverride | string | `""` | Override chart name |
| podAnnotations | object | `{}` | Pod annotations |
| podSecurityContext | object | `{"seccompProfile":{"type":"Unconfined"}}` | Pod security context (server uses io_uring, requires unconfined seccomp) |
| resources | object | `{}` | Resource limits and requests for server |
| securityContext | object | `{"capabilities":{"add":["IPC_LOCK"]}}` | Container security context (server requires IPC_LOCK for io_uring) |
| server | object | `{"affinity":{},"enabled":true,"env":[{"name":"RUST_LOG","value":"info"},{"name":"IGGY_HTTP_ADDRESS","value":"0.0.0.0:3000"},{"name":"IGGY_TCP_ADDRESS","value":"0.0.0.0:8090"},{"name":"IGGY_QUIC_ADDRESS","value":"0.0.0.0:8080"},{"name":"IGGY_WEBSOCKET_ADDRESS","value":"0.0.0.0:8092"}],"image":{"pullPolicy":"Always","repository":"apache/iggy","tag":"0.7.0"},"ingress":{"annotations":{},"className":"","enabled":false,"hosts":[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}],"tls":[]},"nodeSelector":{},"persistence":{"accessMode":"ReadWriteOnce","annotations":{},"enabled":false,"existingClaim":"","size":"8Gi","storageClass":""},"ports":{"http":3000,"quic":8080,"tcp":8090},"replicaCount":1,"service":{"port":3000,"type":"ClusterIP"},"serviceMonitor":{"additionalLabels":{},"enabled":false,"honorLabels":false,"interval":"30s","namespace":"","path":"/metrics","scrapeTimeout":"10s"},"tolerations":[],"users":{"root":{"createSecret":true,"existingSecret":{"name":"","passwordKey":"password","usernameKey":"username"},"password":"changeit","username":"iggy"}}}` | Iggy server configuration |
| server.affinity | object | `{}` | Affinity rules for server pods |
| server.enabled | bool | `true` | Enable the Iggy server deployment |
| server.env | list | `[{"name":"RUST_LOG","value":"info"},{"name":"IGGY_HTTP_ADDRESS","value":"0.0.0.0:3000"},{"name":"IGGY_TCP_ADDRESS","value":"0.0.0.0:8090"},{"name":"IGGY_QUIC_ADDRESS","value":"0.0.0.0:8080"},{"name":"IGGY_WEBSOCKET_ADDRESS","value":"0.0.0.0:8092"}]` | Environment variables for the server container |
| server.image.pullPolicy | string | `"Always"` | Image pull policy |
| server.image.repository | string | `"apache/iggy"` | Server image repository |
| server.image.tag | string | `"0.7.0"` | Server image tag (overrides chart appVersion) |
| server.ingress.annotations | object | `{}` | Ingress annotations (controller-specific) |
| server.ingress.className | string | `""` | Ingress class name (controller-neutral) |
| server.ingress.enabled | bool | `false` | Enable ingress for the server |
| server.ingress.hosts | list | `[{"host":"chart-example.local","paths":[{"path":"/","pathType":"ImplementationSpecific"}]}]` | Ingress hosts configuration |
| server.ingress.tls | list | `[]` | Ingress TLS configuration |
| server.nodeSelector | object | `{}` | Node selector for server pods |
| server.persistence.accessMode | string | `"ReadWriteOnce"` | PVC access mode |
| server.persistence.annotations | object | `{}` | PVC annotations |
| server.persistence.enabled | bool | `false` | Enable persistence using PVC |
| server.persistence.existingClaim | string | `""` | Use existing PVC (requires persistence.enabled: true) |
| server.persistence.size | string | `"8Gi"` | PVC storage size |
| server.persistence.storageClass | string | `""` | Storage class for PVC (empty uses default provisioner) |
| server.ports.http | int | `3000` | HTTP API port |
| server.ports.quic | int | `8080` | QUIC protocol port |
| server.ports.tcp | int | `8090` | TCP protocol port |
| server.replicaCount | int | `1` | Number of server replicas |
| server.service.port | int | `3000` | Service port for the server |
| server.service.type | string | `"ClusterIP"` | Service type for the server |
| server.serviceMonitor.additionalLabels | object | `{}` | Additional labels for the ServiceMonitor |
| server.serviceMonitor.enabled | bool | `false` | Enable ServiceMonitor for Prometheus Operator |
| server.serviceMonitor.honorLabels | bool | `false` | Honor labels from the target |
| server.serviceMonitor.interval | string | `"30s"` | Scrape interval (fallback to Prometheus default) |
| server.serviceMonitor.namespace | string | `""` | Namespace to deploy the ServiceMonitor |
| server.serviceMonitor.path | string | `"/metrics"` | Path to scrape metrics from |
| server.serviceMonitor.scrapeTimeout | string | `"10s"` | Timeout for scrape metrics request |
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
