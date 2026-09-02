{{/*
Expand the name of the chart.
*/}}
{{- define "iggy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "iggy.fullname" -}}
  {{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
  {{- else }}
    {{- $name := default .Chart.Name .Values.nameOverride }}
    {{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
    {{- else }}
      {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "iggy.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "iggy.labels" -}}
helm.sh/chart: {{ include "iggy.chart" . }}
{{ include "iggy.selectorLabels" . }}
  {{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
  {{- end }}
app.kubernetes.io/component: server
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: iggy-server
  {{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
  {{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "iggy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "iggy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "iggy-ui.chart" -}}
  {{- printf "%s-ui-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "iggy-ui.labels" -}}
helm.sh/chart: {{ include "iggy-ui.chart" . }}
{{ include "iggy-ui.selectorLabels" . }}
  {{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
  {{- end }}
app.kubernetes.io/component: server
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: iggy-server
  {{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
  {{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "iggy-ui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "iggy.name" . }}-ui
app.kubernetes.io/instance: {{ .Release.Name }}-ui
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "iggy.serviceAccountName" -}}
  {{- if .Values.serviceAccount.create }}
{{- default (include "iggy.fullname" .) .Values.serviceAccount.name }}
  {{- else }}
{{- default "default" .Values.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Validate the cluster roster and fail the render with an actionable message.
Every check here is one the server would otherwise only reject at boot, after
the pod is already scheduled.
*/}}
{{- define "iggy.validateCluster" -}}
  {{- $cluster := .Values.server.cluster }}
  {{- if not $cluster.nodes }}
    {{- fail "server.cluster.enabled is true but server.cluster.nodes is empty. Every node runs the identical roster; see the Cluster Mode section of the chart README." }}
  {{- end }}
  {{- $count := len $cluster.nodes }}
  {{- $seen := dict }}
  {{- range $cluster.nodes }}
    {{- if not .name }}
      {{- fail "every server.cluster.nodes entry needs a name" }}
    {{- end }}
    {{- if not .ip }}
      {{- fail (printf "server.cluster.nodes entry %q has no ip. The replica listener binds this address verbatim, so it must be a literal IP the pod owns." .name) }}
    {{- end }}
    {{- if kindIs "invalid" .replicaId }}
      {{- fail (printf "server.cluster.nodes entry %q has no replicaId" .name) }}
    {{- end }}
    {{- $id := int .replicaId }}
    {{- if or (lt $id 0) (ge $id $count) }}
      {{- fail (printf "server.cluster.nodes entry %q has replicaId %d, which is outside 0..%d for a %d-node roster" .name $id (sub $count 1) $count) }}
    {{- end }}
    {{- if hasKey $seen (printf "%d" $id) }}
      {{- fail (printf "server.cluster.nodes has two entries with replicaId %d; ids must be unique" $id) }}
    {{- end }}
    {{- $_ := set $seen (printf "%d" $id) .name }}
    {{- if not (and .ports .ports.tcpReplica) }}
      {{- fail (printf "server.cluster.nodes entry %q has no ports.tcpReplica. In cluster mode the server takes every listener port from the roster and refuses to start without it." .name) }}
    {{- end }}
  {{- end }}
  {{- if not (hasKey $seen (printf "%d" (int $cluster.selfReplicaId))) }}
    {{- fail (printf "server.cluster.selfReplicaId is %d but no server.cluster.nodes entry declares that replicaId. Each release picks its own identity out of the shared roster." (int $cluster.selfReplicaId)) }}
  {{- end }}
{{- end }}

{{/*
Render the roster as IGGY_CLUSTER_* environment variables. The server accepts
the whole cluster config this way, so the chart needs no config file mount.
*/}}
{{- define "iggy.clusterEnv" -}}
  {{- $ports := .Values.server.ports }}
- name: IGGY_CLUSTER_ENABLED
  value: "true"
- name: IGGY_CLUSTER_NAME
  value: {{ .Values.server.cluster.name | quote }}
  {{- range .Values.server.cluster.nodes }}
    {{- $id := int .replicaId }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_NAME
  value: {{ .name | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_IP
  value: {{ .ip | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_REPLICA_ID
  value: {{ $id | quote }}
    {{- if .advertisedAddress }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_ADVERTISED_ADDRESS
  value: {{ .advertisedAddress | quote }}
    {{- end }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_PORTS_TCP
  value: {{ (default $ports.tcp (and .ports .ports.tcp)) | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_PORTS_QUIC
  value: {{ (default $ports.quic (and .ports .ports.quic)) | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_PORTS_HTTP
  value: {{ (default $ports.http (and .ports .ports.http)) | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_PORTS_WEBSOCKET
  value: {{ (default $ports.websocket (and .ports .ports.websocket)) | quote }}
- name: IGGY_CLUSTER_NODES_{{ $id }}_PORTS_TCP_REPLICA
  value: {{ .ports.tcpReplica | quote }}
  {{- end }}
{{- end }}

{{/*
Name of the Secret holding the cluster-wide secrets the chart generates from
inline values. Each of encryption, JWT and replica auth may instead point at a
Secret the operator made, which is the path a multi-node cluster wants: the
values have to be byte-identical on every node, so they belong in one object
every release references rather than in each release's values.
*/}}
{{- define "iggy.secretName" -}}
  {{- printf "%s-secrets" (include "iggy.fullname" .) }}
{{- end }}

{{/*
True when any secret has to be generated by the chart, i.e. an inline value is
set for a feature that is switched on and no existing Secret was named for it.
*/}}
{{- define "iggy.createsSecret" -}}
  {{- $server := .Values.server }}
  {{- $create := false }}
  {{- if and $server.encryption.enabled (not $server.encryption.existingSecret.name) }}
    {{- $create = true }}
  {{- end }}
  {{- if and (or $server.jwt.encodingSecret $server.jwt.decodingSecret) (not $server.jwt.existingSecret.name) }}
    {{- $create = true }}
  {{- end }}
  {{- if and $server.cluster.auth.enabled (not $server.cluster.auth.existingSecret.name) }}
    {{- $create = true }}
  {{- end }}
  {{- if $create }}true{{ end }}
{{- end }}

{{/*
Validate the secret configuration. The server enforces all of this at boot, so
catching it during render only saves a scheduling round trip, but a cluster
whose PSK differs between nodes fails as a handshake rejection rather than as
anything that names the cause.
*/}}
{{- define "iggy.validateSecrets" -}}
  {{- $server := .Values.server }}
  {{- if $server.encryption.enabled }}
    {{- if and (not $server.encryption.key) (not $server.encryption.existingSecret.name) }}
      {{- fail "server.encryption.enabled is true but no key was given. Set server.encryption.key to a base64-encoded 32-byte key, or point server.encryption.existingSecret.name at a Secret holding one." }}
    {{- end }}
  {{- end }}
  {{- if $server.cluster.auth.enabled }}
    {{- if not $server.cluster.enabled }}
      {{- fail "server.cluster.auth.enabled is true but server.cluster.enabled is false. Replica authentication only applies to the consensus port, which exists in cluster mode." }}
    {{- end }}
    {{- if and (not $server.cluster.auth.sharedSecret) (not $server.cluster.auth.existingSecret.name) }}
      {{- fail "server.cluster.auth.enabled is true but no shared secret was given. Set server.cluster.auth.sharedSecret, or point server.cluster.auth.existingSecret.name at a Secret holding one. Every node needs the byte-identical value." }}
    {{- end }}
    {{- if and $server.cluster.auth.sharedSecret (lt (len $server.cluster.auth.sharedSecret) 32) }}
      {{- fail (printf "server.cluster.auth.sharedSecret is %d bytes; the server requires at least 32 bytes of CSPRNG output." (len $server.cluster.auth.sharedSecret)) }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
Environment entries for the secret-backed settings, each sourced from whichever
Secret owns it.
*/}}
{{- define "iggy.secretEnv" -}}
  {{- $server := .Values.server }}
  {{- $generated := include "iggy.secretName" . }}
  {{- if $server.encryption.enabled }}
- name: IGGY_SYSTEM_ENCRYPTION_ENABLED
  value: "true"
- name: IGGY_SYSTEM_ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ default $generated $server.encryption.existingSecret.name }}
      key: {{ ternary $server.encryption.existingSecret.key "encryptionKey" (ne $server.encryption.existingSecret.name "") }}
  {{- end }}
  {{- if or $server.jwt.existingSecret.name $server.jwt.encodingSecret }}
- name: IGGY_HTTP_JWT_ENCODING_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ default $generated $server.jwt.existingSecret.name }}
      key: {{ ternary $server.jwt.existingSecret.encodingSecretKey "jwtEncodingSecret" (ne $server.jwt.existingSecret.name "") }}
  {{- end }}
  {{- if or $server.jwt.existingSecret.name $server.jwt.decodingSecret }}
- name: IGGY_HTTP_JWT_DECODING_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ default $generated $server.jwt.existingSecret.name }}
      key: {{ ternary $server.jwt.existingSecret.decodingSecretKey "jwtDecodingSecret" (ne $server.jwt.existingSecret.name "") }}
  {{- end }}
  {{- if $server.cluster.auth.enabled }}
- name: IGGY_CLUSTER_AUTH_ENABLED
  value: "true"
- name: IGGY_CLUSTER_AUTH_SHARED_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ default $generated $server.cluster.auth.existingSecret.name }}
      key: {{ ternary $server.cluster.auth.existingSecret.sharedSecretKey "clusterSharedSecret" (ne $server.cluster.auth.existingSecret.name "") }}
    {{- if or $server.cluster.auth.previousSharedSecret $server.cluster.auth.existingSecret.name }}
- name: IGGY_CLUSTER_AUTH_PREVIOUS_SHARED_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ default $generated $server.cluster.auth.existingSecret.name }}
      key: {{ ternary $server.cluster.auth.existingSecret.previousSharedSecretKey "clusterPreviousSharedSecret" (ne $server.cluster.auth.existingSecret.name "") }}
      optional: true
    {{- end }}
  {{- end }}
{{- end }}
