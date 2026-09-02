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
