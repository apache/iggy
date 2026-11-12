{{/*
Expand the name of the chart.
*/}}
{{- define "iggy-kafka-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "iggy-kafka-gateway.fullname" -}}
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
The headless Service that gives each gateway the DNS name it advertises to Kafka clients.
*/}}
{{- define "iggy-kafka-gateway.headlessName" -}}
  {{- printf "%s-headless" (include "iggy-kafka-gateway.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "iggy-kafka-gateway.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "iggy-kafka-gateway.labels" -}}
helm.sh/chart: {{ include "iggy-kafka-gateway.chart" . }}
{{ include "iggy-kafka-gateway.selectorLabels" . }}
  {{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
  {{- end }}
app.kubernetes.io/component: kafka-gateway
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: iggy
  {{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
  {{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "iggy-kafka-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "iggy-kafka-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "iggy-kafka-gateway.serviceAccountName" -}}
  {{- if .Values.serviceAccount.create }}
{{- default (include "iggy-kafka-gateway.fullname" .) .Values.serviceAccount.name }}
  {{- else }}
{{- default "default" .Values.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
The Secret that holds the Iggy credentials.
*/}}
{{- define "iggy-kafka-gateway.credentialsSecret" -}}
  {{- if .Values.iggy.existingSecret.name }}
{{- .Values.iggy.existingSecret.name }}
  {{- else }}
    {{- printf "%s-iggy-credentials" (include "iggy-kafka-gateway.fullname" .) | trunc 63 | trimSuffix "-" }}
  {{- end }}
{{- end }}

{{/*
The DNS name that gateway `ordinal` advertises: its pod name under the headless Service.
*/}}
{{- define "iggy-kafka-gateway.advertisedHost" -}}
  {{- printf "%s-%d.%s.%s.svc.%s" (include "iggy-kafka-gateway.fullname" .root) (int .ordinal) (include "iggy-kafka-gateway.headlessName" .root) .root.Release.Namespace .root.Values.kafka.clusterDomain }}
{{- end }}

{{/*
IGGY_KAFKA_BROKERS: every gateway as `node-id@host:port`, the node id being the ordinal plus one.
*/}}
{{- define "iggy-kafka-gateway.brokers" -}}
  {{- $root := . }}
  {{- $brokers := list }}
  {{- range $ordinal := until (int .Values.replicaCount) }}
    {{- $host := include "iggy-kafka-gateway.advertisedHost" (dict "root" $root "ordinal" $ordinal) }}
    {{- $brokers = append $brokers (printf "%d@%s:%d" (add $ordinal 1) $host (int $root.Values.kafka.port)) }}
  {{- end }}
{{- join "," $brokers }}
{{- end }}

{{/*
Refuse a render that the gateway would only refuse at startup, after the pods are scheduled.
*/}}
{{- define "iggy-kafka-gateway.validate" -}}
  {{- if not .Values.iggy.address }}
    {{- fail "iggy.address is empty. Set it to host:port of the TCP transport of an Iggy server or of one node of an Iggy cluster." }}
  {{- end }}
  {{- if lt (int .Values.replicaCount) 1 }}
    {{- fail "replicaCount must be 1 or more" }}
  {{- end }}
  {{- if and (not .Values.iggy.existingSecret.name) (not .Values.iggy.password) }}
    {{- fail "iggy.password is empty. Set it, or name a Secret that holds the credentials in iggy.existingSecret.name." }}
  {{- end }}
  {{- if eq (int .Values.kafka.port) (int .Values.http.port) }}
    {{- fail "kafka.port and http.port must differ" }}
  {{- end }}
  {{- $owned := list "IGGY_KAFKA_NODE_ID" "IGGY_KAFKA_BIND_ADDR" "IGGY_KAFKA_ADVERTISED_HOST" "IGGY_KAFKA_ADVERTISED_PORT" "IGGY_KAFKA_BROKERS" "IGGY_KAFKA_COORDINATOR_LOG" "IGGY_KAFKA_HTTP_ADDR" "IGGY_KAFKA_SASL_ENABLED" "IGGY_KAFKA_BRIDGE_ENABLED" "IGGY_KAFKA_IGGY_ADDR" "IGGY_KAFKA_IGGY_STREAM" "IGGY_KAFKA_IGGY_USERNAME" "IGGY_KAFKA_IGGY_PASSWORD" "IGGY_KAFKA_IGGY_TLS_ENABLED" "IGGY_KAFKA_IGGY_TLS_DOMAIN" "IGGY_KAFKA_IGGY_TLS_CA_FILE" "IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED" "IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS" }}
  {{- range .Values.extraEnv }}
    {{- $name := toString .name }}
    {{- if has $name $owned }}
      {{- fail (printf "extraEnv sets %s, which the chart sets. Use the chart values." $name) }}
    {{- end }}
    {{- if or (has $name (list "IGGY_KAFKA_LISTENERS" "IGGY_KAFKA_ADVERTISED_LISTENERS")) (hasPrefix "IGGY_KAFKA_TLS_" $name) }}
      {{- fail (printf "extraEnv sets %s. The chart runs one PLAINTEXT or SASL_PLAINTEXT listener and has no TLS listener yet." $name) }}
    {{- end }}
  {{- end }}
{{- end }}
