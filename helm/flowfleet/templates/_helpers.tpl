{{/* ======================================================================
     Naming
     ====================================================================== */}}
{{- define "flowfleet.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flowfleet.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Component resource name, e.g. "flowfleet-kafka". Falls back to <fullname> when
     the release name already carries the chart name (`helm install flowfleet ...`). */}}
{{- define "flowfleet.component.fullname" -}}
{{- $top := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "%s-%s" (include "flowfleet.fullname" $top) $component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flowfleet.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* ======================================================================
     Labels
     ====================================================================== */}}
{{- define "flowfleet.labels" -}}
helm.sh/chart: {{ include "flowfleet.chart" . }}
app.kubernetes.io/name: {{ include "flowfleet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/part-of: flowfleet
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Args: (list $top $component). Adds the component label + selector labels. */}}
{{- define "flowfleet.componentLabels" -}}
{{- $top := index . 0 -}}
{{- $component := index . 1 -}}
{{ include "flowfleet.labels" $top }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{- define "flowfleet.selectorLabels" -}}
{{- $top := index . 0 -}}
{{- $component := index . 1 -}}
app.kubernetes.io/name: {{ include "flowfleet.name" $top }}
app.kubernetes.io/instance: {{ $top.Release.Name }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{/* ======================================================================
     Images — (list $top $imageDict) where $imageDict has .repository/.tag,
     or a bare image string.
     ====================================================================== */}}
{{- define "flowfleet.image" -}}
{{- $top := index . 0 -}}
{{- $img := index . 1 -}}
{{- $registry := $top.Values.image.registry | default "" -}}
{{- $ref := "" -}}
{{- if kindIs "string" $img -}}
{{- $ref = $img -}}
{{- else -}}
{{- $ref = printf "%s:%s" $img.repository $img.tag -}}
{{- end -}}
{{- if $registry -}}
{{- printf "%s/%s" (trimSuffix "/" $registry) $ref -}}
{{- else -}}
{{- $ref -}}
{{- end -}}
{{- end -}}

{{/* ======================================================================
     Shared config / secret references
     ====================================================================== */}}
{{- define "flowfleet.envConfigMapName" -}}
{{- printf "%s-env" (include "flowfleet.fullname" .) -}}
{{- end -}}

{{- define "flowfleet.secretName" -}}
{{- .Values.secret.name | default (printf "%s-credentials" (include "flowfleet.fullname" .)) -}}
{{- end -}}

{{/* JDBC URLs derived from the in-cluster service names. */}}
{{- define "flowfleet.postgresHost" -}}{{ include "flowfleet.component.fullname" (list . "postgres") }}{{- end -}}
{{- define "flowfleet.timescaleHost" -}}{{ include "flowfleet.component.fullname" (list . "timescaledb") }}{{- end -}}
{{- define "flowfleet.clickhouseHost" -}}{{ include "flowfleet.component.fullname" (list . "clickhouse") }}{{- end -}}
{{- define "flowfleet.kafkaHeadless" -}}{{ include "flowfleet.component.fullname" (list . "kafka") }}{{- end -}}

{{/* Standard pod scheduling block, indented by the caller. */}}
{{- define "flowfleet.scheduling" -}}
{{- with .Values.nodeSelector }}
nodeSelector:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
{{ toYaml . | indent 2 }}
{{- end }}
{{- end -}}
