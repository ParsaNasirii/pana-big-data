{{- define "parquet-writer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "parquet-writer.fullname" -}}
{{- default (include "parquet-writer.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "parquet-writer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "parquet-writer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "parquet-writer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "parquet-writer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
