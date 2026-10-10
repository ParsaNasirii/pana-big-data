{{- define "starrocks-writer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "starrocks-writer.fullname" -}}
{{- default (include "starrocks-writer.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "starrocks-writer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "starrocks-writer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "starrocks-writer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "starrocks-writer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
