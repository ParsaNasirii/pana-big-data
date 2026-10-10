{{- define "schema-registry.name" -}}{{ default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}{{- end }}
{{- define "schema-registry.fullname" -}}{{ default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{- end }}
{{- define "schema-registry.labels" -}}helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "schema-registry.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}{{- end }}
{{- define "schema-registry.selectorLabels" -}}app.kubernetes.io/name: {{ include "schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}{{- end }}
