{{- define "cube-maker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cube-maker.fullname" -}}
{{- default (include "cube-maker.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cube-maker.driverServiceName" -}}
{{- printf "%s-driver" (include "cube-maker.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cube-maker.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "cube-maker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "cube-maker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cube-maker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
