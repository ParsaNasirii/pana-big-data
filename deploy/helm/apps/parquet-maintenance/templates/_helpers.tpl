{{- define "parquet-maintenance.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "parquet-maintenance.selector" -}}
app.kubernetes.io/name: parquet-maintenance
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
