{{- define "starrocks-backfill.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "starrocks-backfill.configName" -}}
{{- printf "%s-config" (include "starrocks-backfill.fullname" .) -}}
{{- end -}}
