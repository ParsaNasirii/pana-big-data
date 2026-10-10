{{- define "postgres.name" -}}
postgres
{{- end -}}

{{- define "postgres.resourceName" -}}
{{- printf "%s-%s" (include "postgres.name" .root) .component -}}
{{- end -}}
