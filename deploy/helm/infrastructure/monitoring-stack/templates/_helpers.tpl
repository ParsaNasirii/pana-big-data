{{- define "monitoring-stack.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "monitoring-stack.resourceName" -}}
{{- printf "%s-%s" (include "monitoring-stack.fullname" .root) .component -}}
{{- end -}}
