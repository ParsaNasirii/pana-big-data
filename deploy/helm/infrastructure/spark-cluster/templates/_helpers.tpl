{{- define "spark-cluster.name" -}}
spark
{{- end -}}

{{- define "spark-cluster.resourceName" -}}
{{- printf "%s-%s" (include "spark-cluster.name" .root) .component -}}
{{- end -}}
