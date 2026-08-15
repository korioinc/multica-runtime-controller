{{- define "multica-runtime-controller.name" -}}
multica-runtime-controller
{{- end }}

{{- define "multica-runtime-controller.fullname" -}}
{{- $name := include "multica-runtime-controller.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}
{{- define "multica-runtime-controller.labels" -}}
app.kubernetes.io/name: {{ include "multica-runtime-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "multica-runtime-controller.workspaceClaimName" -}}
{{- .Values.workspace.claimName -}}
{{- end }}

{{- define "multica-runtime-controller.identitySecretName" -}}
{{- printf "%s-identity" (include "multica-runtime-controller.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "multica-runtime-controller.daemonProxyServiceName" -}}
{{- printf "%s-daemon-proxy" (include "multica-runtime-controller.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
