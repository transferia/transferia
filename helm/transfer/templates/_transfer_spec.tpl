{{- define "transfer.spec" -}}
serviceAccountName: {{ .Values.serviceAccount.externalName | default $.Release.Name }}
{{- if .Values.image.imagePullSecrets }}
imagePullSecrets:
  {{- range .Values.image.imagePullSecrets }}
    {{- printf "- name: %s" .name | nindent 2 }}
  {{- end }}
{{- end }}
containers:
  - name: transfer
    image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default $.Chart.AppVersion }}"
    command:
      - "/usr/local/bin/trcli"
      - "{{ .commandType }}"
      - "--transfer"
      - "/volume-mounts/datatransfer/config/config.yaml"
      - "--coordinator"
      - "{{ .Values.coordinator.type }}"
      - "--coordinator-s3-bucket"
      - "{{ .Values.coordinator.bucket }}"
      - "--log-level"
      - "{{ .Values.log.level }}"
      - "--log-config"
      - "{{ .Values.log.config }}"
      - "--coordinator-job-count"
      - "{{.Values.coordinator.job_count}}"
      - "--coordinator-process-count"
      - "{{.Values.coordinator.process_count}}"
    env:
    - name: GOMEMLIMIT
      valueFrom:
        resourceFieldRef:
          resource: limits.memory
  {{- if .Values.env }}
    {{- range $name, $value := .Values.env }}
    - name: {{ $name }}
      value: {{ $value }}
    {{- end }}
  {{- end }}
  {{- if .Values.secret_env }}
    {{- range .Values.secret_env }}
    - name: {{ .env_name }}
      valueFrom:
        secretKeyRef:
          name: {{ .secret_name }}
          key: {{ .secret_key }}
    {{- end }}
  {{- end }}
    ports:
      - name: pprof
        protocol: TCP
        containerPort: 8080
      - name: health
        protocol: TCP
        containerPort: 3000
    {{- if .Values.podMonitor.enabled }}
      - name: prometheus
        containerPort: 9091
        protocol: TCP
    {{- end }}
    volumeMounts:
      - name: config
        mountPath: /volume-mounts/datatransfer/config
    {{- with .Values.resources }}
    resources:
      {{- toYaml . | nindent 6 }}
    {{- end }}
volumes:
  - name: config
    configMap:
      name: {{ $.Release.Name }}
      items:
        - key: config.yaml
          path: config.yaml
{{- if .Values.nodeSelector }}
nodeSelector:
  {{- toYaml .Values.nodeSelector | nindent 2 }}
{{- end }}
{{- if .Values.tolerations }}
tolerations:
  {{- toYaml .Values.tolerations | nindent 2 }}
{{- end }}
{{- end -}}
