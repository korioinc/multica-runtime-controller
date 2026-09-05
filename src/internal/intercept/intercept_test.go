package intercept

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestValidateExtraMounts(t *testing.T) {
	projected := []corev1.Volume{{
		Name:         "codex-home",
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}},
	}}
	valid := []corev1.VolumeMount{
		{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "config.toml", ReadOnly: true},
		{Name: "codex-home", MountPath: "/home/multica/agents/.codex/auth.json", SubPath: "auth.json", ReadOnly: true},
	}
	if err := validateExtraMounts(projected, valid); err != nil {
		t.Fatalf("default Codex config file mounts failed validation: %v", err)
	}
	tests := []struct {
		name    string
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	}{
		{name: "absolute subPath", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "/config.toml", ReadOnly: true}}},
		{name: "escaping subPath", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "../config.toml", ReadOnly: true}}},
		{name: "writable", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml"}}},
		{name: "workspace root", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/workspace", ReadOnly: true}}},
		{name: "workspace ancestor", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/", ReadOnly: true}}},
		{name: "request child", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/var/run/multica/intercept/other", ReadOnly: true}}},
		{name: "Pi home directory", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: piHomeMountPath, ReadOnly: true}}},
		{name: "Pi home child", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: piHomeMountPath + "/agent/settings.json", ReadOnly: true}}},
		{name: "Pi session path", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: piSessionsMountPath, ReadOnly: true}}},
		{name: "Pi session child", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: piSessionsMountPath + "/session.jsonl", ReadOnly: true}}},
		{name: "unsupported volume", volumes: []corev1.Volume{{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp"}}}}, mounts: []corev1.VolumeMount{{Name: "host", MountPath: "/opt/data", ReadOnly: true}}},
		{name: "mixed supported and unsupported volume", volumes: []corev1.Volume{{Name: "mixed", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "config"}, HostPath: &corev1.HostPathVolumeSource{Path: "/tmp"}}}}, mounts: []corev1.VolumeMount{{Name: "mixed", MountPath: "/opt/data", ReadOnly: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateExtraMounts(tt.volumes, tt.mounts); err == nil {
				t.Fatal("unsafe extra mounts unexpectedly passed validation")
			}
		})
	}
}

func TestPrepareRequestRejectsUnsupportedOrUnsafeExecution(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		workDir  string
	}{
		{name: "unsupported provider", provider: "sh", workDir: "/workspace/task"},
		{name: "outside shared workspace", provider: "codex", workDir: "/tmp/task"},
		{name: "workspace root", provider: "codex", workDir: "/workspace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := PrepareRequest(tt.provider, nil, nil, tt.workDir); err == nil {
				t.Fatal("expected request validation error")
			}
		})
	}
}
