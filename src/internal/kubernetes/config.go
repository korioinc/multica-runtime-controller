// Package kubernetes owns typed Kubernetes resources and UID-fenced operations.
package kubernetes

import (
	"errors"
	"os"
	"regexp"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
)

// Config is the chart's execution policy. Image and configuration selections
// are bound at controller startup and travel only in the versioned RuntimeRef.
type Config struct {
	Platform                string                            `json:"platform"`
	ImagePullPolicy         corev1.PullPolicy                 `json:"imagePullPolicy"`
	WorkspaceClaim          string                            `json:"workspaceClaim"`
	WorkspaceAccessMode     corev1.PersistentVolumeAccessMode `json:"workspaceAccessMode"`
	ServiceAccount          string                            `json:"serviceAccount"`
	ImagePullSecrets        []corev1.LocalObjectReference     `json:"imagePullSecrets"`
	NodeSelector            map[string]string                 `json:"nodeSelector"`
	Tolerations             []corev1.Toleration               `json:"tolerations"`
	SingleNodeName          string                            `json:"singleNodeName"`
	Resources               corev1.ResourceRequirements       `json:"resources"`
	TaskDeadlineSeconds     int64                             `json:"taskDeadlineSeconds"`
	TerminationGraceSeconds int64                             `json:"terminationGraceSeconds"`
	ConfigEnvFrom           []corev1.EnvFromSource            `json:"configEnvFrom"`
	ConfigEnv               []corev1.EnvVar                   `json:"configEnv"`
}

var sha256String = regexp.MustCompile(`^[a-f0-9]{64}$`)

func LoadConfig(path string) (Config, error) {
	var cfg Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err = runtimeimage.Decode(raw, &cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if c.Platform != "linux/amd64" && c.Platform != "linux/arm64" {
		return errors.New("configuration: unsupported platform")
	}
	if c.ImagePullPolicy != corev1.PullAlways && c.ImagePullPolicy != corev1.PullIfNotPresent && c.ImagePullPolicy != corev1.PullNever {
		return errors.New("configuration: invalid image pull policy")
	}
	if c.WorkspaceClaim == "" || c.ServiceAccount == "" || c.TaskDeadlineSeconds < 1 || c.TerminationGraceSeconds < 1 {
		return errors.New("configuration: workspace PVC, worker account and positive deadlines required")
	}
	switch c.WorkspaceAccessMode {
	case corev1.ReadWriteOnce:
		if c.SingleNodeName == "" {
			return errors.New("configuration: RWO requires a fixed Node name")
		}
	case corev1.ReadWriteMany:
	default:
		return errors.New("configuration: unsupported storage access mode")
	}
	if value := c.NodeSelector["kubernetes.io/arch"]; value != "" && value != strings.TrimPrefix(c.Platform, "linux/") {
		return errors.New("configuration: conflicting architecture selector")
	}
	if value := c.NodeSelector["kubernetes.io/os"]; value != "" && value != "linux" {
		return errors.New("configuration: conflicting OS selector")
	}
	for _, v := range c.ConfigEnv {
		if strings.Contains(v.Value, "$(") {
			return errors.New("configuration: operator env requires literal values or valueFrom")
		}
		if runtimeimage.Reserved(v.Name) {
			return errors.New("configuration: operator env overrides runtime authority")
		}
	}
	return nil
}
