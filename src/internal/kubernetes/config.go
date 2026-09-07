// Package kubernetes owns typed Kubernetes resources and UID-fenced operations.
package kubernetes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
)

type Config struct {
	CoreImage               string                            `json:"coreImage"`
	CorePullPolicy          corev1.PullPolicy                 `json:"corePullPolicy"`
	EnvironmentImage        string                            `json:"environmentImage"`
	EnvironmentPullPolicy   corev1.PullPolicy                 `json:"environmentPullPolicy"`
	Platform                string                            `json:"platform"`
	EnvironmentID           string                            `json:"environmentID"`
	ToolsClaim              string                            `json:"toolsClaim"`
	WorkspaceClaim          string                            `json:"workspaceClaim"`
	ToolsAccessMode         corev1.PersistentVolumeAccessMode `json:"toolsAccessMode"`
	WorkspaceAccessMode     corev1.PersistentVolumeAccessMode `json:"workspaceAccessMode"`
	ServiceAccount          string                            `json:"serviceAccount"`
	ImagePullSecrets        []corev1.LocalObjectReference     `json:"imagePullSecrets"`
	NodeSelector            map[string]string                 `json:"nodeSelector"`
	Tolerations             []corev1.Toleration               `json:"tolerations"`
	SingleNodeName          string                            `json:"singleNodeName"`
	Resources               corev1.ResourceRequirements       `json:"resources"`
	TaskDeadlineSeconds     int64                             `json:"taskDeadlineSeconds"`
	TerminationGraceSeconds int64                             `json:"terminationGraceSeconds"`
	ConfigVolumes           []corev1.Volume                   `json:"configVolumes"`
	ConfigMounts            []corev1.VolumeMount              `json:"configMounts"`
	ConfigEnvFrom           []corev1.EnvFromSource            `json:"configEnvFrom"`
	ConfigEnv               []corev1.EnvVar                   `json:"configEnv"`
}

var pinnedImage = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)
var sha256String = regexp.MustCompile(`^[a-f0-9]{64}$`)

func LoadConfig(path string) (Config, error) {
	var cfg Config
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}
func (c Config) Validate() error {
	if !pinnedImage.MatchString(c.CoreImage) || !pinnedImage.MatchString(c.EnvironmentImage) || !sha256String.MatchString(c.EnvironmentID) {
		return errors.New("configuration: pinned core/environment images and environment ID required")
	}
	if c.Platform != "linux/amd64" && c.Platform != "linux/arm64" {
		return errors.New("configuration: unsupported platform")
	}
	for _, policy := range []corev1.PullPolicy{c.CorePullPolicy, c.EnvironmentPullPolicy} {
		if policy != corev1.PullAlways && policy != corev1.PullIfNotPresent && policy != corev1.PullNever {
			return errors.New("configuration: invalid image pull policy")
		}
	}
	if c.ToolsClaim == "" || c.WorkspaceClaim == "" || c.ToolsClaim == c.WorkspaceClaim || c.ServiceAccount == "" || c.TaskDeadlineSeconds < 1 || c.TerminationGraceSeconds < 1 {
		return errors.New("configuration: separate PVCs, worker account and positive deadlines required")
	}
	rwo := false
	for _, mode := range []corev1.PersistentVolumeAccessMode{c.ToolsAccessMode, c.WorkspaceAccessMode} {
		switch mode {
		case corev1.ReadWriteOnce:
			rwo = true
		case corev1.ReadWriteMany:
		default:
			return errors.New("configuration: unsupported storage access mode")
		}
	}
	if rwo && c.SingleNodeName == "" {
		return errors.New("configuration: RWO requires a fixed Node name")
	}
	arch := strings.TrimPrefix(c.Platform, "linux/")
	if value := c.NodeSelector["kubernetes.io/arch"]; value != "" && value != arch {
		return errors.New("configuration: conflicting architecture selector")
	}
	if value := c.NodeSelector["kubernetes.io/os"]; value != "" && value != "linux" {
		return errors.New("configuration: conflicting OS selector")
	}
	if err := validateConfigMounts(c.ConfigVolumes, c.ConfigMounts); err != nil {
		return err
	}
	for _, v := range c.ConfigEnv {
		if strings.Contains(v.Value, "$(") {
			return errors.New("configuration: operator env requires literal values or valueFrom")
		}
		if v.Name == "HOME" || v.Name == "PATH" || v.Name == "TMPDIR" || strings.HasPrefix(v.Name, "MULTICA_") || strings.HasPrefix(v.Name, "POD_") || strings.HasPrefix(v.Name, "ENV_") {
			return errors.New("configuration: operator env overrides runtime authority")
		}
	}
	return nil
}

// Configuration mounts supply native HOME files; they cannot shadow runtime,
// session or storage authority, even by mounting a parent directory.
func validateConfigMounts(volumes []corev1.Volume, mounts []corev1.VolumeMount) error {
	known := map[string]bool{}
	for _, v := range volumes {
		_, exists := known[v.Name]
		if v.Name == "" || exists || strings.HasPrefix(v.Name, "runtime-") {
			return errors.New("configuration: duplicate or reserved volume")
		}
		s := v.VolumeSource
		valid := s.Secret != nil && reflect.DeepEqual(s, corev1.VolumeSource{Secret: s.Secret}) || s.ConfigMap != nil && reflect.DeepEqual(s, corev1.VolumeSource{ConfigMap: s.ConfigMap})
		if s.Projected != nil && reflect.DeepEqual(s, corev1.VolumeSource{Projected: s.Projected}) {
			valid = true
			for _, p := range s.Projected.Sources {
				if p.Secret == nil && p.ConfigMap == nil {
					valid = false
				}
			}
		}
		if !valid {
			return errors.New("configuration: config volumes require Secret or ConfigMap data")
		}
		known[v.Name] = false
	}
	paths := []string{}
	for _, m := range mounts {
		_, ok := known[m.Name]
		if !ok || !m.ReadOnly || m.SubPathExpr != "" || filepath.Clean(m.MountPath) != m.MountPath || !strings.HasPrefix(m.MountPath, wire.Home+"/") || m.SubPath != "" && !filepath.IsLocal(m.SubPath) {
			return errors.New("configuration: unsafe config mount")
		}
		for _, protected := range []string{wire.PiSessionsRoot, wire.Home + "/.multica/config.json", wire.Home + "/.codex/skills"} {
			if overlap(m.MountPath, protected) {
				return errors.New("configuration: config mount shadows session or daemon authority")
			}
		}
		for _, other := range paths {
			if overlap(m.MountPath, other) {
				return errors.New("configuration: overlapping config mounts")
			}
		}
		paths = append(paths, m.MountPath)
		known[m.Name] = true
	}
	for _, used := range known {
		if !used {
			return errors.New("configuration: unused config volume")
		}
	}
	return nil
}
func overlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
