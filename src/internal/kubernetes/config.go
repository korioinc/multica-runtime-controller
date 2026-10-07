// Package kubernetes owns typed Kubernetes resources and UID-fenced operations.
package kubernetes

import (
	"errors"
	"os"
	"regexp"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Config is the chart's execution policy. Image and configuration selections
// are bound at controller startup and travel only in the versioned RuntimeRef.
type Config struct {
	Platform         string                        `json:"platform"`
	ImagePullPolicy  corev1.PullPolicy             `json:"imagePullPolicy"`
	Worker           Worker                        `json:"worker"`
	Storage          Storage                       `json:"storage"`
	NFS              NFS                           `json:"nfs"`
	ServiceAccount   string                        `json:"serviceAccount"`
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets"`
	NodeSelector     map[string]string             `json:"nodeSelector"`
	Tolerations      []corev1.Toleration           `json:"tolerations"`
	ConfigEnvFrom    []corev1.EnvFromSource        `json:"configEnvFrom"`
	ConfigEnv        []corev1.EnvVar               `json:"configEnv"`
}

type Worker struct {
	TemporarySizeLimit        string                      `json:"temporarySizeLimit"`
	Resources                 corev1.ResourceRequirements `json:"resources"`
	TaskDeadlineSeconds       int64                       `json:"taskDeadlineSeconds"`
	TerminationGraceSeconds   int64                       `json:"terminationGraceSeconds"`
	PreparationTimeoutSeconds int64                       `json:"preparationTimeoutSeconds"`
}
type Storage struct {
	ClaimName                 string `json:"claimName"`
	MaxTasks                  int    `json:"maxTasks"`
	MaxBytes                  int64  `json:"maxBytes"`
	MaxConcurrentPreparations int    `json:"maxConcurrentPreparations"`
}
type NFS struct {
	Server string `json:"server"`
	Image  string `json:"image"`
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
	if c.ServiceAccount == "" || c.Worker.TaskDeadlineSeconds < 1 || c.Worker.TaskDeadlineSeconds > 31536000 || c.Worker.TerminationGraceSeconds < 1 || c.Worker.TerminationGraceSeconds > 86400 {
		return errors.New("configuration: worker account and bounded positive deadlines required")
	}
	if c.Worker.PreparationTimeoutSeconds < 1 || c.Worker.PreparationTimeoutSeconds > 86400 {
		return errors.New("configuration: bounded preparation deadline required")
	}
	if c.Storage.ClaimName == "" || c.Storage.MaxTasks < 1 || c.Storage.MaxBytes < 1 || c.Storage.MaxConcurrentPreparations < 1 || c.Storage.MaxConcurrentPreparations > 32 {
		return errors.New("configuration: installation claim and bounded admission budgets required")
	}
	if len(validation.IsDNS1123Label(c.NFS.Server)) != 0 {
		return errors.New("configuration: installation NFS Service name required")
	}
	if image, err := runtimeimage.NormalizeImageID(c.NFS.Image); err != nil || image != c.NFS.Image {
		return errors.New("configuration: NFS image requires an immutable repository digest")
	}
	temporary, err := resource.ParseQuantity(c.Worker.TemporarySizeLimit)
	if err != nil || temporary.Sign() <= 0 {
		return errors.New("configuration: positive temporary emptyDir limit required")
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		request := c.Worker.Resources.Requests[name]
		if request.Sign() <= 0 {
			return errors.New("configuration: positive CPU, memory and ephemeral-storage requests required")
		}
		if limit, ok := c.Worker.Resources.Limits[name]; ok && (limit.Sign() <= 0 || limit.Cmp(request) < 0) {
			return errors.New("configuration: resource limits must be positive and cover requests")
		}
	}
	if limit, ok := c.Worker.Resources.Limits[corev1.ResourceEphemeralStorage]; ok && limit.Cmp(temporary) < 0 {
		return errors.New("configuration: ephemeral-storage limit must cover temporary files")
	}
	if c.NodeSelector["kubernetes.io/hostname"] != "" {
		return errors.New("configuration: worker hostname pinning is forbidden")
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
		if runtimeimage.Reserved(v.Name) && !runtimeimage.ExecutionSetting(v.Name) {
			return errors.New("configuration: operator env overrides runtime authority")
		}
	}
	return nil
}
