package intercept

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	launcherConfigFileEnv = "MULTICA_LAUNCHER_CONFIG_FILE"
	defaultLauncherConfig = "/tmp/multica-runtime-controller/launcher.json"
)

var launcherConfigKeys = []string{
	"POD_NAMESPACE",
	"POD_NAME",
	"MULTICA_RUNTIME_IMAGE",
	"MULTICA_RUNTIME_IMAGE_PULL_POLICY",
	"MULTICA_DAEMON_PROXY_URL",
	"MULTICA_WORKER_SERVICE_ACCOUNT",
	"MULTICA_WORKSPACE_PVC_NAME",
	"MULTICA_WORKER_EXTRA_VOLUMES",
	"MULTICA_WORKER_EXTRA_VOLUME_MOUNTS",
	"MULTICA_WORKER_CPU_REQUEST",
	"MULTICA_WORKER_CPU_LIMIT",
	"MULTICA_WORKER_MEMORY_REQUEST",
	"MULTICA_WORKER_MEMORY_LIMIT",
	"MULTICA_TASK_DEADLINE",
}

type launcherConfigSnapshot struct {
	Values map[string]string `json:"values"`
}

func LoadConfig() (Config, string, error) {
	path, err := launcherConfigPath(os.Getenv)
	if err != nil {
		return Config{}, "", err
	}
	cfg, controllerPodName, err := loadLauncherConfig(path)
	if err == nil {
		return cfg, controllerPodName, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Config{}, "", err
	}
	return loadConfig(os.Getenv)
}

func PersistLauncherConfig() error {
	path, err := launcherConfigPath(os.Getenv)
	if err != nil {
		return err
	}
	return persistLauncherConfig(path, os.Getenv)
}

func launcherConfigPath(getenv func(string) string) (string, error) {
	path := valueOrDefault(getenv(launcherConfigFileEnv), defaultLauncherConfig)
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s must be an absolute path", launcherConfigFileEnv)
	}
	return filepath.Clean(path), nil
}

func persistLauncherConfig(path string, getenv func(string) string) error {
	if _, _, err := loadConfig(getenv); err != nil {
		return fmt.Errorf("validate launcher config: %w", err)
	}
	values := make(map[string]string, len(launcherConfigKeys))
	for _, key := range launcherConfigKeys {
		if value := strings.TrimSpace(getenv(key)); value != "" {
			values[key] = value
		}
	}
	raw, err := json.Marshal(launcherConfigSnapshot{Values: values})
	if err != nil {
		return fmt.Errorf("encode launcher config: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create launcher config directory: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".launcher-*.json")
	if err != nil {
		return fmt.Errorf("create launcher config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure launcher config: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write launcher config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close launcher config: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish launcher config: %w", err)
	}
	return nil
}

func loadLauncherConfig(path string) (Config, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, "", fmt.Errorf("read launcher config: %w", err)
	}
	var snapshot launcherConfigSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Config{}, "", fmt.Errorf("decode launcher config: %w", err)
	}
	return loadConfig(func(key string) string { return snapshot.Values[key] })
}

func loadConfig(getenv func(string) string) (Config, string, error) {
	var extraVolumes []corev1.Volume
	if raw := strings.TrimSpace(getenv("MULTICA_WORKER_EXTRA_VOLUMES")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &extraVolumes); err != nil {
			return Config{}, "", fmt.Errorf("decode MULTICA_WORKER_EXTRA_VOLUMES: %w", err)
		}
	}
	var extraMounts []corev1.VolumeMount
	if raw := strings.TrimSpace(getenv("MULTICA_WORKER_EXTRA_VOLUME_MOUNTS")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &extraMounts); err != nil {
			return Config{}, "", fmt.Errorf("decode MULTICA_WORKER_EXTRA_VOLUME_MOUNTS: %w", err)
		}
	}
	if err := validateExtraMounts(extraVolumes, extraMounts); err != nil {
		return Config{}, "", err
	}

	pullPolicy := corev1.PullPolicy(valueOrDefault(getenv("MULTICA_RUNTIME_IMAGE_PULL_POLICY"), string(corev1.PullIfNotPresent)))
	if pullPolicy != corev1.PullAlways && pullPolicy != corev1.PullIfNotPresent && pullPolicy != corev1.PullNever {
		return Config{}, "", errors.New("MULTICA_RUNTIME_IMAGE_PULL_POLICY must be Always, IfNotPresent, or Never")
	}
	deadline, err := time.ParseDuration(valueOrDefault(getenv("MULTICA_TASK_DEADLINE"), "6h"))
	if err != nil || deadline <= 0 {
		return Config{}, "", errors.New("MULTICA_TASK_DEADLINE must be a positive duration")
	}
	resources, err := resourceRequirements(getenv)
	if err != nil {
		return Config{}, "", err
	}

	cfg := Config{
		Namespace:          strings.TrimSpace(getenv("POD_NAMESPACE")),
		Image:              strings.TrimSpace(getenv("MULTICA_RUNTIME_IMAGE")),
		ImagePullPolicy:    pullPolicy,
		DaemonProxyURL:     strings.TrimSpace(getenv("MULTICA_DAEMON_PROXY_URL")),
		ServiceAccountName: strings.TrimSpace(getenv("MULTICA_WORKER_SERVICE_ACCOUNT")),
		WorkspacePVCName:   strings.TrimSpace(getenv("MULTICA_WORKSPACE_PVC_NAME")),
		ExtraVolumes:       extraVolumes,
		ExtraVolumeMounts:  extraMounts,
		Resources:          resources,
		Deadline:           deadline,
	}
	controllerPodName := strings.TrimSpace(getenv("POD_NAME"))
	if cfg.Namespace == "" || cfg.Image == "" || cfg.DaemonProxyURL == "" || cfg.ServiceAccountName == "" || cfg.WorkspacePVCName == "" || controllerPodName == "" {
		return Config{}, "", errors.New("POD_NAMESPACE, POD_NAME, runtime image, daemon proxy URL, worker service account, and workspace PVC are required")
	}
	return cfg, controllerPodName, nil
}

func resourceRequirements(getenv func(string) string) (corev1.ResourceRequirements, error) {
	values := map[corev1.ResourceName]struct {
		request string
		limit   string
	}{
		corev1.ResourceCPU: {
			request: valueOrDefault(getenv("MULTICA_WORKER_CPU_REQUEST"), "500m"),
			limit:   valueOrDefault(getenv("MULTICA_WORKER_CPU_LIMIT"), "2"),
		},
		corev1.ResourceMemory: {
			request: valueOrDefault(getenv("MULTICA_WORKER_MEMORY_REQUEST"), "1Gi"),
			limit:   valueOrDefault(getenv("MULTICA_WORKER_MEMORY_LIMIT"), "4Gi"),
		},
	}
	result := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	for name, value := range values {
		request, err := resource.ParseQuantity(value.request)
		if err != nil || request.Sign() <= 0 {
			return corev1.ResourceRequirements{}, fmt.Errorf("worker %s request must be a positive Kubernetes quantity", name)
		}
		limit, err := resource.ParseQuantity(value.limit)
		if err != nil || limit.Sign() <= 0 {
			return corev1.ResourceRequirements{}, fmt.Errorf("worker %s limit must be a positive Kubernetes quantity", name)
		}
		result.Requests[name] = request
		result.Limits[name] = limit
	}
	return result, nil
}

func validateExtraMounts(volumes []corev1.Volume, mounts []corev1.VolumeMount) error {
	reservedNames := map[string]bool{"request": true, "workspace": true, "agent-home": true, "tmp": true}
	volumeNames := make(map[string]bool, len(volumes))
	for _, volume := range volumes {
		if volume.Name == "" || reservedNames[volume.Name] || volumeNames[volume.Name] {
			return fmt.Errorf("invalid or duplicate task volume name %q", volume.Name)
		}
		if !approvedVolumeSource(volume.VolumeSource) {
			return fmt.Errorf("task volume %q must use Secret, ConfigMap, or projected data", volume.Name)
		}
		volumeNames[volume.Name] = true
	}
	mountCount := make(map[string]int, len(volumes))
	mountTuples := make(map[string]bool, len(mounts))
	targetPaths := make(map[string]bool, len(mounts))
	for _, mount := range mounts {
		cleanPath := filepath.Clean(mount.MountPath)
		if !volumeNames[mount.Name] || !filepath.IsAbs(cleanPath) || !mount.ReadOnly || mount.SubPathExpr != "" {
			return fmt.Errorf("invalid task volume mount %q", mount.Name)
		}
		cleanSubPath := "."
		if mount.SubPath != "" {
			cleanSubPath = filepath.Clean(mount.SubPath)
			if filepath.IsAbs(cleanSubPath) || cleanSubPath == ".." || strings.HasPrefix(cleanSubPath, ".."+string(filepath.Separator)) {
				return fmt.Errorf("task volume mount %q has an unsafe subPath", mount.Name)
			}
		}
		if reservedExtraMountPath(cleanPath) {
			return fmt.Errorf("task volume mount %q uses a reserved path", mount.Name)
		}
		tuple := mount.Name + "\x00" + cleanPath + "\x00" + cleanSubPath
		if mountTuples[tuple] || targetPaths[cleanPath] {
			return fmt.Errorf("task volume mount %q has a duplicate tuple or target", mount.Name)
		}
		mountTuples[tuple] = true
		targetPaths[cleanPath] = true
		mountCount[mount.Name]++
	}
	for name := range volumeNames {
		if mountCount[name] == 0 {
			return fmt.Errorf("task volume %q has no mount", name)
		}
	}
	return nil
}

func reservedExtraMountPath(path string) bool {
	for _, root := range []string{workspaceRoot, "/tmp", "/home/multica/agents"} {
		if pathShadows(path, root) {
			return true
		}
	}
	if pathsOverlap(path, piHomeMountPath) || pathsOverlap(path, piSessionsMountPath) {
		return true
	}
	requestDir := filepath.Dir(requestPath)
	return pathsOverlap(path, requestDir)
}

func approvedVolumeSource(source corev1.VolumeSource) bool {
	return source.Secret != nil && reflect.DeepEqual(source, corev1.VolumeSource{Secret: source.Secret}) ||
		source.ConfigMap != nil && reflect.DeepEqual(source, corev1.VolumeSource{ConfigMap: source.ConfigMap}) ||
		source.Projected != nil && reflect.DeepEqual(source, corev1.VolumeSource{Projected: source.Projected})
}

func pathShadows(path, root string) bool {
	return path == "/" || path == root || strings.HasPrefix(root, strings.TrimSuffix(path, string(filepath.Separator))+string(filepath.Separator))
}

func pathsOverlap(path, root string) bool {
	return pathShadows(path, root) || strings.HasPrefix(path, root+string(filepath.Separator))
}

func valueOrDefault(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
