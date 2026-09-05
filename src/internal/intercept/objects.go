package intercept

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

const (
	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByValue      = "multica-runtime-controller"
	TaskIDLabel         = "multica.ai/task-id"
	RequestKey          = "request.json"
	requestPath         = "/var/run/multica/intercept/request.json"
	workspaceRoot       = "/workspace"
	providerBinDir      = "/opt/multica/providers/bin"
	agentHome           = "/home/multica/agents"
	agentHomeVolumePath = "/var/run/multica/agent-home"
	agentHomeSubPath    = "home"
	piHomeMountPath     = agentHome + "/.pi"
	piHomeSubPath       = ".pi"
	piSessionsMountPath = agentHome + "/.multica/pi-sessions"
	piSeedMountPath     = "/var/run/multica/pi-seed"
)

var providerExecutables = map[string]string{
	"agy":     providerBinDir + "/agy",
	"codex":   providerBinDir + "/codex",
	"copilot": providerBinDir + "/copilot",
	"pi":      providerBinDir + "/pi",
}

type Request struct {
	Provider string   `json:"provider"`
	Args     []string `json:"args,omitempty"`
	Env      []string `json:"env"`
	WorkDir  string   `json:"work_dir"`
}

type Config struct {
	Namespace          string
	Image              string
	ImagePullPolicy    corev1.PullPolicy
	DaemonProxyURL     string
	ServiceAccountName string
	WorkspacePVCName   string
	ExtraVolumes       []corev1.Volume
	ExtraVolumeMounts  []corev1.VolumeMount
	Resources          corev1.ResourceRequirements
	Deadline           time.Duration
}

func ProviderExecutable(provider string) (string, error) {
	path, ok := providerExecutables[provider]
	if !ok {
		return "", fmt.Errorf("unsupported intercepted provider %q", provider)
	}
	return path, nil
}

func taskGenerateNameFor(taskID string) (string, error) {
	parsed, err := uuid.Parse(taskID)
	if err != nil {
		return "", errors.New("MULTICA_TASK_ID must be a UUID")
	}
	canonical := parsed.String()
	return "task-" + canonical[:8] + "-", nil
}

func PrepareRequest(provider string, args, environ []string, workDir string) (Request, error) {
	if _, err := ProviderExecutable(provider); err != nil {
		return Request{}, err
	}
	cleanWorkDir := filepath.Clean(workDir)
	rel, err := filepath.Rel(workspaceRoot, cleanWorkDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return Request{}, fmt.Errorf("provider work directory %q must be below %s", workDir, workspaceRoot)
	}
	return Request{
		Provider: provider,
		Args:     slices.Clone(args),
		Env:      taskEnvironment(environ),
		WorkDir:  cleanWorkDir,
	}, nil
}

func taskEnvironment(environ []string) []string {
	values := make(map[string]string, len(environ))
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || omitTaskEnvironment(key) {
			continue
		}
		values[key] = value
	}
	values["TMPDIR"] = "/tmp"
	values["TMP"] = "/tmp"
	values["TEMP"] = "/tmp"
	values["PATH"] = providerBinDir + ":/usr/local/bin:/usr/bin:/bin"
	values["MULTICA_REPO_CHECKOUT_MODE"] = "isolated"

	keys := slices.Sorted(maps.Keys(values))
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func omitTaskEnvironment(key string) bool {
	switch key {
	case "MULTICA_CONTROLLER_TOKEN_FILE", "MULTICA_CONTROLLER_GRANT_KEY", "MULTICA_RUNTIME_IMAGE", "MULTICA_RUNTIME_IMAGE_PULL_POLICY",
		"MULTICA_DAEMON_PROXY_URL",
		"MULTICA_WORKER_SERVICE_ACCOUNT", "MULTICA_WORKSPACE_PVC_NAME", "MULTICA_WORKER_EXTRA_VOLUMES",
		"MULTICA_WORKER_EXTRA_VOLUME_MOUNTS", "MULTICA_WORKER_CPU_REQUEST", "MULTICA_WORKER_CPU_LIMIT",
		"MULTICA_WORKER_MEMORY_REQUEST", "MULTICA_WORKER_MEMORY_LIMIT", "MULTICA_TASK_DEADLINE",
		"CODEX_HOME", "PI_CODING_AGENT_DIR", "POD_NAME", "POD_NAMESPACE":
		return true
	}
	return strings.HasPrefix(key, "KUBERNETES_SERVICE_")
}

func requestSecret(cfg Config, taskID string, request Request, owner metav1.OwnerReference) (*corev1.Secret, error) {
	generateName, err := taskGenerateNameFor(taskID)
	if err != nil {
		return nil, err
	}
	if err := validateTaskConfig(cfg); err != nil {
		return nil, err
	}
	rawRequest, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode provider request: %w", err)
	}
	labels := map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID}
	ownerReferences := []metav1.OwnerReference{owner}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName, Namespace: cfg.Namespace, Labels: maps.Clone(labels), OwnerReferences: ownerReferences,
		},
		Immutable: ptr.To(true),
		Type:      corev1.SecretTypeOpaque,
		Data:      map[string][]byte{RequestKey: rawRequest},
	}, nil
}

func taskPod(cfg Config, taskID, requestSecretName string, request Request, owner metav1.OwnerReference) (*corev1.Pod, error) {
	generateName, err := taskGenerateNameFor(taskID)
	if err != nil {
		return nil, err
	}
	if err := validateTaskConfig(cfg); err != nil {
		return nil, err
	}
	if requestEnvironmentValue(request.Env, "MULTICA_TASK_ID") != taskID {
		return nil, errors.New("provider request does not match the task identity")
	}
	taskRoot, taskSubPath, err := taskStorageRoot(request)
	if err != nil {
		return nil, err
	}
	piSession, err := taskPiSession(request)
	if err != nil {
		return nil, err
	}
	requestSecretName = strings.TrimSpace(requestSecretName)
	if requestSecretName == "" || len(k8svalidation.IsDNS1123Subdomain(requestSecretName)) != 0 {
		return nil, errors.New("provider request Secret name must be a valid DNS subdomain")
	}
	labels := map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID}
	ownerReferences := []metav1.OwnerReference{owner}

	volumes := []corev1.Volume{
		{Name: "request", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: requestSecretName, DefaultMode: ptr.To[int32](0o440)}}},
		{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.WorkspacePVCName}}},
		{Name: "agent-home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "request", MountPath: "/var/run/multica/intercept", ReadOnly: true},
		{Name: "workspace", MountPath: taskRoot, SubPath: taskSubPath},
		{Name: "agent-home", MountPath: agentHome, SubPath: agentHomeSubPath},
		{Name: "tmp", MountPath: "/tmp"},
	}
	if piSession != "" {
		mounts = append(mounts, corev1.VolumeMount{
			Name: "workspace", MountPath: piSession,
			SubPath: ".multica/pi-sessions/" + filepath.Base(piSession),
		})
	}
	for i := range cfg.ExtraVolumes {
		volumes = append(volumes, *cfg.ExtraVolumes[i].DeepCopy())
	}
	for i := range cfg.ExtraVolumeMounts {
		mounts = append(mounts, *cfg.ExtraVolumeMounts[i].DeepCopy())
	}

	runAsUser := int64(65532)
	rootUser := int64(0)
	deadlineSeconds := int64(cfg.Deadline.Seconds())
	if deadlineSeconds < 1 {
		deadlineSeconds = int64((6 * time.Hour).Seconds())
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName, Namespace: cfg.Namespace, Labels: maps.Clone(labels), OwnerReferences: ownerReferences,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:            cfg.ServiceAccountName,
			AutomountServiceAccountToken:  ptr.To(false),
			RestartPolicy:                 corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:         &deadlineSeconds,
			TerminationGracePeriodSeconds: ptr.To[int64](10),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr.To(true), RunAsUser: &runAsUser, RunAsGroup: &runAsUser, FSGroup: &runAsUser,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: []corev1.Container{{
				Name:            "tmp-permissions",
				Image:           cfg.Image,
				ImagePullPolicy: cfg.ImagePullPolicy,
				Command:         []string{"/bin/sh", "-ec"},
				Args:            []string{"chmod 1777 /tmp"},
				Resources:       cfg.Resources,
				VolumeMounts:    []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: ptr.To(false), RunAsUser: &rootUser, RunAsGroup: &rootUser,
					AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}, {
				Name:            "provider-home-init",
				Image:           cfg.Image,
				ImagePullPolicy: cfg.ImagePullPolicy,
				Command:         []string{"/bin/sh", "-ec"},
				Args:            []string{taskHomeInitScript},
				Env: []corev1.EnvVar{
					{Name: "PI_SEED", Value: piSeedMountPath},
					{Name: "AGENT_HOME", Value: agentHomeVolumePath + "/" + agentHomeSubPath},
				},
				Resources: cfg.Resources,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "agent-home", MountPath: agentHomeVolumePath},
					{Name: "workspace", MountPath: piSeedMountPath, SubPath: piHomeSubPath, ReadOnly: true},
					{Name: "tmp", MountPath: "/tmp"},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
			Containers: []corev1.Container{{
				Name:            "worker",
				Image:           cfg.Image,
				ImagePullPolicy: cfg.ImagePullPolicy,
				Args:            []string{"hold"},
				Env: []corev1.EnvVar{
					{Name: "HOME", Value: agentHome},
					{Name: "TMPDIR", Value: "/tmp"},
					{Name: "MULTICA_TASK_ID", Value: taskID},
					{Name: "MULTICA_DAEMON_PROXY_URL", Value: cfg.DaemonProxyURL},
					{Name: "MULTICA_REQUEST_SECRET_NAME", Value: requestSecretName},
				},
				Resources:    cfg.Resources,
				VolumeMounts: mounts,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:   corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/usr/bin/bash", "-ec", "exec 3<>/dev/tcp/127.0.0.1/19514"}}},
					PeriodSeconds:  1,
					TimeoutSeconds: 1,
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
			Volumes: volumes,
		},
	}, nil
}

func validateTaskConfig(cfg Config) error {
	if cfg.Namespace == "" || cfg.Image == "" || cfg.DaemonProxyURL == "" || cfg.ServiceAccountName == "" || cfg.WorkspacePVCName == "" {
		return errors.New("task Pod namespace, image, daemon proxy URL, service account, and workspace PVC are required")
	}
	return nil
}

func RequestFilePath() string {
	return requestPath
}
