package kubernetes

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

const managedLabel = "app.kubernetes.io/managed-by"
const managedValue = "multica-runtime-controller"
const taskLabel = "multica.ai/task-id"
const storageLabel = "multica.ai/storage-id"
const attemptLabel = "multica.ai/attempt-id"
const digestAnnotation = "multica.ai/request-sha256"

type Owner struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}
type Reference struct {
	Namespace        string `json:"namespace"`
	Owner            Owner  `json:"owner"`
	TaskID           string `json:"taskID"`
	StorageID        string `json:"storageID"`
	AttemptID        string `json:"attemptID"`
	PodName          string `json:"podName"`
	SecretName       string `json:"secretName"`
	PodUID           string `json:"podUID,omitempty"`
	SecretUID        string `json:"secretUID,omitempty"`
	RequestDigest    string `json:"requestDigest"`
	CoreImage        string `json:"coreImage"`
	EnvironmentImage string `json:"environmentImage"`
	EnvironmentID    string `json:"environmentID"`
	PodDigest        string `json:"podDigest"`
	FixedNode        string `json:"fixedNode,omitempty"`
}

func (r Reference) metadata(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: map[string]string{managedLabel: managedValue, taskLabel: r.TaskID, storageLabel: r.StorageID, attemptLabel: r.AttemptID}, Annotations: map[string]string{digestAnnotation: r.RequestDigest}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: r.Owner.Name, UID: types.UID(r.Owner.UID), Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}}
}
func secretObject(ref Reference, request wire.Request) (*corev1.Secret, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if wire.Digest(raw) != ref.RequestDigest {
		return nil, errors.New("request digest mismatch")
	}
	if _, err := wire.Decode(raw); err != nil {
		return nil, err
	}
	return &corev1.Secret{ObjectMeta: ref.metadata(ref.SecretName), Immutable: ptr.To(true), Type: corev1.SecretTypeOpaque, Data: map[string][]byte{wire.RequestKey: raw}}, nil
}

func podObject(cfg Config, ref Reference, request wire.Request, gateway string) (*corev1.Pod, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if request.EnvironmentInput.CoreImage != cfg.CoreImage || request.EnvironmentInput.EnvironmentImage != cfg.EnvironmentImage || request.EnvironmentInput.Platform != cfg.Platform || request.TerminationGraceSeconds != int(cfg.TerminationGraceSeconds) {
		return nil, errors.New("request deployment selection mismatch")
	}
	root, err := wire.StorageRoot(request)
	if err != nil {
		return nil, err
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return nil, err
	}
	volumes := []corev1.Volume{
		{Name: "runtime-core", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "runtime-control", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "runtime-tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "runtime-home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "runtime-request", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.SecretName, DefaultMode: ptr.To[int32](0400)}}},
		{Name: "runtime-tools", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.ToolsClaim, ReadOnly: true}}},
		{Name: "runtime-workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.WorkspaceClaim}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "runtime-core", MountPath: wire.CoreRoot, ReadOnly: true},
		{Name: "runtime-control", MountPath: wire.ControlRoot},
		{Name: "runtime-request", MountPath: filepath.Dir(wire.RequestPath), ReadOnly: true},
		{Name: "runtime-tools", MountPath: wire.EnvironmentRoot, SubPath: "generations/" + cfg.EnvironmentID, ReadOnly: true},
		{Name: "runtime-workspace", MountPath: root, SubPath: request.WorkerSubPath},
		{Name: "runtime-home", MountPath: wire.Home},
		{Name: "runtime-tmp", MountPath: "/tmp"},
	}
	if session != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "runtime-workspace", MountPath: session, SubPath: ".multica-runtime/sessions/" + filepath.Base(session)})
	}
	if request.Provider == "codex" {
		mounts = append(mounts, corev1.VolumeMount{Name: "runtime-workspace", MountPath: wire.ControlRoot + "/assigned-skills", SubPath: request.WorkerSubPath + "/codex-skills", ReadOnly: true})
	}
	volumes = append(volumes, cfg.ConfigVolumes...)
	mounts = append(mounts, cfg.ConfigMounts...)
	env := append([]corev1.EnvVar{}, cfg.ConfigEnv...)
	for i := range env {
		env[i].Name = "MULTICA_OPERATOR_" + env[i].Name
	}
	from := append([]corev1.EnvFromSource{}, cfg.ConfigEnvFrom...)
	for i := range from {
		from[i].Prefix = "MULTICA_OPERATOR_" + from[i].Prefix
	}
	env = append(env, corev1.EnvVar{Name: "HOME", Value: wire.Home}, corev1.EnvVar{Name: "TMPDIR", Value: "/tmp"}, corev1.EnvVar{Name: "MULTICA_TASK_ID", Value: ref.TaskID}, corev1.EnvVar{Name: "MULTICA_REQUEST_SECRET_NAME", Value: ref.SecretName}, corev1.EnvVar{Name: "MULTICA_REQUEST_DIGEST", Value: ref.RequestDigest}, corev1.EnvVar{Name: "MULTICA_DAEMON_PROXY_URL", Value: gateway}, corev1.EnvVar{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	env = append(env, corev1.EnvVar{Name: "MULTICA_ENVIRONMENT_ID", Value: ref.EnvironmentID}, corev1.EnvVar{Name: "MULTICA_ATTEMPT_ID", Value: ref.AttemptID})
	security := &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	selector := maps.Clone(cfg.NodeSelector)
	if selector == nil {
		selector = map[string]string{}
	}
	selector["kubernetes.io/os"] = "linux"
	selector["kubernetes.io/arch"] = strings.TrimPrefix(cfg.Platform, "linux/")
	var affinity *corev1.Affinity
	if cfg.SingleNodeName != "" {
		affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{cfg.SingleNodeName}}}}}}}}
	}
	port, _ := strconv.Atoi(wire.Value(request.Env, "MULTICA_DAEMON_PORT"))
	if port < 1 || port > 65535 {
		return nil, errors.New("official daemon port required")
	}
	pod := &corev1.Pod{ObjectMeta: ref.metadata(ref.PodName), Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false), ServiceAccountName: cfg.ServiceAccount, ActiveDeadlineSeconds: ptr.To(cfg.TaskDeadlineSeconds), TerminationGracePeriodSeconds: ptr.To(cfg.TerminationGraceSeconds + 5), NodeSelector: selector, Affinity: affinity, Tolerations: cfg.Tolerations, ImagePullSecrets: cfg.ImagePullSecrets, SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), FSGroup: ptr.To[int64](65532), FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}, Volumes: volumes,
		InitContainers: []corev1.Container{{Name: "core", Image: cfg.CoreImage, ImagePullPolicy: cfg.CorePullPolicy, Command: []string{"/artifact/runtime", "materialize"}, Env: []corev1.EnvVar{{Name: "MULTICA_PLATFORM", Value: cfg.Platform}}, VolumeMounts: []corev1.VolumeMount{{Name: "runtime-core", MountPath: wire.CoreRoot}}, SecurityContext: security}},
		Containers:     []corev1.Container{{Name: "worker", Image: cfg.EnvironmentImage, ImagePullPolicy: cfg.EnvironmentPullPolicy, Command: []string{wire.CoreRoot + "/runtime", "worker", "serve"}, Env: env, EnvFrom: from, VolumeMounts: mounts, Resources: cfg.Resources, SecurityContext: security, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{wire.CoreRoot + "/runtime", "worker", "ready"}}}, PeriodSeconds: 2, FailureThreshold: 30}, Ports: []corev1.ContainerPort{{Name: "daemon", ContainerPort: int32(port)}}}},
	}}
	homeCommand := []string{wire.CoreRoot + "/runtime", "home", "layout"}
	for _, mount := range cfg.ConfigMounts {
		if parent := filepath.Dir(mount.MountPath); parent != wire.Home {
			homeCommand = append(homeCommand, "--home-parent="+parent)
		}
	}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{Name: "home-layout", Image: cfg.EnvironmentImage, ImagePullPolicy: cfg.EnvironmentPullPolicy, Command: homeCommand, SecurityContext: security, VolumeMounts: []corev1.VolumeMount{{Name: "runtime-core", MountPath: wire.CoreRoot, ReadOnly: true}, {Name: "runtime-home", MountPath: wire.Home}}})
	return pod, nil
}
