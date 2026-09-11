package kubernetes

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	Namespace     string           `json:"namespace"`
	Owner         Owner            `json:"owner"`
	TaskID        string           `json:"taskID"`
	StorageID     string           `json:"storageID"`
	AttemptID     string           `json:"attemptID"`
	PodName       string           `json:"podName"`
	SecretName    string           `json:"secretName"`
	PodUID        string           `json:"podUID,omitempty"`
	SecretUID     string           `json:"secretUID,omitempty"`
	RequestDigest string           `json:"requestDigest"`
	RuntimeRef    runtimeimage.Ref `json:"runtimeRef"`
	PodDigest     string           `json:"podDigest"`
	FixedNode     string           `json:"fixedNode,omitempty"`
}

func (r Reference) metadata(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: map[string]string{managedLabel: managedValue, taskLabel: r.TaskID, storageLabel: r.StorageID, attemptLabel: r.AttemptID}, Annotations: map[string]string{digestAnnotation: r.RequestDigest}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: r.Owner.Name, UID: types.UID(r.Owner.UID), Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}}
}
func secretObject(ref Reference, request wire.Request) (*corev1.Secret, error) {
	if request.TaskID != ref.TaskID || request.AttemptID != ref.AttemptID || request.WorkerSubPath != ".multica-runtime/workers/"+ref.StorageID || !request.RuntimeRef.Equal(ref.RuntimeRef) {
		return nil, errors.New("request differs from its task resource authority")
	}
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
	if !request.RuntimeRef.Equal(ref.RuntimeRef) || request.RuntimeRef.Platform != cfg.Platform || request.TerminationGraceSeconds != int(cfg.TerminationGraceSeconds) {
		return nil, errors.New("request deployment selection mismatch")
	}
	if _, err := secretObject(ref, request); err != nil {
		return nil, err
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
		{Name: "runtime-private", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "runtime-shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr.To(resource.MustParse("512Mi"))}}},
		{Name: "runtime-request", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.SecretName, DefaultMode: ptr.To[int32](0400)}}},
		{Name: "runtime-workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.WorkspaceClaim}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "runtime-private", MountPath: wire.ControlRoot, SubPath: "run"},
		{Name: "runtime-request", MountPath: filepath.Dir(wire.RequestPath), ReadOnly: true},
		{Name: "runtime-workspace", MountPath: root, SubPath: request.WorkerSubPath},
		{Name: "runtime-private", MountPath: wire.Home, SubPath: "agents"},
		{Name: "runtime-private", MountPath: "/tmp", SubPath: "tmp"},
		{Name: "runtime-shm", MountPath: "/dev/shm"},
	}
	if session != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "runtime-workspace", MountPath: session, SubPath: ".multica-runtime/sessions/" + filepath.Base(session)})
	}
	// The official daemon has already selected operator values for this task.
	// Their sanitized snapshot travels in request.Env. Re-resolving operator
	// Secrets here would expose controller-only App keys in the worker's initial
	// process environment and let a Secret rotation change an admitted attempt.
	var env []corev1.EnvVar
	env = append(env, corev1.EnvVar{Name: "HOME", Value: wire.Home}, corev1.EnvVar{Name: "TMPDIR", Value: "/tmp"}, corev1.EnvVar{Name: "MULTICA_TASK_ID", Value: ref.TaskID}, corev1.EnvVar{Name: "MULTICA_REQUEST_SECRET_NAME", Value: ref.SecretName}, corev1.EnvVar{Name: "MULTICA_REQUEST_DIGEST", Value: ref.RequestDigest}, corev1.EnvVar{Name: "MULTICA_DAEMON_PROXY_URL", Value: gateway}, corev1.EnvVar{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	env = append(env, corev1.EnvVar{Name: "MULTICA_RUNTIME_IMAGE_BUILD_ID", Value: ref.RuntimeRef.ImageBuildID}, corev1.EnvVar{Name: "MULTICA_ATTEMPT_ID", Value: ref.AttemptID})
	security := &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	// Keep init on the Pod's default while allowing worker tools to create
	// their own process sandboxes without changing the other restrictions.
	workerSecurity := security.DeepCopy()
	workerSecurity.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
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
		Containers: []corev1.Container{{Name: "worker", Image: ref.RuntimeRef.Image, ImagePullPolicy: cfg.ImagePullPolicy, Args: []string{"worker", "serve"}, WorkingDir: request.WorkDir, Env: env, VolumeMounts: mounts, Resources: cfg.Resources, SecurityContext: workerSecurity, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{wire.ControllerRoot + "/runtime", "worker", "ready"}}}, PeriodSeconds: 2, FailureThreshold: 30}, Ports: []corev1.ContainerPort{{Name: "daemon", ContainerPort: int32(port)}}}},
	}}
	homeCommand := []string{wire.ControllerRoot + "/runtime", "home", "layout", "--private-root=" + wire.PrivateRoot, "--request=" + wire.RequestPath}
	homeMounts := []corev1.VolumeMount{
		{Name: "runtime-private", MountPath: wire.PrivateRoot},
		{Name: "runtime-request", MountPath: filepath.Dir(wire.RequestPath), ReadOnly: true},
		{Name: "runtime-workspace", MountPath: wire.HomeArtifactPath, SubPath: request.WorkerSubPath + "/.runtime-home/" + request.AttemptID + ".tar", ReadOnly: true},
	}
	pod.Spec.InitContainers = []corev1.Container{{Name: "home-layout", Image: ref.RuntimeRef.Image, ImagePullPolicy: cfg.ImagePullPolicy, Command: homeCommand, SecurityContext: security, VolumeMounts: homeMounts}}
	return pod, nil
}
