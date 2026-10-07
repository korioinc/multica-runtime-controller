package kubernetes

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const managedLabel = "app.kubernetes.io/managed-by"
const managedValue = "multica-runtime-controller"
const taskLabel = "multica.ai/task-id"
const storageLabel = "multica.ai/storage-id"
const attemptLabel = "multica.ai/attempt-id"
const sessionLabel = "multica.ai/worker-session-id"
const digestAnnotation = "multica.ai/request-sha256"
const terminationFinalizer = "multica.ai/termination-evidence"

type Owner struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}
type Reference struct {
	WorkerSessionID       string           `json:"workerSessionID,omitempty"`
	WorkspaceID           string           `json:"workspaceID,omitempty"`
	WorkspaceAnchorTaskID string           `json:"workspaceAnchorTaskID,omitempty"`
	Namespace             string           `json:"namespace"`
	Owner                 Owner            `json:"owner"`
	TaskID                string           `json:"taskID"`
	StorageID             string           `json:"storageID"`
	AttemptID             string           `json:"attemptID"`
	PodName               string           `json:"podName"`
	SecretName            string           `json:"secretName"`
	PodUID                string           `json:"podUID,omitempty"`
	SecretUID             string           `json:"secretUID,omitempty"`
	RequestDigest         string           `json:"requestDigest"`
	RuntimeRef            runtimeimage.Ref `json:"runtimeRef"`
	PodDigest             string           `json:"podDigest"`
	OwnerID               string           `json:"ownerID"`
	PVCName               string           `json:"pvcName"`
	PVCUID                string           `json:"pvcUID"`
	NodeID                string           `json:"nodeID,omitempty"`
	TaskRoot              string           `json:"taskRoot"`
	NFSServer             string           `json:"nfsServer"`
}

func (r Reference) metadata(name string) metav1.ObjectMeta {
	labels := map[string]string{managedLabel: managedValue, ownerLabel: r.OwnerID, storageLabel: r.StorageID}
	if r.WorkerSessionID != "" {
		labels[sessionLabel] = r.WorkerSessionID
	} else {
		labels[taskLabel], labels[attemptLabel] = r.TaskID, r.AttemptID
	}
	return metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: labels, Annotations: map[string]string{digestAnnotation: r.RequestDigest}}
}
func secretObject(ref Reference, request wire.Bootstrap) (*corev1.Secret, error) {
	if ref.WorkerSessionID != "" || request.WorkerSessionID != "" || request.TaskID != ref.TaskID || request.AttemptID != ref.AttemptID || request.StorageID != ref.StorageID || request.PVCName != ref.PVCName || request.PVCUID != ref.PVCUID || request.OwnerID != ref.OwnerID || request.TaskRoot != ref.TaskRoot || request.NFSServer != ref.NFSServer || !request.RuntimeRef.Equal(ref.RuntimeRef) {
		return nil, errors.New("request differs from its task resource authority")
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if wire.Digest(raw) != ref.RequestDigest {
		return nil, errors.New("request digest mismatch")
	}
	if _, err := wire.DecodeBootstrap(raw); err != nil {
		return nil, err
	}
	return &corev1.Secret{ObjectMeta: ref.metadata(ref.SecretName), Immutable: ptr.To(true), Type: corev1.SecretTypeOpaque, Data: map[string][]byte{wire.RequestKey: raw}}, nil
}

// PodRequest captures the creation payload authorized by the journaled digest.
// The controller persists it before creation so retries survive policy changes.
func PodRequest(cfg Config, ref Reference, request wire.Bootstrap) (*corev1.Pod, error) {
	pod, err := podObject(cfg, ref, request)
	if err != nil {
		return nil, err
	}
	if !podRequestMatches(pod, ref) {
		return nil, errors.New("Pod request differs from journaled authority")
	}
	return pod, nil
}

func podObject(cfg Config, ref Reference, request wire.Bootstrap) (*corev1.Pod, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Storage.ClaimName != ref.PVCName || cfg.Platform != ref.RuntimeRef.Platform || request.TerminationGraceSeconds != int(cfg.Worker.TerminationGraceSeconds) {
		return nil, errors.New("bootstrap deployment selection mismatch")
	}
	if _, err := secretObject(ref, request); err != nil {
		return nil, err
	}
	return workerPod(cfg, ref)
}

// workerPod is shared by legacy recovery and conversation incarnations. Their
// callers validate the corresponding immutable bootstrap before construction.
func workerPod(cfg Config, ref Reference) (*corev1.Pod, error) {
	if ref.PVCUID == "" || !validTaskMount(ref) {
		return nil, errors.New("task PVC identity required")
	}
	volumes := []corev1.Volume{
		{Name: "runtime-private", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse(cfg.Worker.TemporarySizeLimit))}}},
		{Name: "runtime-shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr.To(resource.MustParse("512Mi"))}}},
		{Name: "runtime-request", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.SecretName, DefaultMode: ptr.To[int32](0440), Items: []corev1.KeyToPath{{Key: wire.RequestKey, Path: wire.RequestKey}}}}},
		{Name: "runtime-task", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: ref.NFSServer, Path: ref.TaskRoot}}},
	}
	common := []corev1.VolumeMount{
		{Name: "runtime-private", MountPath: wire.ControlRoot, SubPath: "run"},
		{Name: "runtime-request", MountPath: filepath.Dir(wire.RequestPath), ReadOnly: true},
	}
	mounts := append(append([]corev1.VolumeMount{}, common...),
		corev1.VolumeMount{Name: "runtime-task", MountPath: ref.TaskRoot},
		corev1.VolumeMount{Name: "runtime-private", MountPath: "/tmp", SubPath: "tmp"},
		corev1.VolumeMount{Name: "runtime-shm", MountPath: "/dev/shm"})
	env := []corev1.EnvVar{
		{Name: "HOME", Value: wire.Home}, {Name: "TMPDIR", Value: "/tmp"},
		{Name: "MULTICA_PLATFORM", Value: cfg.Platform},
		{Name: "MULTICA_REQUEST_DIGEST", Value: ref.RequestDigest},
		{Name: "MULTICA_RUNTIME_IMAGE_BUILD_ID", Value: ref.RuntimeRef.ImageBuildID},
		{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
	}
	security := &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	workerSecurity := security.DeepCopy()
	workerSecurity.ReadOnlyRootFilesystem = ptr.To(false)
	workerSecurity.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
	selector := maps.Clone(cfg.NodeSelector)
	if selector == nil {
		selector = map[string]string{}
	}
	selector["kubernetes.io/os"], selector["kubernetes.io/arch"] = "linux", strings.TrimPrefix(cfg.Platform, "linux/")
	pod := &corev1.Pod{ObjectMeta: ref.metadata(ref.PodName), Spec: corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false), EnableServiceLinks: ptr.To(false), ServiceAccountName: cfg.ServiceAccount,
		// Preserve the supervisor's writer-stop grace plus its independent
		// 60-second flush/receipt budget before kubelet escalates to SIGKILL.
		ActiveDeadlineSeconds: ptr.To(cfg.Worker.PreparationTimeoutSeconds + cfg.Worker.TaskDeadlineSeconds + cfg.Worker.TerminationGraceSeconds + 30), TerminationGracePeriodSeconds: ptr.To(wire.WorkerTerminationSeconds(cfg.Worker.TerminationGraceSeconds)),
		NodeSelector: selector, Tolerations: cfg.Tolerations, ImagePullSecrets: cfg.ImagePullSecrets,
		SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), FSGroup: ptr.To[int64](65532), FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Volumes:         volumes, Containers: []corev1.Container{{Name: "worker", Image: ref.RuntimeRef.Image, ImagePullPolicy: cfg.ImagePullPolicy, Args: []string{"worker", "serve"}, WorkingDir: filepath.Join(ref.TaskRoot, "workdir"), Env: env, VolumeMounts: mounts, Resources: cfg.Worker.Resources, SecurityContext: workerSecurity, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{wire.ControllerRoot + "/runtime", "worker", "ready"}}}, PeriodSeconds: 2, FailureThreshold: 30}}},
	}}
	pod.Spec.InitContainers = []corev1.Container{{Name: "task-layout", Image: ref.RuntimeRef.Image, ImagePullPolicy: cfg.ImagePullPolicy, Command: []string{wire.ControllerRoot + "/runtime", "worker", "layout", "--private-root=" + wire.PrivateRoot, "--request=" + wire.RequestPath}, SecurityContext: security, VolumeMounts: append(common[1:], corev1.VolumeMount{Name: "runtime-private", MountPath: wire.PrivateRoot}), Env: env, Resources: cfg.Worker.Resources}}
	pod.Finalizers = []string{terminationFinalizer}
	if ref.WorkerSessionID != "" {
		// Each assignment owns its deadline. An idle lifetime never limits a
		// running turn or terminates a later assignment in the same Pod.
		pod.Spec.ActiveDeadlineSeconds = nil
	}
	return pod, nil
}
