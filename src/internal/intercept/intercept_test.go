package intercept

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestLoadConfig(t *testing.T) {
	values := map[string]string{
		"POD_NAMESPACE":                      "multica",
		"POD_NAME":                           "controller-abc",
		"MULTICA_RUNTIME_IMAGE":              "registry.example/runtime@sha256:abc",
		"MULTICA_RUNTIME_IMAGE_PULL_POLICY":  "Always",
		"MULTICA_DAEMON_PROXY_URL":           "http://runtime-controller-daemon-proxy:19515",
		"MULTICA_WORKER_SERVICE_ACCOUNT":     "multica-task-worker",
		"MULTICA_WORKSPACE_PVC_NAME":         "multica-runtime-workspace",
		"MULTICA_WORKER_EXTRA_VOLUMES":       `[{"name":"codex-home","secret":{"secretName":"codex"}}]`,
		"MULTICA_WORKER_EXTRA_VOLUME_MOUNTS": `[{"name":"codex-home","mountPath":"/home/multica/agents/.codex/config.toml","subPath":"config.toml","readOnly":true},{"name":"codex-home","mountPath":"/home/multica/agents/.codex/auth.json","subPath":"auth.json","readOnly":true}]`,
		"MULTICA_WORKER_CPU_REQUEST":         "500m",
		"MULTICA_WORKER_CPU_LIMIT":           "2",
		"MULTICA_WORKER_MEMORY_REQUEST":      "1Gi",
		"MULTICA_WORKER_MEMORY_LIMIT":        "4Gi",
		"MULTICA_TASK_DEADLINE":              "6h",
	}
	cfg, podName, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if podName != "controller-abc" || cfg.Namespace != "multica" || cfg.DaemonProxyURL != "http://runtime-controller-daemon-proxy:19515" || cfg.Deadline != 6*time.Hour {
		t.Fatalf("unexpected launcher config: pod=%q cfg=%+v", podName, cfg)
	}
	if cfg.Resources.Requests.Cpu().String() != "500m" || cfg.Resources.Limits.Memory().String() != "4Gi" {
		t.Fatalf("unexpected resources: %+v", cfg.Resources)
	}
	if len(cfg.ExtraVolumeMounts) != 2 || cfg.ExtraVolumeMounts[0].MountPath != "/home/multica/agents/.codex/config.toml" || cfg.ExtraVolumeMounts[0].SubPath != "config.toml" || cfg.ExtraVolumeMounts[1].MountPath != "/home/multica/agents/.codex/auth.json" || cfg.ExtraVolumeMounts[1].SubPath != "auth.json" {
		t.Fatalf("unexpected Codex config mounts: %+v", cfg.ExtraVolumeMounts)
	}
}

func TestLoadConfigRejectsMissingControllerIdentity(t *testing.T) {
	if _, _, err := loadConfig(func(string) string { return "" }); err == nil {
		t.Fatal("expected missing launcher configuration to fail")
	}
}

func TestLoadConfigUsesPersistedLauncherEnvironment(t *testing.T) {
	values := map[string]string{
		"POD_NAMESPACE":                      "multica",
		"POD_NAME":                           "controller-abc",
		"MULTICA_RUNTIME_IMAGE":              "registry.example/runtime@sha256:abc",
		"MULTICA_RUNTIME_IMAGE_PULL_POLICY":  "Always",
		"MULTICA_DAEMON_PROXY_URL":           "http://runtime-controller-daemon-proxy:19515",
		"MULTICA_WORKER_SERVICE_ACCOUNT":     "multica-task-worker",
		"MULTICA_WORKSPACE_PVC_NAME":         "multica-runtime-workspace",
		"MULTICA_WORKER_EXTRA_VOLUMES":       `[{"name":"codex-home","secret":{"secretName":"codex"}}]`,
		"MULTICA_WORKER_EXTRA_VOLUME_MOUNTS": `[{"name":"codex-home","mountPath":"/home/multica/agents/.codex/config.toml","subPath":"config.toml","readOnly":true},{"name":"codex-home","mountPath":"/home/multica/agents/.codex/auth.json","subPath":"auth.json","readOnly":true}]`,
		"MULTICA_WORKER_CPU_REQUEST":         "500m",
		"MULTICA_WORKER_CPU_LIMIT":           "2",
		"MULTICA_WORKER_MEMORY_REQUEST":      "1Gi",
		"MULTICA_WORKER_MEMORY_LIMIT":        "4Gi",
		"MULTICA_TASK_DEADLINE":              "6h",
	}
	raw, err := json.Marshal(map[string]any{"values": values})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "launcher.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_LAUNCHER_CONFIG_FILE", path)
	for key := range values {
		t.Setenv(key, "")
	}

	cfg, podName, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if podName != "controller-abc" || cfg.Namespace != "multica" || cfg.Image != "registry.example/runtime@sha256:abc" {
		t.Fatalf("unexpected persisted launcher config: pod=%q cfg=%+v", podName, cfg)
	}
}

func TestPersistLauncherConfigExcludesControllerCredentials(t *testing.T) {
	values := map[string]string{
		"POD_NAMESPACE":                     "multica",
		"POD_NAME":                          "controller-abc",
		"MULTICA_RUNTIME_IMAGE":             "registry.example/runtime@sha256:abc",
		"MULTICA_RUNTIME_IMAGE_PULL_POLICY": "Always",
		"MULTICA_DAEMON_PROXY_URL":          "http://runtime-controller-daemon-proxy:19515",
		"MULTICA_WORKER_SERVICE_ACCOUNT":    "multica-task-worker",
		"MULTICA_WORKSPACE_PVC_NAME":        "multica-runtime-workspace",
		"MULTICA_WORKER_CPU_REQUEST":        "500m",
		"MULTICA_WORKER_CPU_LIMIT":          "2",
		"MULTICA_WORKER_MEMORY_REQUEST":     "1Gi",
		"MULTICA_WORKER_MEMORY_LIMIT":       "4Gi",
		"MULTICA_TASK_DEADLINE":             "6h",
		"MULTICA_CONTROLLER_TOKEN_FILE":     "/sensitive/controller/token",
	}
	path := filepath.Join(t.TempDir(), "launcher.json")
	if err := persistLauncherConfig(path, func(key string) string { return values[key] }); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("/sensitive/controller/token")) || bytes.Contains(raw, []byte("MULTICA_CONTROLLER_TOKEN_FILE")) {
		t.Fatal("launcher snapshot contains controller credentials")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("launcher snapshot permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestTaskResourcesForProviderExecution(t *testing.T) {
	taskID := "11111111-2222-3333-4444-555555555555"
	taskCodexHome := "/workspace/teams/a/tasks/" + taskID + "/.codex"
	request, err := PrepareRequest("codex", []string{"app-server"}, []string{
		"MULTICA_TASK_ID=" + taskID,
		"MULTICA_TOKEN=mat_task",
		"CODEX_HOME=" + taskCodexHome,
		"MULTICA_CONTROLLER_TOKEN_FILE=/var/run/multica/controller/token",
		"KUBERNETES_SERVICE_HOST=10.43.0.1",
		"TMPDIR=/tmp/controller-task",
	}, "/workspace/teams/a/tasks/"+taskID)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := environmentEntry(request.Env, "CODEX_HOME"); ok {
		t.Fatalf("Codex request retained CODEX_HOME=%q instead of using $HOME/.codex", got)
	}
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "controller", UID: "owner-uid", Controller: ptr.To(true)}
	cfg := Config{
		Namespace:          "multica",
		Image:              "registry.example/runtime@sha256:abc",
		ImagePullPolicy:    corev1.PullIfNotPresent,
		DaemonProxyURL:     "http://runtime-controller-daemon-proxy:19515",
		ServiceAccountName: "multica-task-worker",
		WorkspacePVCName:   "multica-runtime-workspace",
		ExtraVolumes: []corev1.Volume{{
			Name: "codex-home",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: "multica-runtime-codex-home",
			}},
		}},
		ExtraVolumeMounts: []corev1.VolumeMount{
			{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "config.toml", ReadOnly: true},
			{Name: "codex-home", MountPath: "/home/multica/agents/.codex/auth.json", SubPath: "auth.json", ReadOnly: true},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
		},
		Deadline: 6 * time.Hour,
	}
	secret, err := requestSecret(cfg, taskID, request, owner)
	if err != nil {
		t.Fatal(err)
	}
	secret.Name = secret.GenerateName + "abcde"
	pod, err := taskPod(cfg, taskID, secret.Name, owner)
	if err != nil {
		t.Fatal(err)
	}

	const wantGenerateName = "task-11111111-"
	if secret.GenerateName != wantGenerateName || pod.Name != "" || pod.GenerateName != wantGenerateName {
		t.Fatalf("resource names = Secret prefix %q, Pod name %q, Pod prefix %q", secret.GenerateName, pod.Name, pod.GenerateName)
	}
	if got := requestSecretNameFromPod(pod); got != secret.Name {
		t.Fatalf("task Pod request Secret = %q, want %q", got, secret.Name)
	}
	if got := podEnvironmentValue(pod.Spec.Containers[0].Env, "MULTICA_REQUEST_SECRET_NAME"); got != secret.Name {
		t.Fatalf("task Pod request Secret environment = %q, want %q", got, secret.Name)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("task pod must not mount a Kubernetes service account token")
	}
	if pod.Spec.ServiceAccountName != "multica-task-worker" || pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds != 21600 {
		t.Fatalf("unexpected pod execution policy: %+v", pod.Spec)
	}
	if len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Args) != 1 || pod.Spec.Containers[0].Args[0] != "hold" {
		t.Fatalf("unexpected worker container: %+v", pod.Spec.Containers)
	}
	if len(pod.Spec.InitContainers) != 2 {
		t.Fatalf("task Pod init containers = %+v, want tmp-permissions and provider-home initializers", pod.Spec.InitContainers)
	}
	tmpInit := pod.Spec.InitContainers[0]
	if tmpInit.Name != "tmp-permissions" || tmpInit.Image != cfg.Image || tmpInit.ImagePullPolicy != cfg.ImagePullPolicy {
		t.Fatalf("unexpected tmp permissions initializer identity: %+v", tmpInit)
	}
	if !reflect.DeepEqual(tmpInit.Command, []string{"/bin/sh", "-ec"}) || !reflect.DeepEqual(tmpInit.Args, []string{"chmod 1777 /tmp"}) {
		t.Fatalf("tmp permissions initializer command = %q %q", tmpInit.Command, tmpInit.Args)
	}
	if !reflect.DeepEqual(tmpInit.VolumeMounts, []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}}) {
		t.Fatalf("tmp permissions initializer mounts = %+v", tmpInit.VolumeMounts)
	}
	if tmpInit.SecurityContext == nil || tmpInit.SecurityContext.RunAsNonRoot == nil || *tmpInit.SecurityContext.RunAsNonRoot ||
		tmpInit.SecurityContext.RunAsUser == nil || *tmpInit.SecurityContext.RunAsUser != 0 ||
		tmpInit.SecurityContext.RunAsGroup == nil || *tmpInit.SecurityContext.RunAsGroup != 0 ||
		tmpInit.SecurityContext.AllowPrivilegeEscalation == nil || *tmpInit.SecurityContext.AllowPrivilegeEscalation ||
		tmpInit.SecurityContext.ReadOnlyRootFilesystem == nil || !*tmpInit.SecurityContext.ReadOnlyRootFilesystem ||
		tmpInit.SecurityContext.Capabilities == nil || !reflect.DeepEqual(tmpInit.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("tmp permissions initializer security context = %+v", tmpInit.SecurityContext)
	}
	init := pod.Spec.InitContainers[1]
	if init.Name != "provider-home-init" || init.Image != cfg.Image || init.ImagePullPolicy != cfg.ImagePullPolicy {
		t.Fatalf("unexpected provider-home initializer identity: %+v", init)
	}
	wantInitCommand := []string{"/bin/sh", "-ec"}
	wantInitArgs := []string{
		"install -d -m 0700 " + agentHomeVolumePath + "/" + agentHomeSubPath +
			" && install -d -m 0700 " + agentHomeVolumePath + "/" + agentHomeSubPath + "/.codex",
	}
	if !reflect.DeepEqual(init.Command, wantInitCommand) || !reflect.DeepEqual(init.Args, wantInitArgs) {
		t.Fatalf("provider-home initializer command = %q %q", init.Command, init.Args)
	}
	wantInitMounts := []corev1.VolumeMount{
		{Name: "agent-home", MountPath: agentHomeVolumePath},
		{Name: "tmp", MountPath: "/tmp"},
	}
	if !reflect.DeepEqual(init.VolumeMounts, wantInitMounts) {
		t.Fatalf("provider-home initializer mounts = %+v, want %+v", init.VolumeMounts, wantInitMounts)
	}
	if len(init.Env) != 0 || len(init.EnvFrom) != 0 {
		t.Fatalf("provider-home initializer received environment configuration: env=%+v envFrom=%+v", init.Env, init.EnvFrom)
	}
	if !reflect.DeepEqual(init.Resources, cfg.Resources) {
		t.Fatalf("provider-home initializer resources = %+v, want %+v", init.Resources, cfg.Resources)
	}
	if init.SecurityContext == nil || init.SecurityContext.AllowPrivilegeEscalation == nil || *init.SecurityContext.AllowPrivilegeEscalation ||
		init.SecurityContext.ReadOnlyRootFilesystem == nil || !*init.SecurityContext.ReadOnlyRootFilesystem ||
		init.SecurityContext.Capabilities == nil || !reflect.DeepEqual(init.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("provider-home initializer security context = %+v", init.SecurityContext)
	}
	podSecurity := pod.Spec.SecurityContext
	if podSecurity == nil || podSecurity.RunAsNonRoot == nil || !*podSecurity.RunAsNonRoot ||
		podSecurity.RunAsUser == nil || *podSecurity.RunAsUser != 65532 ||
		podSecurity.RunAsGroup == nil || *podSecurity.RunAsGroup != 65532 ||
		podSecurity.FSGroup == nil || *podSecurity.FSGroup != 65532 {
		t.Fatalf("task Pod security context = %+v", podSecurity)
	}
	if got := podEnvironmentValue(pod.Spec.Containers[0].Env, "MULTICA_DAEMON_PROXY_URL"); got != "http://runtime-controller-daemon-proxy:19515" {
		t.Fatalf("task daemon proxy URL = %q", got)
	}
	if got := podEnvironmentValue(pod.Spec.Containers[0].Env, "HOME"); got != agentHome {
		t.Fatalf("task Pod HOME = %q, want %q", got, agentHome)
	}
	homeMount, ok := volumeMount(pod.Spec.Containers[0].VolumeMounts, agentHome)
	if !ok || homeMount.Name != "agent-home" || homeMount.SubPath != agentHomeSubPath {
		t.Fatalf("task Pod agent HOME mount = %+v, present=%t", homeMount, ok)
	}
	tmpMount, ok := volumeMount(pod.Spec.Containers[0].VolumeMounts, "/tmp")
	if !ok || tmpMount.Name != "tmp" || tmpMount.SubPath != "" {
		t.Fatalf("task Pod tmp mount = %+v, present=%t", tmpMount, ok)
	}
	probe := pod.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.Exec == nil || len(probe.Exec.Command) != 3 || probe.Exec.Command[2] != "exec 3<>/dev/tcp/127.0.0.1/19514" {
		t.Fatalf("task daemon proxy readiness probe = %+v", probe)
	}
	if got := pod.Labels[TaskIDLabel]; got != taskID {
		t.Fatalf("task label = %q, want %q", got, taskID)
	}
	for path, subPath := range map[string]string{
		"/home/multica/agents/.codex/config.toml": "config.toml",
		"/home/multica/agents/.codex/auth.json":   "auth.json",
	} {
		mount, ok := volumeMount(pod.Spec.Containers[0].VolumeMounts, path)
		if !ok || mount.Name != "codex-home" || mount.SubPath != subPath || !mount.ReadOnly {
			t.Fatalf("Codex file mount at %q = %+v, present=%t", path, mount, ok)
		}
	}

	var stored Request
	if err := json.Unmarshal(secret.Data[RequestKey], &stored); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if stored.Provider != "codex" || stored.WorkDir != request.WorkDir {
		t.Fatalf("stored request = %+v", stored)
	}
	for _, entry := range stored.Env {
		if entry == "MULTICA_CONTROLLER_TOKEN_FILE=/var/run/multica/controller/token" || entry == "KUBERNETES_SERVICE_HOST=10.43.0.1" {
			t.Fatalf("controller-only environment leaked to task Pod: %q", entry)
		}
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != "owner-uid" || len(pod.OwnerReferences) != 1 {
		t.Fatal("task resources must be owned by the controller Pod")
	}
}

func TestObjectsForPiProviderExecution(t *testing.T) {
	taskID := "11111111-2222-3333-4444-555555555555"
	request, err := PrepareRequest("pi", []string{"--mode", "json"}, []string{
		"MULTICA_TASK_ID=" + taskID,
		"PI_CODING_AGENT_DIR=/tmp/wrong-pi-home",
	}, "/workspace/teams/a/tasks/"+taskID)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := environmentEntry(request.Env, "PI_CODING_AGENT_DIR"); ok {
		t.Fatalf("Pi request retained PI_CODING_AGENT_DIR=%q instead of using $HOME/.pi/agent", got)
	}
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "controller", UID: "owner-uid", Controller: ptr.To(true)}
	cfg := Config{
		Namespace:          "multica",
		Image:              "registry.example/runtime@sha256:abc",
		ImagePullPolicy:    corev1.PullIfNotPresent,
		DaemonProxyURL:     "http://runtime-controller-daemon-proxy:19515",
		ServiceAccountName: "multica-task-worker",
		WorkspacePVCName:   "multica-runtime-workspace",
		Deadline:           6 * time.Hour,
	}
	secret, err := requestSecret(cfg, taskID, request, owner)
	if err != nil {
		t.Fatal(err)
	}
	secret.Name = secret.GenerateName + "abcde"
	pod, err := taskPod(cfg, taskID, secret.Name, owner)
	if err != nil {
		t.Fatal(err)
	}
	if got := podEnvironmentValue(pod.Spec.Containers[0].Env, "HOME"); got != agentHome {
		t.Fatalf("task Pod HOME = %q, want %q", got, agentHome)
	}
	var workspaceVolume *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == "workspace" {
			workspaceVolume = &pod.Spec.Volumes[i]
			break
		}
	}
	if workspaceVolume == nil || workspaceVolume.PersistentVolumeClaim == nil || workspaceVolume.PersistentVolumeClaim.ClaimName != "multica-runtime-workspace" {
		t.Fatalf("workspace volume is not backed by the configured PVC: %+v", workspaceVolume)
	}
	for _, want := range []struct {
		mountPath string
		subPath   string
	}{
		{mountPath: piHomeMountPath, subPath: piHomeSubPath},
		{mountPath: piSessionsMountPath, subPath: piSessionsSubPath},
	} {
		mount, ok := volumeMount(pod.Spec.Containers[0].VolumeMounts, want.mountPath)
		if !ok || mount.Name != "workspace" || mount.SubPath != want.subPath || mount.ReadOnly {
			t.Fatalf("workspace PVC mount at %q = %+v, present=%t", want.mountPath, mount, ok)
		}
	}
	if path, err := ProviderExecutable("pi"); err != nil || path != "/opt/multica/providers/bin/pi" {
		t.Fatalf("Pi executable = %q, %v", path, err)
	}
}

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
		{name: "duplicate tuple", volumes: projected, mounts: append(append([]corev1.VolumeMount{}, valid[0]), valid[0])},
		{name: "duplicate target", volumes: projected, mounts: []corev1.VolumeMount{{Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "config.toml", ReadOnly: true}, {Name: "codex-home", MountPath: "/home/multica/agents/.codex/config.toml", SubPath: "auth.json", ReadOnly: true}}},
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

func podEnvironmentValue(environ []corev1.EnvVar, name string) string {
	for _, entry := range environ {
		if entry.Name == name {
			return entry.Value
		}
	}
	return ""
}

func environmentValue(environ []string, name string) string {
	value, _ := environmentEntry(environ, name)
	return value
}

func environmentEntry(environ []string, name string) (string, bool) {
	prefix := name + "="
	for _, entry := range environ {
		if value, ok := strings.CutPrefix(entry, prefix); ok {
			return value, true
		}
	}
	return "", false
}

func volumeMount(mounts []corev1.VolumeMount, path string) (corev1.VolumeMount, bool) {
	for _, mount := range mounts {
		if mount.MountPath == path {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
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

func TestTaskResourceGenerateNameUsesEightCanonicalTaskUUIDCharacters(t *testing.T) {
	if _, err := taskGenerateNameFor("claim-1"); err == nil {
		t.Fatal("expected invalid task ID to fail")
	}
	generateName, err := taskGenerateNameFor("AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE")
	if err != nil {
		t.Fatal(err)
	}
	if generateName != "task-aaaaaaaa-" {
		t.Fatalf("task resource generateName = %q", generateName)
	}
}
