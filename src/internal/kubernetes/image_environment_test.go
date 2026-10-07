package kubernetes

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestContainerOverrideCannotAcquireImmutableImageAuthority(t *testing.T) {
	security := &corev1.SecurityContext{ReadOnlyRootFilesystem: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	spec := corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532)}, InitContainers: []corev1.Container{{Name: "init", SecurityContext: security.DeepCopy()}}, Containers: []corev1.Container{{Name: "main", SecurityContext: security.DeepCopy()}}}
	if err := unprivilegedPodSecurity(spec); err != nil {
		t.Fatal("inherited non-root image could not be admitted", err)
	}
	for _, init := range []bool{false, true} {
		for name, change := range map[string]func(*corev1.SecurityContext){
			"root override":        func(s *corev1.SecurityContext) { s.RunAsUser = ptr.To[int64](0) },
			"privileged process":   func(s *corev1.SecurityContext) { s.Privileged = ptr.To(true) },
			"privilege escalation": func(s *corev1.SecurityContext) { s.AllowPrivilegeEscalation = ptr.To(true) },
			"mount capability":     func(s *corev1.SecurityContext) { s.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"} },
		} {
			t.Run(name, func(t *testing.T) {
				modified := spec.DeepCopy()
				container := &modified.Containers[0]
				if init {
					container = &modified.InitContainers[0]
				}
				change(container.SecurityContext)
				if err := unprivilegedPodSecurity(*modified); err == nil {
					t.Fatal("container override acquired immutable image authority")
				}
			})
		}
	}
}

func TestAlteredWorkerCannotAcquireExecutionAuthority(t *testing.T) {
	for name, change := range map[string]func(*corev1.Pod){
		"mount authority": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		},
		"another task mount": func(p *corev1.Pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].NFS != nil {
					p.Spec.Volumes[i].NFS.Path = "/workspace/other/task"
				}
			}
		},
		"image replaced":    func(p *corev1.Pod) { p.Spec.Containers[0].Image = "registry.example/other:latest" },
		"platform replaced": func(p *corev1.Pod) { p.Spec.NodeSelector["kubernetes.io/arch"] = "arm64" },
		"worker seccomp replaced": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("unauthorized-profile.json")}
		},
		"init seccomp replaced": func(p *corev1.Pod) {
			p.Spec.InitContainers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("unauthorized-profile.json")}
		},
		"image shadowed": func(p *corev1.Pod) {
			p.Spec.InitContainers[0].VolumeMounts = append(p.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{Name: "runtime-private", MountPath: "/opt/multica/controller", ReadOnly: true})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, cfg := currentTaskFixture(t)
			var err error
			ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			ref.PodUID, err = ensureTaskPod(ctx, client, cfg, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			pod, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Spec.NodeName = "independent-worker-node"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", Ready: true, ImageID: ref.RuntimeRef.Image}}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: ref.RuntimeRef.Image}}
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = client.Authorize(ctx, ref); err != nil {
				t.Fatal("original worker could not be authorized", err)
			}
			change(pod)
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = client.Authorize(ctx, ref); err == nil {
				t.Fatal("modified worker acquired execution authority")
			}
		})
	}
}

func TestNFSPrivilegesDoNotAuthorizeAnotherRole(t *testing.T) {
	image := "registry.example/nfs@sha256:" + strings.Repeat("a", 64)
	cfg := NFS{Image: image}
	app := &corev1.SecurityContext{ReadOnlyRootFilesystem: ptr.To(false), AllowPrivilegeEscalation: ptr.To(false), RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	initSecurity := app.DeepCopy()
	initSecurity.ReadOnlyRootFilesystem = ptr.To(true)
	storage := &corev1.SecurityContext{RunAsUser: ptr.To[int64](0), RunAsGroup: ptr.To[int64](0), RunAsNonRoot: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(false), AllowPrivilegeEscalation: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"SYS_ADMIN", "DAC_READ_SEARCH", "DAC_OVERRIDE", "SYS_RESOURCE", "CHOWN", "FOWNER", "SETUID", "SETGID"}}}
	spec := corev1.PodSpec{InitContainers: []corev1.Container{{Name: "home-layout", SecurityContext: initSecurity}, {Name: "nfs-layout", Image: image, Command: nfsLayoutCommand, SecurityContext: initSecurity.DeepCopy()}}, Containers: []corev1.Container{{Name: "controller", SecurityContext: app}, {Name: "nfs", Image: image, Command: []string{"/usr/bin/ganesha.nfsd"}, Args: []string{"-F", "-L", "/proc/self/fd/2", "-p", "/run/ganesha/ganesha.pid", "-f", "/etc/ganesha/ganesha.conf"}, SecurityContext: storage, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: nfsReadyCommand}}}}}}
	if err := controllerSecurity(spec, cfg); err != nil {
		t.Fatal("separate storage role was rejected", err)
	}
	spec.Containers = append(spec.Containers, corev1.Container{Name: "cache", SecurityContext: app.DeepCopy()}, corev1.Container{Name: "metrics", SecurityContext: app.DeepCopy()})
	if err := controllerSecurity(spec, cfg); err != nil {
		t.Fatal("unprivileged sidecars prevented controller admission", err)
	}
	for name, change := range map[string]func(*corev1.PodSpec){
		"sidecar mount privilege": func(p *corev1.PodSpec) {
			p.Containers[2].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		},
		"sidecar root authority": func(p *corev1.PodSpec) { p.Containers[2].SecurityContext.RunAsUser = ptr.To[int64](0) },
		"controller mount privilege": func(p *corev1.PodSpec) {
			p.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		},
		"init root authority": func(p *corev1.PodSpec) { p.InitContainers[0].SecurityContext.RunAsUser = ptr.To[int64](0) },
		"initializer mount privilege": func(p *corev1.PodSpec) {
			p.InitContainers[1].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		},
		"unverified export readiness":  func(p *corev1.PodSpec) { p.Containers[1].ReadinessProbe = nil },
		"extra storage process":        func(p *corev1.PodSpec) { p.Containers = append(p.Containers, p.Containers[1]) },
		"arbitrary privileged command": func(p *corev1.PodSpec) { p.Containers[1].Command = []string{"/bin/sh"} },
		"unadmitted storage image":     func(p *corev1.PodSpec) { p.Containers[1].Image = "registry.example/untrusted:latest" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := spec.DeepCopy()
			change(changed)
			if err := controllerSecurity(*changed, cfg); err == nil {
				t.Fatal("storage privileges widened application authority")
			}
		})
	}
}
