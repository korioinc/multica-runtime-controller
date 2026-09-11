package kubernetes

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestContainerOverrideCannotAcquireImmutableImageAuthority(t *testing.T) {
	security := &corev1.SecurityContext{ReadOnlyRootFilesystem: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	spec := corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532)}, InitContainers: []corev1.Container{{Name: "init", SecurityContext: security.DeepCopy()}}, Containers: []corev1.Container{{Name: "main", SecurityContext: security.DeepCopy()}}}
	if err := immutablePodSecurity(spec); err != nil {
		t.Fatal("inherited non-root image could not be admitted", err)
	}
	for _, init := range []bool{false, true} {
		for name, change := range map[string]func(*corev1.SecurityContext){
			"root override":        func(s *corev1.SecurityContext) { s.RunAsUser = ptr.To[int64](0) },
			"writable image":       func(s *corev1.SecurityContext) { s.ReadOnlyRootFilesystem = ptr.To(false) },
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
				if err := immutablePodSecurity(*modified); err == nil {
					t.Fatal("container override acquired immutable image authority")
				}
			})
		}
	}
}

func TestAlteredWorkerCannotAcquireExecutionAuthority(t *testing.T) {
	for name, change := range map[string]func(*corev1.Pod){
		"writable image":    func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem = ptr.To(false) },
		"image replaced":    func(p *corev1.Pod) { p.Spec.Containers[0].Image = "registry.example/other:latest" },
		"platform replaced": func(p *corev1.Pod) { p.Spec.NodeSelector["kubernetes.io/arch"] = "arm64" },
		"image shadowed": func(p *corev1.Pod) {
			p.Spec.InitContainers[0].VolumeMounts = append(p.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{Name: "runtime-private", MountPath: "/opt/multica/controller", ReadOnly: true})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, cfg := currentTaskFixture(t)
			var err error
			ref.SecretUID, err = client.CreateSecret(ctx, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			ref.PodUID, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080")
			if err != nil {
				t.Fatal(err)
			}
			pod, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Spec.NodeName = cfg.SingleNodeName
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", Ready: true}}
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err = client.authorizeExecution(ctx, ref); err != nil {
				t.Fatal("original worker could not be authorized", err)
			}
			change(pod)
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err = client.authorizeExecution(ctx, ref); err == nil {
				t.Fatal("modified worker acquired execution authority")
			}
		})
	}
}
