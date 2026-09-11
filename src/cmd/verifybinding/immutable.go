package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
)

// Exercise the production admission against the installed Linux paths and the
// same live API fixture as the stale-receipt scenario. Each rejection must
// remove execution authority even when the API reports the correct digest.
func verifyImmutableAdmission(ctx context.Context, client *kubernetes.Client, pod *corev1.Pod, descriptor runtimeimage.Descriptor) error {
	original := pod.DeepCopy()
	defer func() { *pod = *original }()
	checks := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"writable rootfs", func(p *corev1.Pod) {
			value := false
			p.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem = &value
		}},
		{"root user", func(p *corev1.Pod) { value := int64(0); p.Spec.SecurityContext.RunAsUser = &value }},
		{"privileged init", func(p *corev1.Pod) { value := true; p.Spec.InitContainers[0].SecurityContext.Privileged = &value }},
		{"privilege escalation", func(p *corev1.Pod) {
			value := true
			p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = &value
		}},
	}
	checks = append(checks, struct {
		name   string
		mutate func(*corev1.Pod)
	}{"hidden init mount target", func(p *corev1.Pod) {
		p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "hidden", MountPath: "/evidence/aliases"})
		p.Spec.InitContainers[0].VolumeMounts = append(p.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{Name: "target", MountPath: "/evidence/aliases/seed", ReadOnly: true})
	}})
	paths := []string{runtimeimage.DescriptorPath, filepath.Dir(descriptor.Controller.RuntimePath), descriptor.Daemon.Path}
	for _, provider := range descriptor.Providers {
		paths = append(paths, provider.Path)
	}
	if descriptor.HomeSeed != "" {
		paths = append(paths, descriptor.HomeSeed, filepath.Join(descriptor.HomeSeed, "mounted-child"))
	}
	for _, directory := range descriptor.BinDirs {
		paths = append(paths, directory, filepath.Join(directory, "mounted-child"))
	}
	for _, path := range paths {
		checks = append(checks, struct {
			name   string
			mutate func(*corev1.Pod)
		}{"installed path shadow", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shadow", MountPath: path, ReadOnly: true})
		}})
	}
	for _, check := range checks {
		*pod = *original.DeepCopy()
		check.mutate(pod)
		if _, err := client.BindImage(ctx, pod.Name, string(pod.UID), "controller", pod.Spec.NodeName, descriptor.Platform, descriptor); err == nil {
			return fmt.Errorf("immutable image admission accepted %s", check.name)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}
