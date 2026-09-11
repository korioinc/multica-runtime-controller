package kubernetes

import (
	"errors"
	"path/filepath"
	"slices"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
)

func admitImageEnvironment(pod *corev1.Pod, descriptor runtimeimage.Descriptor) error {
	if err := immutablePodSecurity(pod.Spec); err != nil {
		return err
	}
	// Future workers replace these image locations with task data. Check them
	// at initial admission even if the controller has no mount at that path.
	// Variable workspace and Pi-session mounts stay inside the protected roots
	// below through the request's existing confinement checks.
	mounts := []string{wire.PrivateRoot, wire.ControlRoot, filepath.Dir(wire.RequestPath), wire.HomeArtifactPath, wire.Home, wire.WorkspaceRoot, "/tmp", "/dev/shm"}
	for _, container := range append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...) {
		for _, mount := range container.VolumeMounts {
			mounts = append(mounts, mount.MountPath)
		}
		for _, device := range container.VolumeDevices {
			mounts = append(mounts, device.DevicePath)
		}
	}
	return runtimeimage.CheckImageMounts(descriptor, mounts)
}

// Kubernetes inherits UID and non-root settings from Pod security context;
// container settings override them. Rootfs and privilege controls have no Pod
// defaults, so every init and application container must state those controls.
func immutablePodSecurity(spec corev1.PodSpec) error {
	for _, container := range append(slices.Clone(spec.InitContainers), spec.Containers...) {
		var nonroot *bool
		var uid *int64
		if spec.SecurityContext != nil {
			nonroot, uid = spec.SecurityContext.RunAsNonRoot, spec.SecurityContext.RunAsUser
		}
		security := container.SecurityContext
		if security == nil {
			return errors.New("image admission requires immutable container security")
		}
		if security.RunAsNonRoot != nil {
			nonroot = security.RunAsNonRoot
		}
		if security.RunAsUser != nil {
			uid = security.RunAsUser
		}
		if uid != nil && *uid <= 0 || uid == nil && (nonroot == nil || !*nonroot) {
			return errors.New("image admission requires effective non-root execution")
		}
		if security.Privileged != nil && *security.Privileged || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem {
			return errors.New("image admission requires unprivileged read-only rootfs")
		}
		if security.Capabilities == nil || len(security.Capabilities.Add) != 0 || !slices.Contains(security.Capabilities.Drop, corev1.Capability("ALL")) {
			return errors.New("image admission requires dropping all capabilities")
		}
	}
	return nil
}
