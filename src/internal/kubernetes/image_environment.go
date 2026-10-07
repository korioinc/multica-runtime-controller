package kubernetes

import (
	"errors"
	"path/filepath"
	"slices"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
)

func admitImageEnvironment(pod *corev1.Pod, descriptor runtimeimage.Descriptor, cfg Config) error {
	if err := controllerSecurity(pod.Spec, cfg.NFS); err != nil {
		return err
	}
	// Future workers replace these image locations with task data. Check them
	// at initial admission even if the controller has no mount at that path.
	// Task volume mounts are checked before worker admission.
	mounts := []string{wire.PrivateRoot, wire.ControlRoot, filepath.Dir(wire.RequestPath), wire.Home, wire.WorkspaceRoot, "/tmp", "/dev/shm"}
	for _, container := range append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...) {
		if container.Name != "controller" && container.Name != "home-layout" {
			continue
		}
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
func unprivilegedPodSecurity(spec corev1.PodSpec) error {
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
		if security.Privileged != nil && *security.Privileged || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.ReadOnlyRootFilesystem == nil {
			return errors.New("image admission requires unprivileged execution")
		}
		if slices.ContainsFunc(spec.InitContainers, func(c corev1.Container) bool { return c.Name == container.Name }) && !*security.ReadOnlyRootFilesystem {
			return errors.New("init image filesystem must be read-only")
		}
		if security.Capabilities == nil || len(security.Capabilities.Add) != 0 || !slices.Contains(security.Capabilities.Drop, corev1.Capability("ALL")) {
			return errors.New("image admission requires dropping all capabilities")
		}
	}
	return nil
}

var nfsLayoutCommand = []string{"/bin/sh", "-ec", "grep ' /workspace ' /proc/self/mounts > /run/ganesha/mtab"}
var nfsReadyCommand = []string{"/usr/bin/timeout", "3", "/bin/bash", "/etc/ganesha/ready.sh"}

var nfsCapabilities = []corev1.Capability{"SYS_ADMIN", "DAC_READ_SEARCH", "DAC_OVERRIDE", "SYS_RESOURCE", "CHOWN", "FOWNER", "SETUID", "SETGID"}

func controllerSecurity(spec corev1.PodSpec, cfg NFS) error {
	if len(spec.InitContainers) != 2 || spec.InitContainers[0].Name != "home-layout" || spec.InitContainers[1].Name != "nfs-layout" || spec.HostNetwork || spec.HostPID || spec.HostIPC || len(spec.EphemeralContainers) != 0 || spec.ShareProcessNamespace != nil && *spec.ShareProcessNamespace {
		return errors.New("controller role isolation changed")
	}
	layout := spec.InitContainers[1]
	if layout.Image != cfg.Image || !slices.Equal(layout.Command, nfsLayoutCommand) || len(layout.Args) != 0 || len(layout.Env) != 0 || len(layout.EnvFrom) != 0 || len(layout.VolumeDevices) != 0 {
		return errors.New("NFS mount table initializer differs from admitted role")
	}
	application := spec.DeepCopy()
	application.Containers = nil
	found := false
	for _, container := range spec.Containers {
		if container.Name != "nfs" {
			application.Containers = append(application.Containers, container)
			continue
		}
		if container.Name != "nfs" || found || container.Image != cfg.Image || !slices.Equal(container.Command, []string{"/usr/bin/ganesha.nfsd"}) || !slices.Equal(container.Args, []string{"-F", "-L", "/proc/self/fd/2", "-p", "/run/ganesha/ganesha.pid", "-f", "/etc/ganesha/ganesha.conf"}) || len(container.EnvFrom) != 0 || len(container.Env) != 0 || len(container.VolumeDevices) != 0 {
			return errors.New("NFS image or command differs from admitted role")
		}
		if container.ReadinessProbe == nil || container.ReadinessProbe.Exec == nil || !slices.Equal(container.ReadinessProbe.Exec.Command, nfsReadyCommand) {
			return errors.New("NFS readiness must verify the exported filesystem")
		}
		found = true
		s := container.SecurityContext
		if s == nil || s.Privileged != nil && *s.Privileged || s.RunAsUser == nil || *s.RunAsUser != 0 || s.RunAsGroup == nil || *s.RunAsGroup != 0 || s.RunAsNonRoot == nil || *s.RunAsNonRoot || s.AllowPrivilegeEscalation == nil || !*s.AllowPrivilegeEscalation || s.ReadOnlyRootFilesystem == nil || *s.ReadOnlyRootFilesystem || s.SeccompProfile == nil || s.SeccompProfile.Type != corev1.SeccompProfileTypeUnconfined || s.Capabilities == nil || len(s.Capabilities.Drop) != 1 || s.Capabilities.Drop[0] != "ALL" || len(s.Capabilities.Add) != len(nfsCapabilities) {
			return errors.New("NFS security differs from admitted storage role")
		}
		for _, capability := range nfsCapabilities {
			if !slices.Contains(s.Capabilities.Add, capability) {
				return errors.New("NFS capability differs from admitted storage role")
			}
		}
	}
	if !found || !slices.ContainsFunc(application.Containers, func(c corev1.Container) bool { return c.Name == "controller" }) {
		return errors.New("controller and NFS roles required")
	}
	return unprivilegedPodSecurity(*application)
}
