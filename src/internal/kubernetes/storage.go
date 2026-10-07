package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const ControllerStateRoot = "/var/lib/multica/controller"
const NFSRecoveryRoot = "/var/lib/nfs/ganesha"
const ownerLabel = "multica.ai/owner-id"
const storageLayoutLabel = "multica.ai/storage-layout"
const storageLayout = "controller-nfs-v1"

func validTaskMount(r Reference) bool {
	if r.WorkerSessionID != "" && !validSessionMount(r) {
		return false
	}
	return net.ParseIP(r.NFSServer) != nil && path.Clean(r.TaskRoot) == r.TaskRoot && strings.HasPrefix(r.TaskRoot, wire.WorkspaceRoot+"/") && len(strings.Split(strings.TrimPrefix(r.TaskRoot, wire.WorkspaceRoot+"/"), "/")) == 2 && !strings.ContainsAny(r.TaskRoot, "\x00\r\n")
}

func retainedFilesystem(p *corev1.PersistentVolumeClaim) bool {
	return p != nil && p.UID != "" && p.DeletionTimestamp == nil && len(p.OwnerReferences) == 0 && len(p.Spec.AccessModes) == 1 && p.Spec.AccessModes[0] == corev1.ReadWriteOnce && (p.Spec.VolumeMode == nil || *p.Spec.VolumeMode == corev1.PersistentVolumeFilesystem) && p.Spec.DataSource == nil && p.Spec.DataSourceRef == nil && p.Spec.Selector == nil && p.Labels[storageLayoutLabel] == storageLayout && wire.UUID(p.Labels[ownerLabel])
}

func pvcMatches(p *corev1.PersistentVolumeClaim, r Reference) bool {
	return retainedFilesystem(p) && p.Status.Phase == corev1.ClaimBound && p.Spec.VolumeName != "" && p.Namespace == r.Namespace && p.Name == r.PVCName && r.PVCUID != "" && string(p.UID) == r.PVCUID && p.Labels[ownerLabel] == r.OwnerID && p.Labels["app.kubernetes.io/component"] == "storage"
}

// ResolveWorkspace reads the installation's storage identity. Creation belongs
// to Helm; this path never adopts or modifies a pre-migration volume.
func (c *Client) ResolveWorkspace(ctx context.Context, cfg Config) (pvcUID, nfsServer string, err error) {
	if err = cfg.Validate(); err != nil {
		return
	}
	p, err := c.API.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, cfg.Storage.ClaimName, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	size := p.Spec.Resources.Requests[corev1.ResourceStorage]
	if !retainedFilesystem(p) || p.Labels["app.kubernetes.io/component"] != "storage" || size.Value() < cfg.Storage.MaxBytes || p.Status.Phase != corev1.ClaimBound || p.Spec.VolumeName == "" {
		return "", "", errors.New("installation workspace requires a bound new-layout RWO filesystem claim covering its admission budget")
	}
	service, err := c.API.CoreV1().Services(c.Namespace).Get(ctx, cfg.NFS.Server, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	if service.DeletionTimestamp != nil || service.Spec.Type != corev1.ServiceTypeClusterIP || len(service.Spec.ExternalIPs) != 0 || service.Spec.ExternalName != "" || net.ParseIP(service.Spec.ClusterIP) == nil || !service.Spec.PublishNotReadyAddresses || service.Labels[ownerLabel] != p.Labels[ownerLabel] || service.Spec.Selector[ownerLabel] != p.Labels[ownerLabel] || service.Spec.Selector["app.kubernetes.io/component"] != "controller" || len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Protocol != corev1.ProtocolTCP || service.Spec.Ports[0].Port != 2049 || service.Spec.Ports[0].TargetPort.IntValue() != 2049 {
		return "", "", errors.New("installation NFS Service identity or fixed endpoint changed")
	}
	return string(p.UID), service.Spec.ClusterIP, nil
}

func (c *Client) ValidatePVC(ctx context.Context, r Reference) error {
	if r.Namespace != c.Namespace || r.PVCUID == "" || !wire.UUID(r.OwnerID) {
		return errors.New("journaled installation workspace UID and owner required")
	}
	p, err := c.API.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, r.PVCName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !pvcMatches(p, r) {
		return errors.New("installation workspace identity changed")
	}
	return nil
}

// StorageAvailable detects overlapping NFS consumers. The journal must also
// prove the previous writer sealed: a missing Pod is never that proof.
func (c *Client) StorageAvailable(ctx context.Context, r Reference) error {
	if err := c.ValidatePVC(ctx, r); err != nil {
		return err
	}
	if !validTaskMount(r) {
		return errors.New("journaled task NFS mount required")
	}
	pods, err := c.Pods(ctx)
	if err != nil {
		return err
	}
	for _, p := range pods {
		if r.PodUID != "" && p.Name == r.PodName && string(p.UID) == r.PodUID {
			continue
		}
		for _, v := range p.Spec.Volumes {
			if v.NFS != nil && v.NFS.Server == r.NFSServer && (path.Clean(v.NFS.Path) == r.TaskRoot || strings.HasPrefix(r.TaskRoot, path.Clean(v.NFS.Path)+"/") || strings.HasPrefix(path.Clean(v.NFS.Path), r.TaskRoot+"/")) {
				return fmt.Errorf("task workspace has an unresolved consumer: %s", p.Name)
			}
		}
	}
	return nil
}

func (c *Client) AdmitControllerStorage(ctx context.Context, owner Owner, cfg Config) error {
	pod, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pod.UID) != owner.UID || pod.DeletionTimestamp != nil || pod.Spec.Hostname != cfg.NFS.Server {
		return errors.New("controller storage owner or NFS identity changed")
	}
	if err := controllerSecurity(pod.Spec, cfg.NFS); err != nil {
		return err
	}
	pvcUID, _, err := c.ResolveWorkspace(ctx, cfg)
	if err != nil {
		return err
	}
	workspace, err := c.API.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, cfg.Storage.ClaimName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(workspace.UID) != pvcUID || pod.Labels[ownerLabel] != workspace.Labels[ownerLabel] {
		return errors.New("controller and workspace installation owners differ")
	}
	volumes := map[string]corev1.VolumeSource{}
	for _, v := range pod.Spec.Volumes {
		volumes[v.Name] = v.VolumeSource
	}
	if err := admitAdditionalContainerStorage(pod.Spec); err != nil {
		return err
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "controller" && container.Name != "nfs" {
			continue
		}
		state, task, recovery, config, run, mtab := false, false, false, false, false, false
		for _, m := range container.VolumeMounts {
			if m.SubPathExpr != "" || m.MountPath == wire.Home || strings.HasPrefix(m.MountPath, wire.Home+"/") || strings.HasPrefix(wire.Home, m.MountPath+"/") {
				return errors.New("controller and NFS HOME cannot be mounted")
			}
			v := volumes[m.Name]
			switch m.MountPath {
			case wire.WorkspaceRoot:
				task = !m.ReadOnly && m.SubPath == "workspace" && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == cfg.Storage.ClaimName && !v.PersistentVolumeClaim.ReadOnly
			case ControllerStateRoot:
				state = container.Name == "controller" && !m.ReadOnly && m.SubPath == "controller" && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == cfg.Storage.ClaimName && !v.PersistentVolumeClaim.ReadOnly
			case NFSRecoveryRoot:
				recovery = container.Name == "nfs" && !m.ReadOnly && m.SubPath == "nfs-recovery" && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == cfg.Storage.ClaimName && !v.PersistentVolumeClaim.ReadOnly
			case "/etc/ganesha":
				config = container.Name == "nfs" && m.ReadOnly && m.SubPath == "" && v.ConfigMap != nil && v.ConfigMap.Name == cfg.NFS.Server
			case "/etc/mtab":
				mtab = container.Name == "nfs" && m.ReadOnly && m.SubPath == "mtab" && m.Name == "nfs-run" && v.EmptyDir != nil
			case "/run/ganesha":
				run = container.Name == "nfs" && !m.ReadOnly && m.SubPath == "" && m.Name == "nfs-run" && v.EmptyDir != nil
			default:
				if container.Name == "nfs" {
					return errors.New("NFS sidecar acquired a private controller mount")
				}
			}
		}
		if container.Name == "controller" && (!state || !task) || container.Name == "nfs" && (!task || !recovery || !config || !run || !mtab || len(container.VolumeMounts) != 5) {
			return errors.New("controller or NFS storage differs from role policy")
		}
	}
	layout := pod.Spec.InitContainers[1]
	if len(layout.VolumeMounts) != 2 {
		return errors.New("NFS initializer acquired a private controller mount")
	}
	workspaceMount, tableMount := false, false
	for _, m := range layout.VolumeMounts {
		v := volumes[m.Name]
		if m.ReadOnly || m.SubPathExpr != "" {
			return errors.New("NFS initializer requires the prepared workspace mount")
		}
		if m.MountPath == wire.WorkspaceRoot {
			workspaceMount = m.SubPath == "workspace" && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == cfg.Storage.ClaimName && !v.PersistentVolumeClaim.ReadOnly
		}
		if m.MountPath == "/run/ganesha" {
			tableMount = m.SubPath == "" && m.Name == "nfs-run" && v.EmptyDir != nil
		}
	}
	if !workspaceMount || !tableMount {
		return errors.New("NFS mount table must describe its installation workspace")
	}
	pods, err := c.Pods(ctx)
	if err != nil {
		return err
	}
	for _, p := range pods {
		if p.Name == owner.Name && string(p.UID) == owner.UID {
			continue
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == cfg.Storage.ClaimName {
				return errors.New("installation storage has another direct consumer")
			}
		}
	}
	return nil
}

func admitAdditionalContainerStorage(spec corev1.PodSpec) error {
	protected := map[string]bool{}
	for _, c := range spec.Containers {
		if c.Name == "controller" || c.Name == "nfs" {
			for _, m := range c.VolumeMounts {
				protected[m.Name] = true
			}
		}
	}
	volumes := map[string]corev1.VolumeSource{}
	for _, v := range spec.Volumes {
		volumes[v.Name] = v.VolumeSource
	}
	for _, c := range spec.Containers {
		if c.Name == "controller" || c.Name == "nfs" {
			continue
		}
		for source := range protected {
			if secret := volumes[source].Secret; secret != nil {
				for _, env := range c.Env {
					if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == secret.SecretName {
						return errors.New("additional container acquired controller credentials")
					}
				}
				for _, env := range c.EnvFrom {
					if env.SecretRef != nil && env.SecretRef.Name == secret.SecretName {
						return errors.New("additional container acquired controller credentials")
					}
				}
			}
		}
		names := []string{}
		for _, m := range c.VolumeMounts {
			names = append(names, m.Name)
		}
		for _, d := range c.VolumeDevices {
			names = append(names, d.Name)
		}
		for _, name := range names {
			v := volumes[name]
			if protected[name] || v.HostPath != nil || v.Projected != nil || v.NFS != nil {
				return errors.New("additional container acquired controller storage or credentials")
			}
			for source := range protected {
				p := volumes[source]
				if v.PersistentVolumeClaim != nil && p.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == p.PersistentVolumeClaim.ClaimName || v.Secret != nil && p.Secret != nil && v.Secret.SecretName == p.Secret.SecretName {
					return errors.New("additional container acquired controller storage or credentials")
				}
			}
		}
	}
	return nil
}
