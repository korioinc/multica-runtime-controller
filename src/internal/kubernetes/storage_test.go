package kubernetes

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func TestInstallationWorkspaceCannotBeReplacedOrImported(t *testing.T) {
	for name, change := range map[string]func(*corev1.PersistentVolumeClaim){
		"replacement":          func(p *corev1.PersistentVolumeClaim) { p.UID = types.UID(uuid.NewString()) },
		"another installation": func(p *corev1.PersistentVolumeClaim) { p.Labels[ownerLabel] = uuid.NewString() },
		"legacy data":          func(p *corev1.PersistentVolumeClaim) { delete(p.Labels, storageLayoutLabel) },
		"snapshot import": func(p *corev1.PersistentVolumeClaim) {
			p.Spec.DataSource = &corev1.TypedLocalObjectReference{Kind: "VolumeSnapshot", Name: "old-data"}
		},
		"shared writable claim": func(p *corev1.PersistentVolumeClaim) {
			p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		},
		"raw device": func(p *corev1.PersistentVolumeClaim) { p.Spec.VolumeMode = ptr.To(corev1.PersistentVolumeBlock) },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c, r, b, cfg := currentTaskFixture(t)
			var err error
			r.SecretUID, err = c.EnsureSecret(ctx, r, b)
			if err != nil {
				t.Fatal(err)
			}
			p, err := c.API.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, r.PVCName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			change(p)
			if _, err = c.API.CoreV1().PersistentVolumeClaims(c.Namespace).Update(ctx, p, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = ensureTaskPod(ctx, c, cfg, r, b); err == nil {
				t.Fatal("changed storage granted provider execution")
			}
			if _, err = c.API.CoreV1().Pods(c.Namespace).Get(ctx, r.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("worker persisted on untrusted storage", err)
			}
		})
	}
}

func TestWorkerLossDoesNotFenceWriterOrDeleteWorkspace(t *testing.T) {
	ctx := context.Background()
	c, r, b, cfg := currentTaskFixture(t)
	var err error
	r.SecretUID, err = c.EnsureSecret(ctx, r, b)
	if err != nil {
		t.Fatal(err)
	}
	r.PodUID, err = ensureTaskPod(ctx, c, cfg, r, b)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.API.CoreV1().Pods(c.Namespace).Delete(ctx, r.PodName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if observed, err := c.ObserveTermination(ctx, r); observed.Stopped || !errors.Is(err, ErrWriterUnknown) {
		t.Fatal("disappeared worker was treated as a fence", err)
	}
	if err = c.Cleanup(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err = c.ValidatePVC(ctx, r); err != nil {
		t.Fatal("attempt cleanup lost retained workspace", err)
	}
}

func TestOverlappingTaskMountPreventsAnotherWorker(t *testing.T) {
	ctx := context.Background()
	c, r, b, cfg := currentTaskFixture(t)
	var err error
	r.SecretUID, err = c.EnsureSecret(ctx, r, b)
	if err != nil {
		t.Fatal(err)
	}
	existing := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unresolved-writer", Namespace: c.Namespace}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: r.NFSServer, Path: r.TaskRoot}}}}}}
	if _, err = c.API.CoreV1().Pods(c.Namespace).Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = ensureTaskPod(ctx, c, cfg, r, b); err == nil {
		t.Fatal("second writer acquired the task workspace")
	}
	if _, err = c.API.CoreV1().Pods(c.Namespace).Get(ctx, r.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("conflicting worker persisted", err)
	}
}

func TestAdditionalContainerCannotAcquireControllerData(t *testing.T) {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "controller", VolumeMounts: []corev1.VolumeMount{{Name: "storage"}, {Name: "token"}, {Name: "private"}}},
			{Name: "nfs", VolumeMounts: []corev1.VolumeMount{{Name: "storage"}}},
			{Name: "cache", VolumeMounts: []corev1.VolumeMount{{Name: "cache", MountPath: "/data"}}},
		},
		Volumes: []corev1.Volume{
			{Name: "storage", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "tasks"}}},
			{Name: "token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "controller-credential"}}},
			{Name: "private", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "cache", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "packages"}}},
		},
	}
	if err := admitAdditionalContainerStorage(spec); err != nil {
		t.Fatal("independent sidecar storage was rejected", err)
	}
	for name, change := range map[string]func(*corev1.PodSpec){
		"shared private files": func(p *corev1.PodSpec) { p.Containers[2].VolumeMounts[0].Name = "private" },
		"task claim alias": func(p *corev1.PodSpec) {
			p.Volumes[3].PersistentVolumeClaim.ClaimName = "tasks"
		},
		"token alias": func(p *corev1.PodSpec) {
			p.Volumes[3].VolumeSource = corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "controller-credential"}}
		},
		"token environment": func(p *corev1.PodSpec) {
			p.Containers[2].Env = []corev1.EnvVar{{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "controller-credential"}, Key: "token"}}}}
		},
		"token environment source": func(p *corev1.PodSpec) {
			p.Containers[2].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "controller-credential"}}}}
		},
		"raw task device": func(p *corev1.PodSpec) {
			p.Containers[2].VolumeMounts = nil
			p.Containers[2].VolumeDevices = []corev1.VolumeDevice{{Name: "storage", DevicePath: "/dev/task"}}
		},
		"direct NFS export": func(p *corev1.PodSpec) {
			p.Volumes[3].VolumeSource = corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "10.43.0.10", Path: "/workspace"}}
		},
		"Kubernetes token": func(p *corev1.PodSpec) {
			p.Volumes[3].VolumeSource = corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := spec.DeepCopy()
			change(changed)
			if err := admitAdditionalContainerStorage(*changed); err == nil {
				t.Fatal("additional container acquired controller data or credentials")
			}
		})
	}
}
