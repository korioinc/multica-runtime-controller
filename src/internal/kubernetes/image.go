package kubernetes

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type ImageBinding struct {
	Owner Owner
	Image string
}

// BindImage uses the current Pod API object and the checked installation. A registry's current tag is
// not evidence for the bytes executing in this Pod, including after a restart.
// The caller must verify its immutable private receipt before entering here.
func (c *Client) BindImage(ctx context.Context, name, uid, container, node, platform string, descriptor runtimeimage.Descriptor) (ImageBinding, error) {
	var binding ImageBinding
	if name == "" || uid == "" || container == "" || !core.SupportedPlatform(platform) || platform != core.HostPlatform() {
		return binding, errors.New("controller image binding requires current Pod identity/platform")
	}
	err := wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		pod, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		image, pending, err := boundImage(pod, c.Namespace, name, uid, container, node, platform)
		if err != nil {
			return false, err
		}
		if pending {
			return false, nil
		}
		if err = admitImageEnvironment(pod, descriptor); err != nil {
			return false, err
		}
		binding = ImageBinding{Owner: Owner{Name: pod.Name, UID: string(pod.UID)}, Image: image}
		return true, nil
	})
	return binding, err
}

func boundImage(p *corev1.Pod, namespace, name, uid, container, node, platform string) (string, bool, error) {
	if p == nil || p.Namespace != namespace || p.Name != name || string(p.UID) != uid || p.DeletionTimestamp != nil || node != "" && p.Spec.NodeName != node {
		return "", false, diagnostics.Wrap("runtime_pod_identity_mismatch", errors.New("controller Pod identity or placement changed"))
	}
	if len(p.Spec.Containers) != 1 || p.Spec.Containers[0].Name != container || len(p.Spec.InitContainers) == 0 {
		return "", false, errors.New("controller application container layout mismatch")
	}
	if p.Spec.NodeSelector["kubernetes.io/os"] != "linux" || p.Spec.NodeSelector["kubernetes.io/arch"] != strings.TrimPrefix(platform, "linux/") {
		return "", false, diagnostics.Wrap("runtime_platform_mismatch", errors.New("controller Pod platform differs from its installed image"))
	}
	selected := ""
	find := func(name string, statuses []corev1.ContainerStatus) (string, bool, error) {
		var result *corev1.ContainerStatus
		for i := range statuses {
			if statuses[i].Name == name {
				if result != nil {
					return "", false, errors.New("duplicate application image status")
				}
				result = &statuses[i]
			}
		}
		if result == nil || result.ImageID == "" {
			return "", true, nil
		}
		image, err := runtimeimage.NormalizeImageID(result.ImageID)
		return image, false, err
	}
	image, pending, err := find(container, p.Status.ContainerStatuses)
	if err != nil || pending {
		return "", pending, err
	}
	selected = image
	for _, init := range p.Spec.InitContainers {
		image, pending, err = find(init.Name, p.Status.InitContainerStatuses)
		if err != nil || pending {
			return "", pending, err
		}
		if image != selected {
			return "", false, diagnostics.Wrap("runtime_init_image_mismatch", errors.New("controller init and main are running different image digests"))
		}
	}
	return selected, false, nil
}
