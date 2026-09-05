package intercept

import (
	"context"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/taskstate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func CollectTaskStorage(ctx context.Context, namespace string, client kubernetes.Interface, taskDeadline time.Duration) (int, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: ManagedByLabel + "=" + ManagedByValue})
	if err != nil {
		return 0, err
	}
	active := make(map[string]bool)
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			active[pod.Labels[StorageIDLabel]] = true
		}
	}
	store, err := taskstate.New(taskstate.DefaultDirectory)
	if err != nil {
		return 0, err
	}
	grace := max(taskDeadline, 6*time.Hour) + 24*time.Hour
	return store.Collect(workspaceRoot, time.Now().Add(-grace), active)
}
