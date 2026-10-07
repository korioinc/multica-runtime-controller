package kubernetes

import (
	"context"
	"log/slog"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// WatchWorkers only schedules observations. It never authorizes a cached Pod;
// live admission still checks all namespace consumers, including unknown Pods.
func (c *Client) WatchWorkers(ctx context.Context, ownerID string, attemptChanged, sessionChanged func(string)) {
	watchAPI := c.watchAPI
	if watchAPI == nil {
		watchAPI = c.API
	}
	selector := labels.Set{managedLabel: managedValue, ownerLabel: ownerID}.String()
	listWatch := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = selector
			return c.API.CoreV1().Pods(c.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = selector
			return watchAPI.CoreV1().Pods(c.Namespace).Watch(ctx, options)
		},
	}
	informer := cache.NewSharedIndexInformer(listWatch, &corev1.Pod{}, 0, cache.Indexers{})
	notify := func(object any) {
		if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
			object = tombstone.Obj
		}
		pod, ok := object.(*corev1.Pod)
		if ok && pod.Namespace == c.Namespace && pod.Labels[managedLabel] == managedValue && pod.Labels[ownerLabel] == ownerID {
			if id := pod.Labels[sessionLabel]; wire.UUID(id) {
				sessionChanged(id)
			} else if id := pod.Labels[attemptLabel]; wire.UUID(id) {
				attemptChanged(id)
			}
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: notify,
		UpdateFunc: func(old, next any) {
			if old.(*corev1.Pod).ResourceVersion != next.(*corev1.Pod).ResourceVersion {
				notify(next)
			}
		},
		DeleteFunc: notify,
	})
	_ = informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, _ *cache.Reflector, _ error) {
		if ctx.Err() == nil {
			slog.Warn("worker observation watch unavailable", "reason", "watch_unavailable")
		}
	})
	informer.RunWithContext(ctx)
}
