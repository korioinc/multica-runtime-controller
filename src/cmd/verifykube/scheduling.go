package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func (f *fixture) fixedAffinity(node string) *corev1.Affinity {
	if node == "" {
		return nil
	}
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node}}}}}}}}
}

// Every OR term must require the same Node name. Merely finding one matching
// term would leave an unconstrained scheduling route untested.
func fixedNode(policy *corev1.Affinity) (string, error) {
	if policy == nil || policy.NodeAffinity == nil || policy.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return "", errors.New("required Node affinity absent")
	}
	name := ""
	terms := policy.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return "", errors.New("required Node affinity has no feasible term")
	}
	for _, term := range terms {
		selected := ""
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 {
				selected = field.Values[0]
			}
		}
		if selected == "" || name != "" && selected != name {
			return "", errors.New("Node affinity permits more than the selected fixed Node")
		}
		name = selected
	}
	return name, nil
}
func missingNodePolicy(policy *corev1.Affinity, missing string) *corev1.Affinity {
	cloned := policy.DeepCopy()
	for i := range cloned.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		term := &cloned.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		for j := range term.MatchFields {
			if term.MatchFields[j].Key == "metadata.name" && term.MatchFields[j].Operator == corev1.NodeSelectorOpIn {
				term.MatchFields[j].Values = []string{missing}
			}
		}
	}
	return cloned
}
func (f *fixture) scheduleProbe(policy *corev1.Affinity, selector map[string]string, expected string, unavailable bool) error {
	probe := f.simplePod("verify-schedule-")
	probe.Spec.Affinity = policy.DeepCopy()
	probe.Spec.NodeSelector = maps.Clone(selector)
	actual, err := f.api.CoreV1().Pods(f.selection.Namespace).Create(f.ctx, probe, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepPod(actual)
	err = wait.PollUntilContextTimeout(f.ctx, 250*time.Millisecond, 45*time.Second, true, func(ctx context.Context) (bool, error) {
		pod, err := f.api.CoreV1().Pods(f.selection.Namespace).Get(ctx, actual.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if unavailable {
			if pod.Spec.NodeName != "" {
				return false, fmt.Errorf("missing fixed Node silently moved to %s", pod.Spec.NodeName)
			}
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == "Unschedulable" {
					return pod.Status.Phase == corev1.PodPending, nil
				}
			}
			return false, nil
		}
		if pod.Spec.NodeName == "" {
			return false, nil
		}
		if pod.Spec.NodeName != expected {
			return false, fmt.Errorf("probe scheduled on %s instead of %s", pod.Spec.NodeName, expected)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	return f.deletePod(actual.Name, actual.UID)
}
func (f *fixture) alternativeNode(fixed string) (*corev1.Node, error) {
	name := "verify-scheduler-" + uuid.NewString()[:8]
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": strings.TrimPrefix(f.selection.Worker.Platform, "linux/"), "kubernetes.io/hostname": fixed, "verification.multica.io/scheduler-node": name}}}
	node, err := f.api.CoreV1().Nodes().Create(f.ctx, node, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	uid := node.UID
	heartbeatCtx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	f.cleanup = append(f.cleanup, func() {
		stop()
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = f.api.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	})
	heartbeat := func(ctx context.Context) error {
		current, err := f.api.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.UID != uid {
			return errors.New("scheduler fixture Node was replaced")
		}
		current.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("128"), corev1.ResourceMemory: resource.MustParse("512Gi"), corev1.ResourcePods: resource.MustParse("110")}
		current.Status.Allocatable = current.Status.Capacity.DeepCopy()
		current.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "DisposableSchedulerFixture", LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
		_, err = f.api.CoreV1().Nodes().UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	}
	if err = heartbeat(f.ctx); err != nil {
		close(done)
		return nil, err
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = heartbeat(heartbeatCtx)
			}
		}
	}()
	return node, nil
}
func (f *fixture) scheduling() error {
	cfg := f.selection.Worker
	if cfg.SingleNodeName == "" || cfg.ToolsAccessMode != corev1.ReadWriteOnce && cfg.WorkspaceAccessMode != corev1.ReadWriteOnce {
		return errors.New("RWO fixture with explicit singleNodeName required")
	}
	node, err := f.api.CoreV1().Nodes().Get(f.ctx, cfg.SingleNodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		ready = ready || condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
	}
	if !ready {
		return errors.New("selected actual Node is not Ready")
	}
	// Verify the fixture really uses RWO storage, rather than only declaring it in
	// the runtime config while Kubernetes sees a different access model.
	for claim, mode := range map[string]corev1.PersistentVolumeAccessMode{cfg.ToolsClaim: cfg.ToolsAccessMode, cfg.WorkspaceClaim: cfg.WorkspaceAccessMode} {
		pvc, err := f.api.CoreV1().PersistentVolumeClaims(f.selection.Namespace).Get(f.ctx, claim, metav1.GetOptions{})
		if err != nil {
			return err
		}
		found := false
		for _, actual := range pvc.Spec.AccessModes {
			found = found || actual == mode
		}
		if !found {
			return errors.New("fixture PVC access mode does not match its declaration")
		}
	}
	deployments, err := f.api.AppsV1().Deployments(f.selection.Namespace).List(f.ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/instance=verify"})
	if err != nil {
		return err
	}
	if len(deployments.Items) != 1 {
		return errors.New("one actual verify chart Deployment required")
	}
	deployment := deployments.Items[0]
	chartPolicy := deployment.Spec.Template.Spec.Affinity
	name, err := fixedNode(chartPolicy)
	if err != nil || name != cfg.SingleNodeName {
		return errors.Join(err, errors.New("chart does not constrain the selected Node name"))
	}
	ref, _, err := f.createPair()
	if err != nil {
		return err
	}
	worker, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	workerPolicy := worker.Spec.Affinity.DeepCopy()
	name, err = fixedNode(workerPolicy)
	if err != nil || name != cfg.SingleNodeName {
		return errors.Join(err, errors.New("worker does not constrain the selected Node name"))
	}
	if err = f.client.Cleanup(f.ctx, ref); err != nil {
		return err
	}
	alternative, err := f.alternativeNode(cfg.SingleNodeName)
	if err != nil {
		return err
	}
	// This successful binding proves the extra Node is genuinely scheduler-feasible.
	// Its hostname label equals the real Node name, exposing hostname assumptions.
	if err = f.scheduleProbe(nil, map[string]string{"verification.multica.io/scheduler-node": alternative.Name}, alternative.Name, false); err != nil {
		return fmt.Errorf("alternative Node feasibility: %w", err)
	}
	if err = f.scheduleProbe(chartPolicy, deployment.Spec.Template.Spec.NodeSelector, cfg.SingleNodeName, false); err != nil {
		return fmt.Errorf("chart Node policy: %w", err)
	}
	if err = f.scheduleProbe(workerPolicy, worker.Spec.NodeSelector, cfg.SingleNodeName, false); err != nil {
		return fmt.Errorf("worker Node policy: %w", err)
	}
	missing := "verify-missing-" + uuid.NewString()[:8]
	if err = f.scheduleProbe(missingNodePolicy(chartPolicy, missing), deployment.Spec.Template.Spec.NodeSelector, missing, true); err != nil {
		return fmt.Errorf("missing controller Node: %w", err)
	}
	if err = f.scheduleProbe(missingNodePolicy(workerPolicy, missing), worker.Spec.NodeSelector, missing, true); err != nil {
		return fmt.Errorf("missing worker Node: %w", err)
	}
	wrong := cfg
	wrong.NodeSelector = maps.Clone(cfg.NodeSelector)
	if wrong.NodeSelector == nil {
		wrong.NodeSelector = map[string]string{}
	}
	if strings.HasSuffix(cfg.Platform, "amd64") {
		wrong.NodeSelector["kubernetes.io/arch"] = "arm64"
	} else {
		wrong.NodeSelector["kubernetes.io/arch"] = "amd64"
	}
	if err = wrong.Validate(); err == nil {
		return errors.New("conflicting architecture selector accepted")
	}
	invalidRef, request, err := f.attempt()
	if err != nil {
		return err
	}
	if _, err = f.client.CreatePod(f.ctx, wrong, invalidRef, request, f.selection.Gateway); err == nil {
		return errors.New("conflicting selector created a task Pod")
	}
	// Actual controller recreation uses the real chart Deployment and both PVCs.
	old, err := f.api.CoreV1().Pods(f.selection.Namespace).Get(f.ctx, f.selection.Controller.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if old.Spec.NodeName != cfg.SingleNodeName {
		return errors.New("original controller escaped fixed Node")
	}
	if err = f.deletePod(old.Name, old.UID); err != nil {
		return err
	}
	var replacement *corev1.Pod
	err = wait.PollUntilContextTimeout(f.ctx, 500*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := f.api.CoreV1().Pods(f.selection.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(deployment.Spec.Selector)})
		if err != nil {
			return false, err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.UID == old.UID || pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
				continue
			}
			if pod.Spec.NodeName != cfg.SingleNodeName {
				return false, errors.New("recreated controller moved away from the fixed Node")
			}
			bound, err := fixedNode(pod.Spec.Affinity)
			if err != nil || bound != cfg.SingleNodeName {
				return false, errors.Join(err, errors.New("recreated controller lost required Node name"))
			}
			replacement = pod
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	f.evidence.Checks = append(f.evidence.Checks, check{Name: "actual-controller-recreated", Passed: true, Details: map[string]string{"beforeUID": string(old.UID), "afterUID": string(replacement.UID), "fixedNode": cfg.SingleNodeName, "alternativeNode": alternative.Name}})
	return nil
}
