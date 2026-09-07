package kubernetes

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// PodFingerprint stores only a digest of the complete creation payload. The
// journal never acquires the operator env values contained in that payload.
func PodFingerprint(cfg Config, ref Reference, request wire.Request, gateway string) (string, error) {
	pod, err := podObject(cfg, ref, request, gateway)
	if err != nil {
		return "", err
	}
	return specFingerprint(pod.Spec), nil
}

// Normalize only defaulted Kubernetes fields and scheduler Node assignment.
// Every mount, volume source, credential source, command and security setting
// remains part of the payload proof, including additional admission-injected
// containers. A configured fixed Node is checked separately before normalization.
func specFingerprint(input corev1.PodSpec) string {
	p := input.DeepCopy()
	p.NodeName = ""
	if p.DNSPolicy == "" {
		p.DNSPolicy = corev1.DNSClusterFirst
	}
	if p.SchedulerName == "" {
		p.SchedulerName = "default-scheduler"
	}
	if p.DeprecatedServiceAccount == "" {
		p.DeprecatedServiceAccount = p.ServiceAccountName
	}
	if p.EnableServiceLinks == nil {
		p.EnableServiceLinks = ptr.To(true)
	}
	if p.PreemptionPolicy == nil {
		p.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
	}
	if p.Priority == nil {
		p.Priority = ptr.To[int32](0)
	}
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		exists := false
		for _, t := range p.Tolerations {
			if (t.Key == key || t.Key == "") && (t.Effect == "" || t.Effect == corev1.TaintEffectNoExecute) {
				exists = true
			}
		}
		if !exists {
			p.Tolerations = append(p.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To[int64](300)})
		}
	}
	for i := range p.Tolerations {
		if p.Tolerations[i].Operator == "" {
			p.Tolerations[i].Operator = corev1.TolerationOpEqual
		}
	}
	slices.SortFunc(p.Tolerations, func(a, b corev1.Toleration) int {
		x, _ := json.Marshal(a)
		y, _ := json.Marshal(b)
		return strings.Compare(string(x), string(y))
	})
	containerDefaults := func(containers []corev1.Container) {
		for i := range containers {
			c := &containers[i]
			for name, limit := range c.Resources.Limits {
				if _, exists := c.Resources.Requests[name]; !exists {
					if c.Resources.Requests == nil {
						c.Resources.Requests = corev1.ResourceList{}
					}
					c.Resources.Requests[name] = limit.DeepCopy()
				}
			}
			for j := range c.Env {
				v := c.Env[j].ValueFrom
				if v == nil {
					continue
				}
				if v.FieldRef != nil && v.FieldRef.APIVersion == "" {
					v.FieldRef.APIVersion = "v1"
				}
				if v.ResourceFieldRef != nil && v.ResourceFieldRef.Divisor.IsZero() {
					v.ResourceFieldRef.Divisor = resource.MustParse("1")
				}
			}
			if c.TerminationMessagePath == "" {
				c.TerminationMessagePath = "/dev/termination-log"
			}
			if c.TerminationMessagePolicy == "" {
				c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}
			for n := range c.Ports {
				if c.Ports[n].Protocol == "" {
					c.Ports[n].Protocol = corev1.ProtocolTCP
				}
			}
			for _, probe := range []*corev1.Probe{c.ReadinessProbe, c.LivenessProbe, c.StartupProbe} {
				if probe == nil {
					continue
				}
				if probe.TimeoutSeconds == 0 {
					probe.TimeoutSeconds = 1
				}
				if probe.PeriodSeconds == 0 {
					probe.PeriodSeconds = 10
				}
				if probe.SuccessThreshold == 0 {
					probe.SuccessThreshold = 1
				}
				if probe.FailureThreshold == 0 {
					probe.FailureThreshold = 3
				}
			}
		}
	}
	containerDefaults(p.Containers)
	containerDefaults(p.InitContainers)
	for i := range p.Volumes {
		v := &p.Volumes[i]
		if v.Secret != nil && v.Secret.DefaultMode == nil {
			v.Secret.DefaultMode = ptr.To[int32](0644)
		}
		if v.ConfigMap != nil && v.ConfigMap.DefaultMode == nil {
			v.ConfigMap.DefaultMode = ptr.To[int32](0644)
		}
		if v.Projected != nil && v.Projected.DefaultMode == nil {
			v.Projected.DefaultMode = ptr.To[int32](0644)
		}
	}
	raw, _ := json.Marshal(p)
	return wire.Digest(raw)
}
