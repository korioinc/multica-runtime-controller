package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
)

func (c *Client) Execute(ctx context.Context, r Reference, deadline time.Duration, streams Streams) error {
	if r.PodUID == "" || r.SecretUID == "" {
		return errors.New("execution UIDs are unresolved")
	}
	if err := c.waitWorkerReady(ctx, r, deadline); err != nil {
		return err
	}
	if err := c.authorizeExecution(ctx, r); err != nil {
		return err
	}
	endpoint := c.API.CoreV1().RESTClient().Post().Resource("pods").Namespace(c.Namespace).Name(r.PodName).SubResource("exec").VersionedParams(workerExecOptions(r, streams), scheme.ParameterCodec).URL()
	return streamExec(ctx, c.Transport, endpoint, streams)
}

func (c *Client) waitWorkerReady(ctx context.Context, r Reference, deadline time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, min(deadline, 5*time.Minute), true, func(ctx context.Context) (bool, error) {
		pod, err := c.executionPod(ctx, r)
		if err != nil {
			return false, err
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "worker" && status.Ready {
				return true, nil
			}
		}
		return false, nil
	})
}

// Readiness is not execution authority. Re-read live resources after waiting;
// cleanup resolution intentionally permits the controller to be absent.
func (c *Client) authorizeExecution(ctx context.Context, r Reference) error {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return err
	}
	secret, err := c.secret(ctx, r.SecretName)
	if err != nil {
		return err
	}
	if !secretMatches(secret, r) || secret.DeletionTimestamp != nil {
		return errors.New("execution request Secret identity changed")
	}
	_, err = c.executionPod(ctx, r)
	return err
}

func (c *Client) executionPod(ctx context.Context, r Reference) (*corev1.Pod, error) {
	pod, err := c.pod(ctx, r.PodName)
	if err != nil {
		return nil, err
	}
	if !podMatches(pod, r) || pod.DeletionTimestamp != nil {
		return nil, errors.New("execution Pod identity changed")
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return nil, errors.New("worker ended before execution")
	}
	return pod, nil
}

func workerExecOptions(r Reference, streams Streams) *corev1.PodExecOptions {
	return &corev1.PodExecOptions{
		Container: "worker",
		Command: []string{
			wire.ControllerRoot + "/runtime", "worker", "execute",
			"--request-digest=" + r.RequestDigest,
			"--pod-uid=" + r.PodUID,
			fmt.Sprintf("--stdin=%t", streams.Stdin != nil),
			fmt.Sprintf("--stdout=%t", streams.Stdout != nil),
			fmt.Sprintf("--stderr=%t", streams.Stderr != nil),
		},
		Stdin: streams.Stdin != nil, Stdout: streams.Stdout != nil, Stderr: streams.Stderr != nil,
	}
}
