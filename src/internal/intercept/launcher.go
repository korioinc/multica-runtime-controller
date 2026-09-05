package intercept

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type Streams struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type Launcher struct {
	config     Config
	owner      metav1.OwnerReference
	client     kubernetes.Interface
	restConfig *rest.Config
}

func NewLauncher(ctx context.Context, cfg Config, controllerPodName string) (*Launcher, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	controllerPod, err := client.CoreV1().Pods(cfg.Namespace).Get(ctx, controllerPodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get controller Pod: %w", err)
	}
	owner := *metav1.NewControllerRef(controllerPod, schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	return &Launcher{config: cfg, owner: owner, client: client, restConfig: restConfig}, nil
}

func (l *Launcher) Run(ctx context.Context, taskID string, request Request, streams Streams) error {
	releaseStorage, err := l.prepareTaskStorage(ctx, taskID, &request)
	if err != nil {
		return err
	}
	defer releaseStorage()
	port, token, closeBroker, err := startCheckoutBroker(request)
	if err != nil {
		return err
	}
	defer closeBroker()
	request.BrokerPort, request.BrokerToken = port, token
	secret, err := requestSecret(l.config, taskID, request, l.owner)
	if err != nil {
		return err
	}
	secrets := l.client.CoreV1().Secrets(l.config.Namespace)
	pods := l.client.CoreV1().Pods(l.config.Namespace)
	createdSecret, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create provider request Secret with prefix %s: %w", secret.GenerateName, err)
	}
	createdSecretName := createdSecret.Name
	createdSecretUID := createdSecret.UID
	createdPodName := ""
	var createdPodUID types.UID
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if createdPodName != "" {
			_ = pods.Delete(cleanupCtx, createdPodName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &createdPodUID}})
			_ = wait.PollUntilContextTimeout(cleanupCtx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
				pod, err := pods.Get(ctx, createdPodName, metav1.GetOptions{})
				return apierrors.IsNotFound(err) || (err == nil && pod.UID != createdPodUID), nil
			})
		}
		if createdSecretName != "" && createdSecretUID != "" {
			_ = secrets.Delete(cleanupCtx, createdSecretName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &createdSecretUID}})
		}
	}()
	if createdSecretName == "" || createdSecretUID == "" {
		return errors.New("Kubernetes API did not confirm the provider request Secret identity")
	}

	pod, err := taskPod(l.config, taskID, createdSecretName, request, l.owner)
	if err != nil {
		return err
	}

	createdPod, err := pods.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create exclusive task Pod %s: %w", pod.Name, err)
	}
	if createdPod.Name != pod.Name || createdPod.UID == "" {
		return errors.New("Kubernetes API did not confirm the expected task Pod identity")
	}
	createdPodName, createdPodUID = createdPod.Name, createdPod.UID
	if err := l.waitUntilRunning(ctx, createdPodName); err != nil {
		return err
	}
	if err := l.streamProvider(ctx, createdPodName, streams); err != nil {
		return fmt.Errorf("execute provider in task Pod %s: %w", createdPodName, err)
	}
	return nil
}

func (l *Launcher) waitUntilRunning(ctx context.Context, podName string) error {
	timeout := 5 * time.Minute
	if l.config.Deadline < timeout {
		timeout = l.config.Deadline
	}
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := l.client.CoreV1().Pods(l.config.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch pod.Status.Phase {
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fmt.Errorf("task Pod %s became %s before provider execution", podName, pod.Status.Phase)
		case corev1.PodRunning:
			for _, status := range pod.Status.ContainerStatuses {
				if status.Name == "worker" && status.Ready {
					return true, nil
				}
			}
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Waiting != nil && nonRetryableWaitingReason(status.State.Waiting.Reason) {
				return false, fmt.Errorf("task Pod %s cannot start: %s: %s", podName, status.State.Waiting.Reason, status.State.Waiting.Message)
			}
		}
		return false, nil
	})
}

func nonRetryableWaitingReason(reason string) bool {
	switch reason {
	case "ErrImagePull", "ImagePullBackOff", "CreateContainerConfigError", "CreateContainerError", "InvalidImageName":
		return true
	default:
		return false
	}
}

func (l *Launcher) streamProvider(ctx context.Context, podName string, streams Streams) error {
	request := l.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(l.config.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "worker",
			Command:   []string{"/usr/local/bin/multica-runtime", "provider-worker", "--request-file=" + RequestFilePath()},
			Stdin:     streams.Stdin != nil,
			Stdout:    streams.Stdout != nil,
			Stderr:    streams.Stderr != nil,
			TTY:       false,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(l.restConfig, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("create task Pod exec stream: %w", err)
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: streams.Stdin, Stdout: streams.Stdout, Stderr: streams.Stderr, Tty: false,
	})
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return err
}
