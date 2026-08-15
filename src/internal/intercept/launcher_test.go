package intercept

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestLauncherRetryCreatesDistinctAttemptResourcesAndCleansOnlyRetry(t *testing.T) {
	const (
		namespace = "multica"
		taskID    = "11111111-2222-4333-8444-555555555555"
	)
	residueSecretName := "task-" + taskID
	residuePodName := "task-1111111-first"
	client := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: residueSecretName, Namespace: namespace}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: residuePodName, Namespace: namespace}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var createdSecret *corev1.Secret
	client.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createdSecret = action.(k8stesting.CreateAction).GetObject().(*corev1.Secret).DeepCopy()
		if createdSecret.Name == "" {
			createdSecret.Name = createdSecret.GenerateName + "second"
		}
		err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), createdSecret, namespace)
		return true, createdSecret, err
	})
	var createdPod *corev1.Pod
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createdPod = action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		if createdPod.Name == "" {
			createdPod.Name = createdPod.GenerateName + "second"
		}
		err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), createdPod, namespace)
		cancel()
		return true, createdPod, err
	})

	launcher := &Launcher{
		config: testLauncherConfig(namespace),
		owner:  metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "controller", UID: "controller-uid"},
		client: client,
	}
	err := launcher.Run(ctx, taskID, testProviderRequest(taskID), Streams{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry launch error = %v, want cancellation after distinct Pod creation", err)
	}
	if createdSecret == nil || createdSecret.Name == "" || createdSecret.Name == residueSecretName {
		t.Fatalf("retry Secret = %+v, want a distinct generated name", createdSecret)
	}
	if createdPod == nil || createdPod.Name == "" || createdPod.Name == residuePodName {
		t.Fatalf("retry Pod = %+v, want a distinct generated name", createdPod)
	}
	if got := requestSecretNameFromPod(createdPod); got != createdSecret.Name {
		t.Fatalf("retry Pod request Secret = %q, want %q", got, createdSecret.Name)
	}
	if got := podEnvironmentValue(createdPod.Spec.Containers[0].Env, "MULTICA_REQUEST_SECRET_NAME"); got != createdSecret.Name {
		t.Fatalf("retry Pod selector environment = %q, want %q", got, createdSecret.Name)
	}

	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), residueSecretName, metav1.GetOptions{}); err != nil {
		t.Fatalf("retry cleanup deleted attempt-1 Secret: %v", err)
	}
	if _, err := client.CoreV1().Pods(namespace).Get(context.Background(), residuePodName, metav1.GetOptions{}); err != nil {
		t.Fatalf("retry cleanup deleted attempt-1 Pod: %v", err)
	}
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), createdSecret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("retry Secret still exists after normal return: %v", err)
	}
	if _, err := client.CoreV1().Pods(namespace).Get(context.Background(), createdPod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("retry Pod still exists after normal return: %v", err)
	}
}

func TestLauncherPodCreateFailureCleansOnlyCreatedAttemptSecret(t *testing.T) {
	const (
		namespace = "multica"
		taskID    = "11111111-2222-4333-8444-555555555555"
	)
	residueSecretName := "task-" + taskID
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: residueSecretName, Namespace: namespace},
	})

	var createdSecretName string
	client.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		secret := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret).DeepCopy()
		if secret.Name == "" {
			secret.Name = secret.GenerateName + "failed"
		}
		createdSecretName = secret.Name
		err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), secret, namespace)
		return true, secret, err
	})
	errPodCreate := errors.New("stop after Pod create request")
	client.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errPodCreate
	})

	launcher := &Launcher{
		config: testLauncherConfig(namespace),
		owner:  metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "controller", UID: "controller-uid"},
		client: client,
	}
	err := launcher.Run(context.Background(), taskID, testProviderRequest(taskID), Streams{})
	if !errors.Is(err, errPodCreate) {
		t.Fatalf("Pod-create failure = %v, want %v", err, errPodCreate)
	}
	if createdSecretName == "" || createdSecretName == residueSecretName {
		t.Fatalf("created Secret name = %q, want distinct attempt name", createdSecretName)
	}
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), createdSecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed attempt Secret still exists: %v", err)
	}
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), residueSecretName, metav1.GetOptions{}); err != nil {
		t.Fatalf("failed attempt cleanup deleted prior Secret: %v", err)
	}
}

func testLauncherConfig(namespace string) Config {
	return Config{
		Namespace:          namespace,
		Image:              "registry.example/runtime@sha256:abc",
		ImagePullPolicy:    corev1.PullIfNotPresent,
		DaemonProxyURL:     "http://runtime-controller-daemon-proxy:19515",
		ServiceAccountName: "multica-task-worker",
		WorkspacePVCName:   "multica-runtime-workspace",
		Deadline:           time.Hour,
	}
}

func testProviderRequest(taskID string) Request {
	return Request{
		Provider: "codex",
		Env:      []string{"MULTICA_TASK_ID=" + taskID, "MULTICA_TOKEN=mat_attempt_two"},
		WorkDir:  "/workspace/tasks/" + taskID,
	}
}

func requestSecretNameFromPod(pod *corev1.Pod) string {
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "request" && volume.Secret != nil {
			return volume.Secret.SecretName
		}
	}
	return ""
}
