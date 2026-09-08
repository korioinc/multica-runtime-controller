// verifykube exercises resource ownership and scheduling against disposable K3s.
// It never falls back to ambient kubeconfig or an in-cluster service account.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/execution"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type check struct {
	Name    string            `json:"name"`
	Passed  bool              `json:"passed"`
	Details map[string]string `json:"details,omitempty"`
}
type evidence struct {
	SchemaVersion int       `json:"schemaVersion"`
	Started       time.Time `json:"started"`
	Completed     time.Time `json:"completed"`
	Namespace     string    `json:"namespace"`
	Checks        []check   `json:"checks"`
	Error         string    `json:"error,omitempty"`
}
type fixture struct {
	ctx           context.Context
	api           *clientset.Clientset
	config        *rest.Config
	client        *runtimekube.Client
	selection     execution.Selection
	request       wire.Request
	indexImage    string
	indexManifest string
	evidence      evidence
	cleanup       []func()
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "explicit disposable K3s kubeconfig")
	namespace := flag.String("namespace", "runtime-verify", "fixture namespace")
	selectionPath := flag.String("selection", "", "verified selection fixture")
	requestPath := flag.String("request", "", "dummy-credential request fixture")
	evidencePath := flag.String("evidence", "", "JSON verification result")
	selectedCase := flag.String("case", "", "run one bounded case; empty runs the complete suite")
	indexImage := flag.String("index-image", "", "actual published local OCI index for the installed build")
	indexManifest := flag.String("index-manifest", "", "raw registry OCI index bytes")
	flag.Parse()
	if err := run(*kubeconfig, *namespace, *selectionPath, *requestPath, *evidencePath, *selectedCase, *indexImage, *indexManifest); err != nil {
		fmt.Fprintln(os.Stderr, "verifykube:", err)
		os.Exit(1)
	}
}
func run(kubeconfig, namespace, selectionPath, requestPath, evidencePath, selectedCase, indexImage, indexManifest string) (result error) {
	if os.Getenv("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true" || kubeconfig != "/etc/rancher/k3s/k3s.yaml" || namespace != "runtime-verify" {
		return errors.New("explicit disposable K3s guard, local kubeconfig and runtime-verify namespace required")
	}
	for _, path := range []string{selectionPath, requestPath, evidencePath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != "/verification-evidence" {
			return errors.New("fixture paths must be direct files in /verification-evidence")
		}
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	server, err := url.Parse(cfg.Host)
	if err != nil {
		return err
	}
	if server.Scheme != "https" || server.Hostname() != "127.0.0.1" && server.Hostname() != "localhost" && server.Hostname() != "::1" {
		return errors.New("only the node-local HTTPS K3s API is allowed")
	}
	cfg.Timeout = 30 * time.Second
	api, err := clientset.NewForConfig(cfg)
	if err != nil {
		return err
	}
	var selection execution.Selection
	raw, err := os.ReadFile(selectionPath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &selection); err != nil {
		return err
	}
	if err = selection.Validate(); err != nil {
		return err
	}
	if selection.Namespace != namespace {
		return errors.New("selection namespace does not match disposable namespace")
	}
	raw, err = os.ReadFile(requestPath)
	if err != nil {
		return err
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	if request.OwnerID != selection.OwnerID || !request.RuntimeRef.Equal(selection.RuntimeRef) {
		return errors.New("request does not belong to selected fixture environment")
	}
	// A new controller owns new snapshot objects even when the stable runtime
	// contents remain compatible. Match Runner's new-attempt selection behavior.
	request.Snapshots = selection.Snapshots
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	f := &fixture{ctx: ctx, api: api, config: cfg, client: &runtimekube.Client{API: api, Transport: cfg, Namespace: namespace}, selection: selection, request: request, evidence: evidence{SchemaVersion: 1, Started: time.Now().UTC(), Namespace: namespace}}
	if indexImage != "" {
		if indexManifest != "/verification-evidence/index-manifest.json" {
			return errors.New("index image requires its raw local registry manifest evidence")
		}
		f.indexImage, f.indexManifest = indexImage, indexManifest
	} else if indexManifest != "" {
		return errors.New("index manifest requires its explicit image")
	}
	defer func() {
		for i := len(f.cleanup) - 1; i >= 0; i-- {
			f.cleanup[i]()
		}
		f.evidence.Completed = time.Now().UTC()
		if result != nil {
			f.evidence.Error = result.Error()
		}
		raw, err := json.MarshalIndent(f.evidence, "", "  ")
		if err == nil {
			err = os.WriteFile(evidencePath, append(raw, '\n'), 0600)
		}
		result = errors.Join(result, err)
	}()
	if _, err = api.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		return err
	}
	if _, err = f.client.Controller(ctx, selection.Controller.Name, selection.Controller.UID, selection.Worker.SingleNodeName); err != nil {
		return fmt.Errorf("live fixture controller: %w", err)
	}
	steps := []struct {
		name string
		run  func() error
	}{
		{"create-response-loss", f.responseLoss},
		{"limits-only-api-defaults", f.limitsDefaults},
		{"stream-drop", f.streamDrop},
		{"pod-payload-substitution", f.payloadSubstitution},
		{"pod-uid-replacement", f.podReplacement},
		{"secret-uid-replacement", f.secretReplacement},
		{"referenced-secret", f.referencedSecret},
		{"cleanup-recovery-preserves-files", f.cleanupRecovery},
	}
	if indexImage != "" {
		steps = append(steps, struct {
			name string
			run  func() error
		}{"oci-index-worker-execution", f.indexWorker})
	}
	steps = append(steps, struct {
		name string
		run  func() error
	}{"rwo-fixed-node-scheduling", f.scheduling})
	found := selectedCase == ""
	for _, step := range steps {
		if step.name == selectedCase {
			found = true
		}
	}
	if !found {
		return errors.New("unknown verification case")
	}
	for _, step := range steps {
		if selectedCase != "" && step.name != selectedCase {
			continue
		}
		if err = step.run(); err != nil {
			f.evidence.Checks = append(f.evidence.Checks, check{Name: step.name, Passed: false})
			return fmt.Errorf("%s: %w", step.name, err)
		}
		f.evidence.Checks = append(f.evidence.Checks, check{Name: step.name, Passed: true})
		fmt.Println("passed:", step.name)
	}
	return nil
}
func (f *fixture) keepPod(p *corev1.Pod) {
	name, uid := p.Name, p.UID
	f.cleanup = append(f.cleanup, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = f.api.CoreV1().Pods(f.selection.Namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}, GracePeriodSeconds: int64Pointer(0)})
	})
}
func (f *fixture) keepSecret(s *corev1.Secret) {
	name, uid := s.Name, s.UID
	f.cleanup = append(f.cleanup, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = f.api.CoreV1().Secrets(f.selection.Namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	})
}
func int64Pointer(v int64) *int64 { return &v }
func (f *fixture) deletePod(name string, uid types.UID) error {
	if err := f.api.CoreV1().Pods(f.selection.Namespace).Delete(f.ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}, GracePeriodSeconds: int64Pointer(0)}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return wait.PollUntilContextTimeout(f.ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := f.api.CoreV1().Pods(f.selection.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if p.UID != uid {
			return false, errors.New("fixture Pod was replaced during deletion")
		}
		return false, nil
	})
}
